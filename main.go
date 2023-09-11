package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	log := slog.Default()
	log.Info("starting the program...")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer cancel()

	if err := run(ctx, log); err != nil {
		log.Error("error running program",
			"err", err,
		)
		os.Exit(1)
	}

	log.Info("program finished successfully")
}

const (
	dbDriver = "pgx"
	dbDSN    = "postgres://root@localhost:26257/postgres?sslmode=disable&serial_normalization=sql_sequence"
)

func run(ctx context.Context, logger *slog.Logger) error {
	db, err := sql.Open(dbDriver, dbDSN)
	if err != nil {
		return fmt.Errorf("error opening db connection: %w", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("error checking db connection: %w", err)
	}

	if _, err := db.ExecContext(ctx, queryConfigure); err != nil {
		return fmt.Errorf("error configuring db session: %w", err)
	}

	if _, err := db.ExecContext(ctx, queryInit); err != nil {
		return fmt.Errorf("error initialising database: %w", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)

	go func(ctx context.Context) {
		defer wg.Done()

		rows, err := db.QueryContext(ctx, queryChangefeed)
		if err != nil {
			logger.Error("error starting changefeed",
				"err", err,
			)
			return
		}
		defer rows.Close()

		// cols, err := rows.ColumnTypes()
		// if err != nil {
		// 	logger.Error("error getting changefeed query columns",
		// 		"err", err,
		// 	)
		// 	return
		// }

		var row ChangefeedRow

		// TODO: figure out how to properly parse sizes,
		// because what we get in the driver is clearly not bytes, e.g.:
		// 2023/09/11 16:50:56 INFO parsed changefeed key size size=9223372036854775807
		//
		// for _, col := range cols {
		// 	switch col.Name() {
		// 	case "key":
		// 		ln, ok := col.Length()
		// 		if !ok {
		// 			// IDK lol
		// 			panic("expected key to be variable-length")
		// 		}

		// 		logger.Info("parsed changefeed key size",
		// 			"size", ln,
		// 		)
		// 		row.Key = make([]byte, ln) // actually should re-use existing length
		// 	case "value":
		// 		ln, ok := col.Length()
		// 		if !ok {
		// 			// IDK lol
		// 			panic("expected value to be variable-length")
		// 		}

		// 		logger.Info("parsed changefeed value size",
		// 			"size", ln,
		// 		)
		// 		row.Value = make([]byte, ln) // actually should re-use existing length
		// 	}
		// }

		for {
			select {
			case <-ctx.Done():
				return
			default:
				if !rows.Next() {
					logger.Info("changefeed scan finished")
					return
				}

				// TODO: row.Key is the primary key of the table, in our case it's just an integer.
				// We need to figure out how to parse it properly.
				if err := rows.Scan(&row.Table, &row.Key, &row.Value); err != nil {
					logger.Error("error reading changefeed row",
						"err", err,
					)
					continue
				}

				// Since we are requesting changefeed with JSON format output,
				// the values are just the rows before / after the insert / update / delete operation in the JSON format.
				// Decode it here.
				var val ChangefeedValue
				if err := json.NewDecoder(strings.NewReader(row.Value)).Decode(&val); err != nil {
					logger.Error("error parsing changefeed row value",
						"err", err,
					)
					continue
				}

				logger.Info("parsed changefeed value",
					"before", val.Before,
					"after", val.After,
				)
			}
		}
	}(ctx)

	var lastInsertID int

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("name-%d", i)
		val := []byte(key)

		if err := db.QueryRowContext(ctx, queryInsert, key, val, i).Scan(&lastInsertID); err != nil {
			return fmt.Errorf("error inserting data: %w", err)
		}

		logger.Info("inserted new row",
			"id", lastInsertID,
		)
	}

	wg.Wait()

	return nil
}

const (
	queryInit = `
CREATE TABLE IF NOT EXISTS test_1 (
	id SERIAL PRIMARY KEY,
	key VARCHAR(255),
	revision INTEGER,
	value bytea
);
`
	queryConfigure = `
SET CLUSTER SETTING kv.rangefeed.enabled = true;
	`

	queryInsert = `
INSERT INTO test_1 (key,value,revision) VALUES ($1,$2,$3) RETURNING id;
	`

	queryChangefeed = `
EXPERIMENTAL CHANGEFEED FOR test_1 WITH format = 'json';
`
)

type ChangefeedRow struct {
	Table string
	Key   string
	Value string
}

type ChangefeedValue struct {
	Before TableRow `json:"before"`
	After  TableRow `json:"after"`
}

type TableRow struct {
	ID       int    `json:"id"`
	Key      string `json:"key"`
	Revision int    `json:"revision"`

	// TODO: figure out how Cockroach / Go driver encode bytes
	// 2023/09/11 17:07:00 INFO parsed changefeed value before="{ID:0 Key: Revision:0 Value:}" after="{ID:130 Key:name-9 Revision:9 Value:\\x6e616d652d39}"
	Value string `json:"value"`
}
