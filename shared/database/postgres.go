package database

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/sertacyildirim/notification-system/shared/config"
)

func NewPostgres(cfg config.DBConfig) (*sqlx.DB, error) {
	dsn := cfg.DSN()
	if !strings.Contains(dsn, "binary_parameters=yes") {
		if strings.Contains(dsn, "?") {
			dsn += "&binary_parameters=yes"
		} else {
			dsn += "?binary_parameters=yes"
		}
	}

	db, err := sqlx.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("pinging database: %w", err)
	}

	return db, nil
}
