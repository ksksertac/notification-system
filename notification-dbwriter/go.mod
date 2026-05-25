module github.com/sertacyildirim/notification-system/notification-dbwriter

go 1.24.0

require (
	github.com/golang-migrate/migrate/v4 v4.19.1
	github.com/google/uuid v1.6.0
	github.com/redis/go-redis/v9 v9.7.3
	github.com/sertacyildirim/notification-system/shared v0.0.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/jmoiron/sqlx v1.4.0 // indirect
	github.com/lib/pq v1.10.9 // indirect
)

replace github.com/sertacyildirim/notification-system/shared => ../shared
