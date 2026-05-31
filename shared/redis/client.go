package redis

import (
	"github.com/redis/go-redis/v9"
	"github.com/sertacyildirim/notification-system/shared/config"
)

func NewClient(cfg config.RedisConfig) redis.UniversalClient {
	if len(cfg.ClusterAddrs) > 0 {
		return redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:    cfg.ClusterAddrs,
			Password: cfg.Password,
		})
	}
	return redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})
}
