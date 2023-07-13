// Package config represents struct Config.
package config

// Config is a structure of environment variables.
type Config struct {
	PostgresPathKafka string `env:"POSTGRES_PATH_KAFKA"`
}
