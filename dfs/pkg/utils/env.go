package utils

import (
	"log"
	"os"
	"strconv"
)

func GetEnvString(key string, fallback string) string {
	val, ok := os.LookupEnv(key)
	if !ok {
		log.Printf("Environment variable '%s' not set, defaulting to '%s'...\n", key, fallback)
		return fallback
	}

	return val
}

func GetEnvInt(key string, fallback int) int {
	val, ok := os.LookupEnv(key)
	if !ok {
		log.Printf("Environment variable '%s' not set, defaulting to '%d'...\n", key, fallback)
		return fallback
	}

	valAsInt, err := strconv.Atoi(val)
	if err != nil {
		log.Printf("Environment variable '%s' not an integer, defaulting to '%d'...\n", key, fallback)
		return fallback
	}

	return valAsInt
}

func GetEnvBool(key string, fallback bool) bool {
	val, ok := os.LookupEnv(key)
	if !ok {
		log.Printf("Environment variable '%s' not set, defaulting to '%t'...\n", key, fallback)
		return fallback
	}

	boolVal, err := strconv.ParseBool(val)
	if err != nil {
		log.Printf("Environment variable '%s' not a bool, defaulting to '%t'...\n", key, fallback)
		return fallback
	}

	return boolVal
}
