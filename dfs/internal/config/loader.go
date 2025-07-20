package config

import (
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/spf13/viper"
)

// LoadDatanodeConfig reads datanode configuration from file and environment variables.
func LoadDatanodeConfig(path string) (*DatanodeAppConfig, error) {
	v := viper.New()

	// Set defaults
	datanodeDefaults := DefaultDatanodeAppConfig()
	v.SetDefault("node", datanodeDefaults.Node)
	v.SetDefault("agent", datanodeDefaults.Agent)

	// Configure file reading
	// v.SetConfigName("datanode") // a file named `datanode.yaml` can be used
	// v.SetConfigType("yaml")
	// v.AddConfigPath(path)
	// v.AddConfigPath(".")

	// if err := v.ReadInConfig(); err != nil {
	// 	if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
	// 		return nil, err // Only return error if it's not a "file not found" error
	// 	}
	// }

	// Configure environment variable reading
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Unmarshal and validate
	var cfg DatanodeAppConfig
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	validate := validator.New()
	if err := validate.Struct(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// LoadCoordinatorConfig reads coordinator configuration from file and environment variables.
func LoadCoordinatorConfig(path string) (*CoordinatorAppConfig, error) {
	v := viper.New()

	// Set defaults
	coordinatorDefaults := DefaultCoordinatorAppConfig()
	v.SetDefault("coordinator", coordinatorDefaults.Coordinator)
	v.SetDefault("agent", coordinatorDefaults.Agent)

	// // Configure file reading
	// v.SetConfigName("coordinator") // a file named `coordinator.yaml` can be used
	// v.SetConfigType("yaml")
	// v.AddConfigPath(path)
	// v.AddConfigPath(".")

	// if err := v.ReadInConfig(); err != nil {
	// 	if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
	// 		return nil, err
	// 	}
	// }

	// Configure environment variable reading
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Unmarshal and validate
	var cfg CoordinatorAppConfig
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	validate := validator.New()
	if err := validate.Struct(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}
