package config

// Config is the root configuration for the entire application.
// It is intended to be used with a library like Viper.
type Config struct {
	DataNode    DataNodeConfig    `mapstructure:"datanode"`
	Coordinator CoordinatorConfig `mapstructure:"coordinator"`
	Cluster     ClusterNodeConfig `mapstructure:"cluster"`
}
