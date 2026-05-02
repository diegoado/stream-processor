package config

import "time"

// KafkaProducerConfig holds Kafka producer tuning settings.
type KafkaProducerConfig struct {
	Acks            string        `env:"KAFKA_PRODUCER_ACKS"             envDefault:"all"`
	CompressionType string        `env:"KAFKA_PRODUCER_COMPRESSION_TYPE" envDefault:"gzip"`
	LingerMs        int           `env:"KAFKA_PRODUCER_LINGER_MS"        envDefault:"5"`
	BatchSize       int           `env:"KAFKA_PRODUCER_BATCH_SIZE"       envDefault:"131072"`
	BufferMemory    int           `env:"KAFKA_PRODUCER_BUFFER_MEMORY"    envDefault:"16777216"`
	FlushTimeout    time.Duration `env:"KAFKA_PRODUCER_FLUSH_TIMEOUT"    envDefault:"5s"`
}

// KafkaConsumerConfig holds Kafka consumer tuning settings.
type KafkaConsumerConfig struct {
	AutoOffsetReset        string `env:"KAFKA_CONSUMER_AUTO_OFFSET_RESET"         envDefault:"latest"`
	MaxPollRecords         int    `env:"KAFKA_CONSUMER_MAX_POLL_RECORDS"          envDefault:"5000"`
	MaxPollIntervalMs      int    `env:"KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS"      envDefault:"300000"`
	SessionTimeoutMs       int    `env:"KAFKA_CONSUMER_SESSION_TIMEOUT_MS"        envDefault:"30000"`
	HeartbeatIntervalMs    int    `env:"KAFKA_CONSUMER_HEARTBEAT_INTERVAL_MS"     envDefault:"5000"`
	FetchMinBytes          int    `env:"KAFKA_CONSUMER_FETCH_MIN_BYTES"           envDefault:"2097152"`
	FetchMaxBytes          int    `env:"KAFKA_CONSUMER_FETCH_MAX_BYTES"           envDefault:"104857600"`
	FetchMaxWaitMs         int    `env:"KAFKA_CONSUMER_FETCH_MAX_WAIT_MS"         envDefault:"10000"`
	MaxPartitionFetchBytes int    `env:"KAFKA_CONSUMER_MAX_PARTITION_FETCH_BYTES" envDefault:"10485760"`
}

// KafkaConfig holds Kafka connection, producer, and consumer settings.
type KafkaConfig struct {
	BootstrapServers string `env:"KAFKA_BOOTSTRAP_SERVERS" envDefault:"localhost:9092"`
	GroupID          string `env:"KAFKA_GROUP_ID"          envDefault:"stream-processor"`

	Producer KafkaProducerConfig
	Consumer KafkaConsumerConfig
}
