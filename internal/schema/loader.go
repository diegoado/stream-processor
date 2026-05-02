package schema

import (
	"context"
	"io"
	"log/slog"
	"time"

	"github.com/diegoado/stream-processor/pkg/aws"
	"github.com/diegoado/stream-processor/pkg/config"
	"github.com/diegoado/stream-processor/pkg/logger"
)

// Loader fetches and refreshes JSON Schema from S3.
type Loader struct {
	log    *slog.Logger
	client aws.S3Client
	cfg    config.SchemaConfig
}

// NewLoader creates a Loader with the given S3 client and schema configuration.
func NewLoader(client aws.S3Client, cfg config.SchemaConfig) *Loader {
	return &Loader{log: logger.Get("schema-loader"), client: client, cfg: cfg}
}

// Load fetches the schema from S3 and returns the raw bytes and ETag.
func (l *Loader) Load(ctx context.Context) ([]byte, string, error) {
	out, err := l.client.GetObject(ctx, l.cfg.Bucket, l.cfg.Key)
	if err != nil {
		return nil, "", err
	}
	defer func(Body io.ReadCloser) {
		_ = Body.Close()
	}(out.Body)

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, "", err
	}

	etag := ""
	if out.ETag != nil {
		etag = *out.ETag
	}
	return data, etag, nil
}

// StartAutoRefresh polls S3 for schema changes and updates the validator.
func (l *Loader) StartAutoRefresh(ctx context.Context, validator *Validator) {
	go func() {
		ticker := time.NewTicker(l.cfg.RefreshInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				l.log.Info("schema auto-refresh stopped")
				return
			case <-ticker.C:
				l.checkAndRefresh(ctx, validator)
			}
		}
	}()
}

func (l *Loader) checkAndRefresh(ctx context.Context, validator *Validator) {
	head, err := l.client.HeadObject(ctx, l.cfg.Bucket, l.cfg.Key)
	if err != nil {
		l.log.Error("failed to check schema ETag", slog.Any("error", err))
		return
	}

	newETag := ""
	if head.ETag != nil {
		newETag = *head.ETag
	}

	if validator.ETag() == newETag {
		return
	}

	l.log.Info("schema ETag changed, refreshing", slog.String("old", validator.ETag()), slog.String("new", newETag))

	data, etag, err := l.Load(ctx)
	if err != nil {
		l.log.Error("failed to download schema", slog.Any("error", err))
		return
	}

	if err = validator.Update(data, etag); err != nil {
		l.log.Error("failed to compile schema", slog.Any("error", err))
	}
}
