package schema

import (
	"encoding/json"
	"sync"

	"github.com/xeipuuv/gojsonschema"

	"github.com/diegoado/stream-processor/pkg/event"
	"github.com/diegoado/stream-processor/pkg/utilities"
)

// Validator validates and sanitizes events.
type Validator interface {
	ValidateAndSanitize(evt event.Event) (*event.Event, []string, error)
	Update(data []byte, etag string) error
	ETag() string
}

type validatorImpl struct {
	mu         sync.RWMutex
	schema     *gojsonschema.Schema
	dataSchema *DataSchema
	etag       string
}

// NewValidator compiles a JSON Schema from raw bytes and creates a Validator.
func NewValidator(data []byte, etag string) (Validator, error) {
	s, err := gojsonschema.NewSchema(gojsonschema.NewBytesLoader(data))
	if err != nil {
		return nil, err
	}
	ds, err := BuildDataSchema(data)
	if err != nil {
		return nil, err
	}
	return &validatorImpl{schema: s, dataSchema: ds, etag: etag}, nil
}

// validate validates an event against the current schema.
func (v *validatorImpl) validate(evt event.Event) ([]string, error) {
	raw, err := json.Marshal(evt)
	if err != nil {
		return nil, err
	}

	result, err := v.schema.Validate(gojsonschema.NewBytesLoader(raw))
	if err != nil {
		return nil, err
	}
	if result.Valid() {
		return nil, nil
	}

	errors := make([]string, len(result.Errors()))
	for i, e := range result.Errors() {
		errors[i] = e.String()
	}
	return errors, nil
}

// ValidateAndSanitize validates an event and, if valid, returns a sanitized copy with extra payload fields removed.
func (v *validatorImpl) ValidateAndSanitize(evt event.Event) (*event.Event, []string, error) {
	errors, err := v.validate(evt)
	if err != nil {
		return nil, nil, err
	}
	if len(errors) > 0 {
		return nil, errors, nil
	}

	allowed := v.dataSchema.AllowedFields(evt.TenantID, evt.EventType)
	sanitized := utilities.DoSanitizeEventPayload(&evt, allowed)
	return sanitized, nil, nil
}

// Update replaces the schema with a newly compiled one. Thread-safe.
func (v *validatorImpl) Update(data []byte, etag string) error {
	schema, err := gojsonschema.NewSchema(gojsonschema.NewBytesLoader(data))
	if err != nil {
		return err
	}
	dataSchema, err := BuildDataSchema(data)
	if err != nil {
		return err
	}

	v.mu.Lock()
	v.schema = schema
	v.dataSchema = dataSchema
	v.etag = etag
	v.mu.Unlock()

	return nil
}

// ETag returns the current schema ETag. Thread-safe.
func (v *validatorImpl) ETag() string {
	return v.etag
}
