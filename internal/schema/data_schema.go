package schema

import "encoding/json"

// DataSchema indexes allowed payload fields per event_type, with optional tenant-specific overrides.
type DataSchema struct {
	defaults  map[string]map[string]struct{}
	overrides map[string]map[string]map[string]struct{}
}

// AllowedFields returns the allowed payload field names for the given tenant and event type.
// It checks for a tenant-specific override first, then falls back to the default for the event type.
func (d *DataSchema) AllowedFields(tenantID, eventType string) map[string]struct{} {
	if tenantOverrides, ok := d.overrides[tenantID]; ok {
		if fields, found := tenantOverrides[eventType]; found {
			return fields
		}
	}
	return d.defaults[eventType]
}

// BuildDataSchema parses a raw JSON Schema and builds a DataSchema from its $defs section.
func BuildDataSchema(data []byte) (*DataSchema, error) {
	var raw rawSchemaDefs
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}

	ds := &DataSchema{
		defaults:  make(map[string]map[string]struct{}, len(raw.Defs.Defaults)),
		overrides: make(map[string]map[string]map[string]struct{}, len(raw.Defs.Overrides)),
	}

	for eventType, def := range raw.Defs.Defaults {
		ds.defaults[eventType] = def.extractKeys()
	}
	for tenantID, eventTypes := range raw.Defs.Overrides {
		ds.overrides[tenantID] = make(map[string]map[string]struct{}, len(eventTypes))
		for eventType, def := range eventTypes {
			ds.overrides[tenantID][eventType] = def.extractKeys()
		}
	}
	return ds, nil
}

type rawSchemaDefs struct {
	Defs struct {
		Defaults  map[string]rawPayloadDef            `json:"defaults"`
		Overrides map[string]map[string]rawPayloadDef `json:"overrides"`
	} `json:"$defs"`
}

type rawPayloadDef struct {
	Properties map[string]json.RawMessage `json:"properties"`
}

func (d *rawPayloadDef) extractKeys() map[string]struct{} {
	keys := make(map[string]struct{}, len(d.Properties))
	for k := range d.Properties {
		keys[k] = struct{}{}
	}
	return keys
}
