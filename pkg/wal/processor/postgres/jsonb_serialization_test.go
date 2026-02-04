// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"atvenupgstream/pkg/wal"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	sonicjson "atvenupgstream/internal/json"
)

func TestFilterRowColumnsJSONBHandling(t *testing.T) {
	t.Parallel()

	var jsonbValue map[string]any
	require.NoError(t, sonicjson.Unmarshal([]byte(`{
		"name": "David Richard 🏳️‍🌈",
		"location": "São Paulo",
		"quote": "said \"hello\""
	}`), &jsonbValue))

	cols := []wal.Column{
		{Name: "id", Type: "integer", Value: 1},
		{Name: "data", Type: "jsonb", Value: jsonbValue},
	}

	_, values := (&dmlAdapter{}).filterRowColumns(cols, schemaInfo{})

	jsonbResult, ok := values[1].([]byte)
	require.True(t, ok, "JSONB map should be pre-serialized to []byte, got %T", values[1])

	var parsed map[string]any
	require.NoError(t, json.Unmarshal(jsonbResult, &parsed))
	require.Equal(t, "David Richard 🏳️‍🌈", parsed["name"])
}

func TestFilterRowColumnsJSONBStringPassthrough(t *testing.T) {
	t.Parallel()

	// Simulates schemalog snapshot generator passing string(schema)
	originalJSON := `{"tables":[{"name":"users"}]}`

	cols := []wal.Column{
		{Name: "id", Type: "integer", Value: 1},
		{Name: "schema", Type: "jsonb", Value: originalJSON},
	}

	_, values := (&dmlAdapter{}).filterRowColumns(cols, schemaInfo{})

	// String values must pass through unchanged (not double-encoded)
	result, ok := values[1].(string)
	require.True(t, ok, "JSONB string should remain string, got %T", values[1])
	require.Equal(t, originalJSON, result)
}

func TestFilterRowColumnsJSONBArrayHandling(t *testing.T) {
	t.Parallel()

	var jsonbValue []any
	require.NoError(t, sonicjson.Unmarshal([]byte(`[
		{"name": "item1", "emoji": "🎉"},
		{"name": "item2", "location": "São Paulo"}
	]`), &jsonbValue))

	cols := []wal.Column{
		{Name: "id", Type: "integer", Value: 1},
		{Name: "items", Type: "jsonb", Value: jsonbValue},
	}

	_, values := (&dmlAdapter{}).filterRowColumns(cols, schemaInfo{})

	jsonbResult, ok := values[1].([]byte)
	require.True(t, ok, "JSONB array should be pre-serialized to []byte, got %T", values[1])

	var parsed []any
	require.NoError(t, json.Unmarshal(jsonbResult, &parsed))
	require.Len(t, parsed, 2)
	emoji, ok := parsed[0].(map[string]any)["emoji"]
	require.True(t, ok, "first array item should have emoji field")
	require.Equal(t, "🎉", emoji)
}

func TestBuildWhereQueryJSONBHandling(t *testing.T) {
	t.Parallel()

	// Simulates REPLICA IDENTITY FULL with JSONB column in identity
	var jsonbValue map[string]any
	require.NoError(t, sonicjson.Unmarshal([]byte(`{"name": "test 🎉"}`), &jsonbValue))

	d := &wal.Data{
		Identity: []wal.Column{
			{Name: "id", Type: "integer", Value: 1},
			{Name: "data", Type: "jsonb", Value: jsonbValue},
		},
	}

	adapter := &dmlAdapter{}
	_, whereValues, err := adapter.buildWhereQuery(d, 0)
	require.NoError(t, err)

	// JSONB in WHERE clause should also be pre-serialized
	jsonbResult, ok := whereValues[1].([]byte)
	require.True(t, ok, "JSONB in WHERE should be pre-serialized to []byte, got %T", whereValues[1])

	var parsed map[string]any
	require.NoError(t, json.Unmarshal(jsonbResult, &parsed))
	require.Equal(t, "test 🎉", parsed["name"])
}
