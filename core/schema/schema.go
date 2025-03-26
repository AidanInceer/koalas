package schema

import (
	"fmt"
	"strings"
)

// DataType represents the supported data types in the schema

type Schema struct {
	columnMap map[string]string
}

func Create(columns map[string]string) *Schema {
	schema := &Schema{
		columnMap: make(map[string]string),
	}
	for name, dtype := range columns {
		schema.columnMap[name] = dtype
	}
	return schema
}

func (s *Schema) AddColumn(name string, dtype string) {
	s.columnMap[name] = dtype
}

func (s *Schema) GetColumn(name string) string {
	return s.columnMap[name]
}

// GetColumnMap returns the internal column map
func (s *Schema) GetColumnMap() map[string]string {
	return s.columnMap
}

// Display returns a formatted string representation of the schema
func (s *Schema) Display() string {
	if len(s.columnMap) == 0 {
		return "Empty Schema"
	}

	var sb strings.Builder
	sb.WriteString("Schema:\n")
	sb.WriteString("--------\n")

	// Find the longest column name for alignment
	maxLen := 0
	for colName := range s.columnMap {
		if len(colName) > maxLen {
			maxLen = len(colName)
		}
	}

	// Print each column with aligned formatting
	for colName, dtype := range s.columnMap {
		padding := strings.Repeat(" ", maxLen-len(colName))
		sb.WriteString(fmt.Sprintf("%s%s: %s\n", colName, padding, dtype))
	}
	return sb.String()
}
