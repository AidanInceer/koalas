package dataframe

import (
	"fmt"
)

// Select creates a new DataFrame with only the specified columns
func (df *DataFrame) Select(columns []string) (*DataFrame, error) {
	// Validate input
	if len(columns) == 0 {
		return nil, fmt.Errorf("no columns specified for selection")
	}

	// Validate all columns exist
	for _, column := range columns {
		if _, exists := df.columns.Get(column); !exists {
			return nil, fmt.Errorf("column '%s' does not exist in DataFrame", column)
		}
	}

	// Create new DataFrame with selected columns
	newDf := &DataFrame{
		columns: NewOrderedMap(),
		numRows: df.numRows,
		numCols: len(columns),
		schema:  make(map[string]string),
	}

	// Copy selected columns to new DataFrame
	for _, column := range columns {
		if col, exists := df.columns.Get(column); exists {
			newDf.columns.Set(column, col)
			newDf.schema[column] = df.schema[column]
		}
	}

	return newDf, nil
}
