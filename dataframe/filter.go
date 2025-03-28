package dataframe

import (
	"fmt"
	"koalas/series"
)

// Alias for the Filter function
func (df *DataFrame) Where(column string, value interface{}) (*DataFrame, error) {
	return df.Filter(column, value)
}

func (df *DataFrame) Filter(column string, value interface{}) (*DataFrame, error) {
	// Validate input
	col, exists := df.columns.Get(column)
	if !exists {
		return nil, fmt.Errorf("column '%s' does not exist in DataFrame", column)
	}

	// Find indexes where the value matches
	indexes := []int{}
	for i, entry := range col.Data {
		if entry.Value == value {
			indexes = append(indexes, i)
		}
	}

	// Update each column in place
	for _, name := range df.columns.Keys() {
		if col, exists := df.columns.Get(name); exists {
			// Keep only the filtered entries
			newData := make([]series.Entry, len(indexes))
			for i, idx := range indexes {
				newData[i] = col.Data[idx]
			}
			col.Data = newData
		}
	}

	// Update row count
	df.numRows = len(indexes)
	return df, nil
}
