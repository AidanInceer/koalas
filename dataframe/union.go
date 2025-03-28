package dataframe

import (
	"fmt"
	"koalas/series"
	"koalas/utils"
)

// Union combines two DataFrames
func (df *DataFrame) Union(other *DataFrame) (*DataFrame, error) {
	// Check if number of columns match
	if df.numCols != other.numCols {
		return nil, fmt.Errorf("number of columns mismatch: expected %d columns, got %d", df.numCols, other.numCols)
	}

	// Create column info slices for both DataFrames
	cols1 := make([]ColumnInfo, len(df.columns.Keys()))
	cols2 := make([]ColumnInfo, len(other.columns.Keys()))

	// Fill column info for first DataFrame
	for i, name := range df.columns.Keys() {
		cols1[i] = ColumnInfo{
			Name:     name,
			DataType: df.schema[name],
		}
	}

	// Fill column info for second DataFrame
	for i, name := range other.columns.Keys() {
		cols2[i] = ColumnInfo{
			Name:     name,
			DataType: other.schema[name],
		}
	}

	// Compare columns using Zip
	colCheck := utils.Zip(cols1, cols2)

	// Check each column pair for name and data type match
	for i, pair := range colCheck {
		if pair.First.Name != pair.Second.Name {
			return nil, fmt.Errorf("column name mismatch at position %d: %s != %s",
				i, pair.First.Name, pair.Second.Name)
		}
		if pair.First.DataType != pair.Second.DataType {
			return nil, fmt.Errorf("data type mismatch for column %s: %s != %s",
				pair.First.Name, pair.First.DataType, pair.Second.DataType)
		}
	}

	// Create a new DataFrame to store the result
	result := &DataFrame{
		columns: NewOrderedMap(),
		numCols: df.numCols,
		numRows: df.numRows + other.numRows,
		schema:  make(map[string]string),
	}

	// For each column, combine the data from both DataFrames
	for _, name := range df.columns.Keys() {
		// Get columns from both DataFrames
		col1, _ := df.columns.Get(name)
		col2, _ := other.columns.Get(name)

		// Pre-allocate the combined data slice with exact capacity
		combinedData := make([]series.Entry, 0, df.numRows+other.numRows)

		// Append data from both DataFrames without creating intermediate slices
		combinedData = append(combinedData, col1.Data...)
		combinedData = append(combinedData, col2.Data...)

		// Create new series with combined data
		newSeries := &series.Series{
			Name:     name,
			Datatype: col1.Datatype,
			Data:     combinedData,
		}

		// Add the combined series to the result DataFrame
		result.columns.Set(name, newSeries)
		result.schema[name] = col1.Datatype
	}

	return result, nil
}
