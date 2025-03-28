package dataframe

import (
	"fmt"
	"koalas/series"
)

type Row struct {
	Index  int
	Values []interface{}
}


// Adds a row to the DataFrame
func (df *DataFrame) AddRow(row []interface{}) error {
	if len(row) != df.numCols {
		return fmt.Errorf("row length mismatch: expected %d columns, got %d", df.numCols, len(row))
	}

	// Add values in the correct order
	for i, name := range df.columns.Keys() {
		col, _ := df.columns.Get(name)
		if !series.IsValidType(row[i], col.Datatype) {
			return fmt.Errorf("type mismatch for column %s: expected %s, got %v",
				name, col.Datatype, row[i])
		}
		col.Append(row[i])
	}

	df.numRows++
	return nil
}

// Adds multiple rows to the DataFrame
func (df *DataFrame) AddRows(rows [][]interface{}) error {
	for _, row := range rows {
		if err := df.AddRow(row); err != nil {
			return err
		}
	}
	return nil
}

// GetRow returns all values for a given row index
func (df *DataFrame) GetRow(index int) []interface{} {
	row := make([]interface{}, df.numCols)
	for i, name := range df.columns.Keys() {
		if col, exists := df.columns.Get(name); exists {
			if val, err := col.Get(index); err == nil {
				row[i] = val
			}
		}
	}
	return row
}
