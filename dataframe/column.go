package dataframe

import (
	"fmt"
	"koalas/series"
)

// Core operations - Add, Drop Columns and Rows
func (df *DataFrame) Columns() map[string]*series.Series {
	result := make(map[string]*series.Series)
	for _, key := range df.columns.Keys() {
		if value, exists := df.columns.Get(key); exists {
			result[key] = value
		}
	}
	return result
}

// Add columns to the DataFrame
func (df *DataFrame) AddColumn(name string, series *series.Series) error {
	// Check if column already exists
	if _, exists := df.columns.Get(name); exists {
		return fmt.Errorf("column '%s' already exists", name)
	}

	// Add the column
	df.columns.Set(name, series)
	df.numCols++

	// Set or validate number of rows
	if df.numRows == 0 {
		df.numRows = series.Len()
	} else if df.numRows != series.Len() {
		return fmt.Errorf("series length mismatch: expected %d rows, got %d",
			df.numRows, series.Len())
	}

	return nil
}

// Drops columns from the DataFrame
func (df *DataFrame) DropColumns(names []string) error {
	for _, name := range names {
		df.DropColumn(name)
	}
	return nil
}

// Drops a single column from the DataFrame
func (df *DataFrame) DropColumn(name string) error {
	// Check if column exists
	if _, exists := df.columns.Get(name); !exists {
		return fmt.Errorf("column '%s' does not exist", name)
	}

	// Update the schema and delete the column
	delete(df.schema, name)
	df.columns.Delete(name)

	// Update the number of columns
	df.numCols--
	return nil
}

func (df *DataFrame) RenameColumns(mapping map[string]string) error {
	fmt.Println(mapping)
	for oldName, newName := range mapping {
		if _, exists := df.columns.Get(oldName); !exists {
			// Check if column exists
			return fmt.Errorf("column '%s' does not exist", oldName)
		} else if _, exists := df.columns.Get(newName); exists {
			// Check if new column name already exists
			return fmt.Errorf("column '%s' already exists", newName)
		}

		for _, col := range df.columns.Values() {
			if col.Name == oldName {
				col.Name = newName
				df.columns.Delete(oldName)
				df.columns.Set(newName, col)
			}
		}
	}
	return nil
}

func (df *DataFrame) OrderColumns(order []string) error {
	// Validate all columns exist
	for _, name := range order {
		if _, exists := df.columns.Get(name); !exists {
			return fmt.Errorf("column '%s' does not exist", name)
		}
	}

	// Create new ordered map with desired order
	newColumns := NewOrderedMap()
	for _, name := range order {
		if col, exists := df.columns.Get(name); exists {
			newColumns.Set(name, col)
		}
	}

	df.columns = newColumns
	return nil
}
