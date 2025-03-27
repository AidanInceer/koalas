package dataframe

import (
	"fmt"
	"koalas/core/series"
	"koalas/core/utils"
)

type DataFrame struct {
	columns *OrderedMap
	numRows int
	numCols int
	schema  map[string]string
}

// Constructor
func Create(seriesList []*series.Series) (*DataFrame, error) {
	// Create map of series by name for easier lookup
	seriesMap := make(map[string]*series.Series)
	for _, s := range seriesList {
		if _, exists := seriesMap[s.Name]; exists {
			return nil, fmt.Errorf("duplicate series name: %s", s.Name)
		}
		seriesMap[s.Name] = s
	}

	df := &DataFrame{
		columns: NewOrderedMap(),
		numCols: 0,
		numRows: 0,
	}

	for _, s := range seriesList {
		if err := df.AddColumn(s.Name, s); err != nil {
			return nil, err
		}
	}

	df.schema = make(map[string]string)
	for _, s := range seriesList {
		df.schema[s.Name] = s.Datatype
	}

	return df, nil
}

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

// GetOrder returns the current column order
func (df *DataFrame) GetOrder() []string {
	return df.columns.Keys()
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

// ColumnInfo holds both name and data type of a column
type ColumnInfo struct {
	Name     string
	DataType string
}

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

/*
Stuff to add:

- GroupBy
- Sort
- Join
- Union and Union All

Sorting

- Sort by column
- Sort by multiple columns


Filtering
 - where
- Filter by column value
- Filter by multiple column values


Grouping

- Group by column
- Group by multiple columns

Aggregation

- Aggregate functions
	- count
	- sum
	- mean
	- median
	- mode
	- std
	- min
	- max
- Group by and aggregate


Selection

- Select columns by name
- Select rows by value


Change data type or column name


handling Null data
- is null
- not null
- drop na
- fill na

Exporting and reading data


head and tail




datetime stuff?


duplicate removal
- drop duplicates
- distinct




*/
