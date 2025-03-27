package dataframe

import (
	"fmt"
	"koalas/series"
	"koalas/utils"
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

// Join combines two DataFrames based on a common column
func (df *DataFrame) Join(other *DataFrame, leftCols []string, rightCols []string, using string, how string) (*DataFrame, error) {

	// Check if the join type is valid
	joinTypes := []string{"inner", "left", "right", "outer", "cross"}
	if utils.StringContains(joinTypes, how) {
		return nil, fmt.Errorf("invalid join type: %s", how)
	}

	//check if len of leftCols and rightCols are equal
	if len(leftCols) != len(rightCols) {
		return nil, fmt.Errorf("number of columns mismatch: expected %d columns, got %d", len(leftCols), len(rightCols))
	}

	// check if leftCols in df and RightCols in other
	for i := 0; i < len(leftCols); i++ {
		if _, exists := df.columns.Get(leftCols[i]); !exists {
			return nil, fmt.Errorf("column '%s' does not exist in DataFrame", leftCols[i])
		}
		if _, exists := other.columns.Get(rightCols[i]); !exists {
			return nil, fmt.Errorf("column '%s' does not exist in DataFrame", rightCols[i])
		}
	}
	// TODO: Update to allow for the using statement to be a list of strings
	if len(using) > 0 && len(leftCols) == 0 && len(rightCols) == 0 {
		// check that using is in both dfs
		_, dfExists := df.columns.Get(using)
		_, otherExists := other.columns.Get(using)
		if !dfExists || !otherExists {
			return nil, fmt.Errorf("column '%s' does not exist in DataFrame", using[0])
		}
	}

	//switch statement to select sub functions deepen on how
	switch how {
	case "inner":
		return df.innerJoin(other, leftCols, rightCols, using)
	case "left":
		return df.leftJoin(other, leftCols, rightCols, using)
	case "right":
		return df.rightJoin(other, leftCols, rightCols, using)
	case "outer":
		return df.outerJoin(other, leftCols, rightCols, using)
	case "cross":
		return df.crossJoin(other, leftCols, rightCols, using)
	default:
		return nil, fmt.Errorf("invalid join type: %s", how)
	}

	return df, nil
}

// innerJoin combines two DataFrames based on a common column
func (df *DataFrame) innerJoin(other *DataFrame, leftCols []string, rightCols []string, using string) (*DataFrame, error) {
	return nil, nil
}

// leftJoin combines two DataFrames based on a common column
func (df *DataFrame) leftJoin(other *DataFrame, leftCols []string, rightCols []string, using string) (*DataFrame, error) {
	return nil, nil
}

// rightJoin combines two DataFrames based on a common column
func (df *DataFrame) rightJoin(other *DataFrame, leftCols []string, rightCols []string, using string) (*DataFrame, error) {
	return nil, nil
}

// outerJoin combines two DataFrames based on a common column
func (df *DataFrame) outerJoin(other *DataFrame, leftCols []string, rightCols []string, using string) (*DataFrame, error) {
	return nil, nil
}

// crossJoin combines two DataFrames based on a common column
func (df *DataFrame) crossJoin(other *DataFrame, leftCols []string, rightCols []string, using string) (*DataFrame, error) {
	return nil, nil
}
