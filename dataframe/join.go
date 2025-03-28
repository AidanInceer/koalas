package dataframe

import (
	"fmt"
	"koalas/series"
	"koalas/utils"
)

// Join combines two DataFrames based on a common column
func (df *DataFrame) Join(other *DataFrame, leftCols []string, rightCols []string, using string, how string) (*DataFrame, error) {
	// Check if the join type is valid
	joinTypes := []string{"inner", "left", "right", "outer", "cross"}
	if !utils.StringContains(joinTypes, how) {
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
			return nil, fmt.Errorf("column '%s' does not exist in DataFrame", using)
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
		return df.crossJoin(other)
	}

	return df, fmt.Errorf("unsupported join type: %s", how)
}

// innerJoin combines two DataFrames based on a common column
func (df *DataFrame) innerJoin(other *DataFrame, leftCols []string, rightCols []string, using string) (*DataFrame, error) {
	leftCol, rightCol := GetJoinColumns(*df, *other, leftCols, rightCols)

	// Create a map to store the join pairs
	joinPairs := make(map[series.Entry][]series.Entry, 0)
	for _, lVal := range leftCol.Data {
		for _, RVal := range rightCol.Data {
			if lVal.Value == RVal.Value {
				joinPairs[lVal] = append(joinPairs[lVal], RVal)
			}
		}
	}

	// Create a new DataFrame to store the result
	result := &DataFrame{
		columns: NewOrderedMap(),
		numCols: df.numCols + (other.numCols - len(rightCols)), // Correct column count
		numRows: 0,
		schema:  make(map[string]string),
	}

	// Add the columns from the left DataFrame
	for _, name := range df.columns.Keys() {
		col, _ := df.columns.Get(name)
		newSeries := &series.Series{
			Name:     name,
			Datatype: col.Datatype,
			Data:     make([]series.Entry, 0),
		}
		result.columns.Set(name, newSeries)
		result.schema[name] = col.Datatype
	}

	// Add the columns from the right DataFrame (excluding join columns)
	for _, name := range other.columns.Keys() {
		if name != rightCols[0] { // Skip the join column
			col, _ := other.columns.Get(name)
			newSeries := &series.Series{
				Name:     name + "_right",
				Datatype: col.Datatype,
				Data:     make([]series.Entry, 0),
			}
			result.columns.Set(name+"_right", newSeries)
			result.schema[name+"_right"] = col.Datatype
		}
	}

	// Process each join pair and add rows to the result
	for lEntry, rEntries := range joinPairs {
		for _, rEntry := range rEntries {
			// Get the full rows from both DataFrames
			lRow := df.GetRow(lEntry.Index)
			rRow := other.GetRow(rEntry.Index)

			// Create a new row with the correct capacity
			newRow := make([]interface{}, 0, result.numCols)

			// Add values from left DataFrame
			newRow = append(newRow, lRow...)

			// Add values from right DataFrame (excluding join columns)
			for i, name := range other.columns.Keys() {
				if name != rightCols[0] { // Skip the join column
					newRow = append(newRow, rRow[i])
				}
			}

			// Add the row to the result DataFrame
			if err := result.AddRow(newRow); err != nil {
				return nil, fmt.Errorf("error adding row: %v", err)
			}
		}
	}

	return result, nil
}

// leftJoin combines two DataFrames based on a common column
func (df *DataFrame) leftJoin(other *DataFrame, leftCols []string, rightCols []string, using string) (*DataFrame, error) {
	leftCol, rightCol := GetJoinColumns(*df, *other, leftCols, rightCols)

	// Create a map to store the join pairs
	joinPairs := make(map[series.Entry][]series.Entry, 0)
	for _, lVal := range leftCol.Data {
		for _, RVal := range rightCol.Data {
			if lVal.Value == RVal.Value {
				joinPairs[lVal] = append(joinPairs[lVal], RVal)
			}
		}
	}

	// Create a new DataFrame to store the result
	result := &DataFrame{
		columns: NewOrderedMap(),
		numCols: df.numCols + (other.numCols - len(rightCols)), // Correct column count
		numRows: 0,
		schema:  make(map[string]string),
	}

	// Add the columns from the left DataFrame
	for _, name := range df.columns.Keys() {
		col, _ := df.columns.Get(name)
		newSeries := &series.Series{
			Name:     name,
			Datatype: col.Datatype,
			Data:     make([]series.Entry, 0),
		}
		result.columns.Set(name, newSeries)
		result.schema[name] = col.Datatype
	}

	// Add the columns from the right DataFrame (excluding join columns)
	for _, name := range other.columns.Keys() {
		if name != rightCols[0] { // Skip the join column
			col, _ := other.columns.Get(name)
			newSeries := &series.Series{
				Name:     name + "_right",
				Datatype: col.Datatype,
				Data:     make([]series.Entry, 0),
			}
			result.columns.Set(name+"_right", newSeries)
			result.schema[name+"_right"] = col.Datatype
		}
	}

	// Process each row from the left DataFrame
	for i, lEntry := range leftCol.Data {
		// Get the row from the left DataFrame
		lRow := df.GetRow(i)

		// Check if there are matching rows in the right DataFrame
		if rEntries, exists := joinPairs[lEntry]; exists {
			// If there are matches, add a row for each match
			for _, rEntry := range rEntries {
				// Get the matching row from the right DataFrame
				rRow := other.GetRow(rEntry.Index)

				// Create a new row with the correct capacity
				newRow := make([]interface{}, 0, result.numCols)

				// Add values from left DataFrame
				newRow = append(newRow, lRow...)

				// Add values from right DataFrame (excluding join columns)
				for j, name := range other.columns.Keys() {
					if name != rightCols[0] { // Skip the join column
						newRow = append(newRow, rRow[j])
					}
				}

				// Add the row to the result DataFrame
				if err := result.AddRow(newRow); err != nil {
					return nil, fmt.Errorf("error adding row: %v", err)
				}
			}
		} else {
			// If no matches, add the left row with nulls for right columns
			newRow := make([]interface{}, 0, result.numCols)

			// Add values from left DataFrame
			newRow = append(newRow, lRow...)

			// Add nulls for right DataFrame columns
			for _, name := range other.columns.Keys() {
				if name != rightCols[0] { // Skip the join column
					newRow = append(newRow, nil)
				}
			}

			// Add the row to the result DataFrame
			if err := result.AddRow(newRow); err != nil {
				return nil, fmt.Errorf("error adding row: %v", err)
			}
		}
	}

	return result, nil
}

// rightJoin combines two DataFrames based on a common column
func (df *DataFrame) rightJoin(other *DataFrame, leftCols []string, rightCols []string, using string) (*DataFrame, error) {
	leftCol, rightCol := GetJoinColumns(*df, *other, leftCols, rightCols)

	// Create a map to store the join pairs
	joinPairs := make(map[series.Entry][]series.Entry, 0)
	for _, rVal := range rightCol.Data {
		for _, lVal := range leftCol.Data {
			if rVal.Value == lVal.Value {
				joinPairs[rVal] = append(joinPairs[rVal], lVal)
			}
		}
	}

	// Create a new DataFrame to store the result
	result := &DataFrame{
		columns: NewOrderedMap(),
		numCols: df.numCols + (other.numCols - len(rightCols)), // Correct column count
		numRows: 0,
		schema:  make(map[string]string),
	}

	// Add the columns from the left DataFrame (excluding join columns)
	for _, name := range df.columns.Keys() {
		if name != leftCols[0] { // Skip the join column
			col, _ := df.columns.Get(name)
			newSeries := &series.Series{
				Name:     name + "_left",
				Datatype: col.Datatype,
				Data:     make([]series.Entry, 0),
			}
			result.columns.Set(name+"_left", newSeries)
			result.schema[name+"_left"] = col.Datatype
		}
	}

	// Add the columns from the right DataFrame
	for _, name := range other.columns.Keys() {
		col, _ := other.columns.Get(name)
		newSeries := &series.Series{
			Name:     name,
			Datatype: col.Datatype,
			Data:     make([]series.Entry, 0),
		}
		result.columns.Set(name, newSeries)
		result.schema[name] = col.Datatype
	}

	// Process each row from the right DataFrame
	for i, rEntry := range rightCol.Data {
		// Get the row from the right DataFrame
		rRow := other.GetRow(i)

		// Check if there are matching rows in the left DataFrame
		if lEntries, exists := joinPairs[rEntry]; exists {
			// If there are matches, add a row for each match
			for _, lEntry := range lEntries {
				// Get the matching row from the left DataFrame
				lRow := df.GetRow(lEntry.Index)

				// Create a new row with the correct capacity
				newRow := make([]interface{}, 0, result.numCols)

				// Add values from left DataFrame (excluding join columns)
				for j, name := range df.columns.Keys() {
					if name != leftCols[0] { // Skip the join column
						newRow = append(newRow, lRow[j])
					}
				}

				// Add values from right DataFrame
				newRow = append(newRow, rRow...)

				// Add the row to the result DataFrame
				if err := result.AddRow(newRow); err != nil {
					return nil, fmt.Errorf("error adding row: %v", err)
				}
			}
		} else {
			// If no matches, add the right row with nulls for left columns
			newRow := make([]interface{}, 0, result.numCols)

			// Add nulls for left DataFrame columns
			for _, name := range df.columns.Keys() {
				if name != leftCols[0] { // Skip the join column
					newRow = append(newRow, nil)
				}
			}

			// Add values from right DataFrame
			newRow = append(newRow, rRow...)

			// Add the row to the result DataFrame
			if err := result.AddRow(newRow); err != nil {
				return nil, fmt.Errorf("error adding row: %v", err)
			}
		}
	}

	return result, nil
}

// outerJoin combines two DataFrames based on a common column
func (df *DataFrame) outerJoin(other *DataFrame, leftCols []string, rightCols []string, using string) (*DataFrame, error) {
	leftCol, rightCol := GetJoinColumns(*df, *other, leftCols, rightCols)

	// Create maps to store the join pairs for both directions
	leftToRight := make(map[series.Entry][]series.Entry, 0)
	rightToLeft := make(map[series.Entry][]series.Entry, 0)

	// Build join pairs in both directions
	for _, lVal := range leftCol.Data {
		for _, rVal := range rightCol.Data {
			if lVal.Value == rVal.Value {
				leftToRight[lVal] = append(leftToRight[lVal], rVal)
				rightToLeft[rVal] = append(rightToLeft[rVal], lVal)
			}
		}
	}

	// Create a new DataFrame to store the result
	result := &DataFrame{
		columns: NewOrderedMap(),
		numCols: df.numCols + (other.numCols - len(rightCols)), // Correct column count
		numRows: 0,
		schema:  make(map[string]string),
	}

	// Add the columns from the left DataFrame
	for _, name := range df.columns.Keys() {
		col, _ := df.columns.Get(name)
		newSeries := &series.Series{
			Name:     name,
			Datatype: col.Datatype,
			Data:     make([]series.Entry, 0),
		}
		result.columns.Set(name, newSeries)
		result.schema[name] = col.Datatype
	}

	// Add the columns from the right DataFrame (excluding join columns)
	for _, name := range other.columns.Keys() {
		if name != rightCols[0] { // Skip the join column
			col, _ := other.columns.Get(name)
			newSeries := &series.Series{
				Name:     name + "_right",
				Datatype: col.Datatype,
				Data:     make([]series.Entry, 0),
			}
			result.columns.Set(name+"_right", newSeries)
			result.schema[name+"_right"] = col.Datatype
		}
	}

	// Process all rows from both DataFrames
	processedLeft := make(map[int]bool)
	processedRight := make(map[int]bool)

	// Process matching rows
	for lEntry, rEntries := range leftToRight {
		for _, rEntry := range rEntries {
			lRow := df.GetRow(lEntry.Index)
			rRow := other.GetRow(rEntry.Index)

			newRow := make([]interface{}, 0, result.numCols)

			// Add values from left DataFrame
			newRow = append(newRow, lRow...)

			// Add values from right DataFrame (excluding join columns)
			for i, name := range other.columns.Keys() {
				if name != rightCols[0] { // Skip the join column
					newRow = append(newRow, rRow[i])
				}
			}

			if err := result.AddRow(newRow); err != nil {
				return nil, fmt.Errorf("error adding row: %v", err)
			}

			processedLeft[lEntry.Index] = true
			processedRight[rEntry.Index] = true
		}
	}

	// Process unmatched left rows
	for i := range leftCol.Data {
		if !processedLeft[i] {
			lRow := df.GetRow(i)
			newRow := make([]interface{}, 0, result.numCols)

			// Add values from left DataFrame
			newRow = append(newRow, lRow...)

			// Add nulls for right DataFrame columns
			for _, name := range other.columns.Keys() {
				if name != rightCols[0] { // Skip the join column
					newRow = append(newRow, nil)
				}
			}

			if err := result.AddRow(newRow); err != nil {
				return nil, fmt.Errorf("error adding row: %v", err)
			}

			processedLeft[i] = true
		}
	}

	// Process unmatched right rows
	for i, rEntry := range rightCol.Data {
		if !processedRight[i] {
			rRow := other.GetRow(i)
			newRow := make([]interface{}, 0, result.numCols)

			// Add nulls for left DataFrame columns
			for _, name := range df.columns.Keys() {
				if name != leftCols[0] { // Skip the join column
					newRow = append(newRow, nil)
				}
			}

			// Add join column value
			newRow = append(newRow, rEntry.Value)

			// Add values from right DataFrame (excluding join columns)
			for i, name := range other.columns.Keys() {
				if name != rightCols[0] { // Skip the join column
					newRow = append(newRow, rRow[i])
				}
			}

			if err := result.AddRow(newRow); err != nil {
				return nil, fmt.Errorf("error adding row: %v", err)
			}

			processedRight[i] = true
		}
	}

	return result, nil
}

// crossJoin combines two DataFrames by creating a Cartesian product of all rows
func (df *DataFrame) crossJoin(other *DataFrame) (*DataFrame, error) {
	// Create a new DataFrame to store the result
	result := &DataFrame{
		columns: NewOrderedMap(),
		numCols: df.numCols + other.numCols, // All columns from both DataFrames
		numRows: 0,
		schema:  make(map[string]string),
	}

	// Add the columns from the left DataFrame
	for _, name := range df.columns.Keys() {
		col, _ := df.columns.Get(name)
		newSeries := &series.Series{
			Name:     name,
			Datatype: col.Datatype,
			Data:     make([]series.Entry, 0),
		}
		result.columns.Set(name, newSeries)
		result.schema[name] = col.Datatype
	}

	// Add the columns from the right DataFrame
	for _, name := range other.columns.Keys() {
		col, _ := other.columns.Get(name)
		newSeries := &series.Series{
			Name:     name + "_right",
			Datatype: col.Datatype,
			Data:     make([]series.Entry, 0),
		}
		result.columns.Set(name+"_right", newSeries)
		result.schema[name+"_right"] = col.Datatype
	}

	// Perform cross join
	for i := 0; i < df.numRows; i++ {
		lRow := df.GetRow(i)
		for j := 0; j < other.numRows; j++ {
			rRow := other.GetRow(j)
			newRow := make([]interface{}, 0, result.numCols)

			// Add values from left DataFrame
			newRow = append(newRow, lRow...)

			// Add values from right DataFrame
			newRow = append(newRow, rRow...)

			if err := result.AddRow(newRow); err != nil {
				return nil, fmt.Errorf("error adding row: %v", err)
			}
		}
	}

	return result, nil
}

func GetJoinColumns(df DataFrame, other DataFrame, leftCols []string, rightCols []string) (*series.Series, *series.Series) {
	leftCol, _ := df.columns.Get(leftCols[0])
	rightCol, _ := other.columns.Get(rightCols[0])
	return leftCol, rightCol
}
