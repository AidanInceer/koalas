package dataframe

import (
	"fmt"
	"koalas/core/schema"
	"koalas/core/series"
	"strings"
)

type DataFrame struct {
	columns map[string]*series.Series
	numRows int
	numCols int
	schema  *schema.Schema
}

// Constructor
func Create(schema *schema.Schema, seriesList []*series.Series) (*DataFrame, error) {
	// Create map of series by name for easier lookup
	seriesMap := make(map[string]*series.Series)
	for _, s := range seriesList {
		if _, exists := seriesMap[s.Name]; exists {
			return nil, fmt.Errorf("duplicate series name: %s", s.Name)
		}
		seriesMap[s.Name] = s
	}

	df := &DataFrame{
		columns: make(map[string]*series.Series),
		schema:  schema,
		numCols: 0,
		numRows: 0,
	}

	// Match series to schema columns by name
	schemaCols := schema.GetColumnMap()
	for colName, schemaType := range schemaCols {
		series, exists := seriesMap[colName]
		if !exists {
			return nil, fmt.Errorf("series not found for schema column: %s", colName)
		}

		// Validate data type matches schema
		if series.Datatype != schemaType {
			return nil, fmt.Errorf("type mismatch for column %s: schema expects %s, series has %s",
				colName, schemaType, series.Datatype)
		}

		// Add the column
		df.columns[colName] = series
		df.numCols++

		// Set or validate number of rows
		if df.numRows == 0 {
			df.numRows = series.Len()
		} else if df.numRows != series.Len() {
			return nil, fmt.Errorf("series length mismatch for column %s: expected %d rows, got %d",
				colName, df.numRows, series.Len())
		}
	}

	// Check for any series that don't match schema columns
	for seriesName := range seriesMap {
		if _, exists := schemaCols[seriesName]; !exists {
			return nil, fmt.Errorf("series %s not found in schema", seriesName)
		}
	}

	return df, nil
}

// Core operations
func (df *DataFrame) AddColumn(name string, series *series.Series) error {
	// Check if column exists in schema
	if df.schema.GetColumn(name) == "" {
		return fmt.Errorf("column '%s' not found in schema", name)
	}

	// Check if column already exists
	if _, exists := df.columns[name]; exists {
		return fmt.Errorf("column '%s' already exists", name)
	}

	// Validate data type matches schema
	if df.schema.GetColumn(name) != series.Dtype() {
		return fmt.Errorf("data type mismatch for column '%s': expected %s, got %s",
			name, df.schema.GetColumn(name), series.Dtype())
	}

	// Add the column
	df.columns[name] = series
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

func (df *DataFrame) GetColumn(name string) *series.Series {
	return df.columns[name]
}

// Validation methods
func (df *DataFrame) Validate() error {
	schemaCols := make(map[string]bool)
	for colName := range df.schema.GetColumnMap() {
		schemaCols[colName] = true
	}

	for colName := range df.columns {
		if !schemaCols[colName] {
			return fmt.Errorf("column '%s' exists in data but not in schema", colName)
		}
	}

	for colName, exists := range schemaCols {
		if exists && df.columns[colName] == nil {
			return fmt.Errorf("column '%s' defined in schema but missing in data", colName)
		}
	}

	return nil
}

// Utility methods
func (df *DataFrame) GetSchema() *schema.Schema {
	return df.schema
}

func (df *DataFrame) GetNumRows() int {
	return df.numRows
}

func (df *DataFrame) GetNumCols() int {
	return df.numCols
}

func (df *DataFrame) Shape() []int {
	return []int{df.numRows, df.numCols}
}

// Display methods
func (df *DataFrame) Display() string {
	if df.numRows == 0 || df.numCols == 0 {
		return "Empty DataFrame"
	}

	var sb strings.Builder

	// Display schema
	sb.WriteString(df.schema.Display())
	sb.WriteString("\n")

	// Display data
	sb.WriteString("Data:\n")
	sb.WriteString("--------\n")

	// Get column names and find max width for each column
	colNames := make([]string, 0, df.numCols)
	maxWidths := make(map[string]int)

	for name := range df.columns {
		colNames = append(colNames, name)
		maxWidths[name] = len(name)
		// Check data width
		for i := 0; i < df.columns[name].Len(); i++ {
			if val, err := df.columns[name].Get(i); err == nil {
				width := len(fmt.Sprintf("%v", val))
				if width > maxWidths[name] {
					maxWidths[name] = width
				}
			}
		}
	}

	// Print header
	sb.WriteString("  ")
	for _, name := range colNames {
		padding := strings.Repeat(" ", maxWidths[name]-len(name))
		sb.WriteString(fmt.Sprintf("%s%s  ", name, padding))
	}
	sb.WriteString("\n")

	// Print data rows
	for i := 0; i < df.numRows; i++ {
		sb.WriteString(fmt.Sprintf("%d ", i))
		for _, name := range colNames {
			val, _ := df.columns[name].Get(i)
			padding := strings.Repeat(" ", maxWidths[name]-len(fmt.Sprintf("%v", val)))
			sb.WriteString(fmt.Sprintf("%v%s  ", val, padding))
		}
		sb.WriteString("\n")
	}

	// Print shape
	sb.WriteString(fmt.Sprintf("\nShape: [%d, %d]", df.numRows, df.numCols))

	return sb.String()
}
