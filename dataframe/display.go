package dataframe

import (
	"fmt"
	"strings"
)

// Display methods
func (df *DataFrame) Display() {
	if df.numRows == 0 {
		fmt.Println("Empty DataFrame")
		return
	}

	// Get column names in order
	colNames := df.columns.Keys()

	// Find maximum width for each column
	maxWidths := make([]int, len(colNames)+1) // +1 for index column
	// Set width for index column
	maxWidths[0] = len("Index")

	// Check data widths for index column
	for i := 0; i < df.numRows; i++ {
		width := len(fmt.Sprintf("%d", i))
		if width > maxWidths[0] {
			maxWidths[0] = width
		}
	}

	// Check widths for other columns
	for i, name := range colNames {
		// Start with header width
		maxWidths[i+1] = len(name)

		// Check data widths
		if col, exists := df.columns.Get(name); exists {
			for j := 0; j < col.Len(); j++ {
				if val, err := col.Get(j); err == nil {
					width := len(fmt.Sprintf("%v", val))
					if width > maxWidths[i+1] {
						maxWidths[i+1] = width
					}
				}
			}
		}
	}

	// Print headers
	fmt.Print("\n")
	fmt.Printf("%-*s", maxWidths[0], "Index")
	for i, name := range colNames {
		fmt.Print(" | ")
		fmt.Printf("%-*s", maxWidths[i+1], name)
	}
	fmt.Print("\n")

	// Print separator line
	fmt.Printf("%s", strings.Repeat("-", maxWidths[0]))
	for i := range colNames {
		fmt.Print("-+-")
		fmt.Printf("%s", strings.Repeat("-", maxWidths[i+1]))
	}
	fmt.Print("\n")

	// Print data rows
	for row := 0; row < df.numRows; row++ {
		fmt.Printf("%-*d", maxWidths[0], row)
		for i, name := range colNames {
			fmt.Print(" | ")
			if col, exists := df.columns.Get(name); exists {
				if val, err := col.Get(row); err == nil {
					fmt.Printf("%-*v", maxWidths[i+1], val)
				} else {
					fmt.Printf("%-*s", maxWidths[i+1], "ERROR")
				}
			}
		}
		fmt.Print("\n")
	}
	fmt.Print("\n")
}

func (df *DataFrame) DisplaySchema() {
	// Find the longest column name for alignment
	maxNameLen := 0
	for _, name := range df.columns.Keys() {
		if len(name) > maxNameLen {
			maxNameLen = len(name)
		}
	}

	// Print each column and its data type
	fmt.Println("Schema:")
	for _, name := range df.columns.Keys() {
		fmt.Printf("  %-*s: %s\n", maxNameLen, name, df.schema[name])
	}
}

func (df *DataFrame) Shape() []int {
	return []int{df.numRows, df.numCols}
}
