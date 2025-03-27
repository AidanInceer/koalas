package main

import (
	"fmt"
	"koalas/core/dataframe"
	"koalas/core/series"
)

func main() {
	// Create series (order doesn't matter)
	activeSeries, _ := series.Create("active", "bool", []interface{}{true, false, true})
	ageSeries, _ := series.Create("age", "int", []interface{}{20, 30, 40})
	nameSeries, _ := series.Create("name", "string", []interface{}{"John", "Jane", "Bob"})

	// Create DataFrame - order doesn't matter
	df, _ := dataframe.Create([]*series.Series{activeSeries, ageSeries, nameSeries})

	// Create second DataFrame with same columns but different data
	activeSeries2, _ := series.Create("active", "bool", []interface{}{false, true, false})
	ageSeries2, _ := series.Create("age", "int", []interface{}{25, 35, 45})
	nameSeries2, _ := series.Create("name", "string", []interface{}{"Alice", "Charlie", "David"})

	df2, _ := dataframe.Create([]*series.Series{activeSeries2, ageSeries2, nameSeries2})

	// Display both DataFrames
	fmt.Println("First DataFrame:")
	df.Display()

	fmt.Println("\nSecond DataFrame:")
	df2.Display()

	df, _ = df.Union(df2)

	fmt.Println("\nUnion DataFrame:")
	df.Display()
}
