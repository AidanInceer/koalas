package main

import (
	"fmt"
	"koalas/dataframe"
	"koalas/series"
)

func main() {
	// Create series (order doesn't matter)
	activeSeries, _ := series.Create("active", "bool", []interface{}{true, false, true})
	ageSeries, _ := series.Create("age", "int", []interface{}{20, 30, 40})
	nameSeries, _ := series.Create("name", "string", []interface{}{"John", "Nilly", "John"})

	// Create DataFrame - order doesn't matter
	df, _ := dataframe.Create([]*series.Series{activeSeries, ageSeries, nameSeries})

	// Create second DataFrame with same columns but different data
	activeSeries2, _ := series.Create("active", "bool", []interface{}{false, true, false})
	ageSeries2, _ := series.Create("age", "int", []interface{}{20, 35, 45})
	nameSeries2, _ := series.Create("name", "string", []interface{}{"John", "Nilly", "Nilly"})

	df2, _ := dataframe.Create([]*series.Series{activeSeries2, ageSeries2, nameSeries2})

	// Display both DataFrames
	fmt.Println("First DataFrame:")
	df.Display(false)

	fmt.Println("\nSecond DataFrame:")
	df2.Display(false)

	fmt.Println("\nJoin DataFrame:")
	joinDf, err := df.Join(df2, []string{"name"}, []string{"name"}, "", "cross")
	if err != nil {
		fmt.Printf("Error joining DataFrames: %v\n", err)
		return
	}
	joinDf.Display(true)

}
