package main

import (
	"fmt"
	"koalas/core/dataframe"
	"koalas/core/schema"
	"koalas/core/series"
	"log"
)

func main() {
	// Create a schema
	// Create schema
	columns := map[string]string{
		"age":    "int",
		"name":   "string",
		"active": "bool",
	}
	schema := schema.Create(columns)

	// Create series (order doesn't matter)
	activeSeries, _ := series.Create("active", "bool", []interface{}{true, false, true})
	ageSeries, _ := series.Create("age", "int", []interface{}{20, 30, 40})
	nameSeries, _ := series.Create("name", "string", []interface{}{"John", "Jane", "Bob"})

	// Create DataFrame - order doesn't matter
	df, err := dataframe.Create(schema, []*series.Series{activeSeries, ageSeries, nameSeries})
	if err != nil {
		log.Fatal(err)
	}

	// Display the DataFrame
	fmt.Println(df.Display())
}
