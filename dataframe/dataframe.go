package dataframe

import (
	"fmt"
	"koalas/series"
)

type DataFrame struct {
	columns *OrderedMap
	numRows int
	numCols int
	schema  map[string]string
}

// ColumnInfo holds both name and data type of a column
type ColumnInfo struct {
	Name     string
	DataType string
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
 - where ( i have where value n X) but not where  value in list[]


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
