package series

import (
	"fmt"
	"reflect"
	"sort"
)

// Entry represents a single value with its index
type Entry struct {
	Value interface{}
	Index int
}

// Series represents a single column of data
type Series struct {
	Name     string
	Datatype string
	Data     []Entry
}

// Append adds a value to the end of the series
func (s *Series) Append(value interface{}) error {
	if !IsValidType(value, s.Datatype) {
		return fmt.Errorf("invalid type: expected %s, got %T", s.Datatype, value)
	}
	s.Data = append(s.Data, Entry{
		Value: value,
		Index: len(s.Data),
	})
	return nil
}

// Len returns the length of the series
func (s *Series) Len() int {
	return len(s.Data)
}

// Get returns the value at the given index
func (s *Series) Get(index int) (interface{}, error) {
	if index < 0 || index >= len(s.Data) {
		return nil, fmt.Errorf("index out of range: %d", index)
	}
	return s.Data[index].Value, nil
}

// Set sets the value at the given index
func (s *Series) Set(index int, value interface{}) error {
	if index < 0 || index >= len(s.Data) {
		return fmt.Errorf("index out of range: %d", index)
	}
	if !IsValidType(value, s.Datatype) {
		return fmt.Errorf("invalid type: expected %s, got %T", s.Datatype, value)
	}
	s.Data[index].Value = value
	return nil
}

// Create creates a new Series with the given name, datatype, and values
func Create(name string, datatype string, values []interface{}) (*Series, error) {
	if !IsValidType(values[0], datatype) {
		return nil, fmt.Errorf("invalid type: expected %s, got %T", datatype, values[0])
	}

	// Create data array with values and indices
	data := make([]Entry, len(values))
	for i, v := range values {
		data[i] = Entry{
			Value: v,
			Index: i,
		}
	}

	return &Series{
		Name:     name,
		Datatype: datatype,
		Data:     data,
	}, nil
}

// IsValidType checks if a value matches the expected data type
func IsValidType(value interface{}, datatype string) bool {
	switch datatype {
	case "int":
		_, ok := value.(int)
		return ok
	case "float":
		_, ok := value.(float64)
		return ok
	case "string":
		_, ok := value.(string)
		return ok
	case "bool":
		_, ok := value.(bool)
		return ok
	default:
		return false
	}
}

// GetIndex returns the index value at the given position
func (s *Series) GetIndex(pos int) (int, error) {
	if pos < 0 || pos >= len(s.Data) {
		return 0, fmt.Errorf("position out of range: %d", pos)
	}
	return s.Data[pos].Index, nil
}

// SetIndex sets the index value at the given position
func (s *Series) SetIndex(pos int, index int) error {
	if pos < 0 || pos >= len(s.Data) {
		return fmt.Errorf("position out of range: %d", pos)
	}
	s.Data[pos].Index = index
	return nil
}

// GetIndices returns all indices
func (s *Series) GetIndices() []int {
	indices := make([]int, len(s.Data))
	for i, entry := range s.Data {
		indices[i] = entry.Index
	}
	return indices
}

// Reindex creates a new series with the given indices
func (s *Series) Reindex(indices []int) error {
	if len(indices) != len(s.Data) {
		return fmt.Errorf("invalid indices length: expected %d, got %d", len(s.Data), len(indices))
	}
	for i, index := range indices {
		s.Data[i].Index = index
	}
	return nil
}

// SortByIndex sorts the series by its indices
func (s *Series) SortByIndex() {
	sort.Slice(s.Data, func(i, j int) bool {
		return s.Data[i].Index < s.Data[j].Index
	})
}

// Dtype returns the data type of the series
func (s *Series) Dtype() string {
	if len(s.Data) == 0 {
		return "empty"
	}
	return reflect.TypeOf(s.Data[0].Value).String()
}

// Filter keeps only the entries at the specified indexes
func (s *Series) Filter(indexes []int) {
	// Create a map for O(1) lookup of valid indexes
	validIndexes := make(map[int]bool)
	for _, idx := range indexes {
		validIndexes[idx] = true
	}

	// Create new data slice with only valid entries
	newData := make([]Entry, 0, len(indexes))
	for i, entry := range s.Data {
		if validIndexes[i] {
			newData = append(newData, entry)
		}
	}

	s.Data = newData
}
