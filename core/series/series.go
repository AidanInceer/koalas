package series

import (
	"fmt"
	"reflect"
)

type Series struct {
	Name     string
	Datatype string
	Values   []interface{}
}

// Append adds a value to the end of the series
func (s *Series) Append(value interface{}) {
	s.Values = append(s.Values, value)
}

// Len returns the length of the series
func (s *Series) Len() int {
	return len(s.Values)
}

// Get returns the value at the specified index
func (s *Series) Get(index int) (interface{}, error) {
	if index < 0 || index >= len(s.Values) {
		return nil, fmt.Errorf("index out of range")
	}
	return s.Values[index], nil
}

// Set sets the value at the specified index
func (s *Series) Set(index int, value interface{}) error {
	if index < 0 || index >= len(s.Values) {
		return fmt.Errorf("index out of range")
	}
	s.Values[index] = value
	return nil
}

// String returns a string representation of the series
func (s *Series) String() string {
	return fmt.Sprintf("%v", s.Values)
}

// Create creates a new Series with a name and data type
func Create(name string, datatype string, values []interface{}) (*Series, error) {
	// Validate data type matches values
	for _, v := range values {
		if !isValidType(v, datatype) {
			return nil, fmt.Errorf("value %v does not match datatype %s", v, datatype)
		}
	}

	return &Series{
		Name:     name,
		Datatype: datatype,
		Values:   values,
	}, nil
}

// isValidType checks if a value matches the expected data type
func isValidType(value interface{}, datatype string) bool {
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

// Dtype returns the data type of the series
func (s *Series) Dtype() string {
	if len(s.Values) == 0 {
		return "empty"
	}
	return reflect.TypeOf(s.Values[0]).String()
}
