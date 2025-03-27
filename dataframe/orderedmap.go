package dataframe

import "koalas/series"

// OrderedMap maintains insertion order of keys
type OrderedMap struct {
	keys   []string
	values map[string]*series.Series
}

// NewOrderedMap creates a new ordered map
func NewOrderedMap() *OrderedMap {
	return &OrderedMap{
		keys:   make([]string, 0),
		values: make(map[string]*series.Series),
	}
}

// Set adds or updates a key-value pair
func (om *OrderedMap) Set(key string, value *series.Series) {
	if _, exists := om.values[key]; !exists {
		om.keys = append(om.keys, key)
	}
	om.values[key] = value
}

// Get retrieves a value by key
func (om *OrderedMap) Get(key string) (*series.Series, bool) {
	value, exists := om.values[key]
	return value, exists
}

// Delete removes a key-value pair
func (om *OrderedMap) Delete(key string) {
	if _, exists := om.values[key]; exists {
		delete(om.values, key)
		for i, k := range om.keys {
			if k == key {
				om.keys = append(om.keys[:i], om.keys[i+1:]...)
				break
			}
		}
	}
}

// Keys returns all keys in order
func (om *OrderedMap) Keys() []string {
	return om.keys
}

// Values returns all values in order
func (om *OrderedMap) Values() []*series.Series {
	values := make([]*series.Series, len(om.keys))
	for i, key := range om.keys {
		values[i] = om.values[key]
	}
	return values
}

// Len returns the number of key-value pairs
func (om *OrderedMap) Len() int {
	return len(om.keys)
}
