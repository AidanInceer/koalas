package utils

// Zip combines two slices of any type into a slice of pairs
func Zip[T, U any](list1 []T, list2 []U) []struct {
	First  T
	Second U
} {
	length := len(list1)
	if len(list2) < length {
		length = len(list2)
	}
	result := make([]struct {
		First  T
		Second U
	}, length)
	for i := 0; i < length; i++ {
		result[i] = struct {
			First  T
			Second U
		}{list1[i], list2[i]}
	}
	return result
}
