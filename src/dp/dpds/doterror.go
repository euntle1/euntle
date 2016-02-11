package dpds


type DotError struct {
	Id        uint64 `json:"-"`   // ID of dot causing failure.
	errors    []error             // List of errors occuring on this DOT.
}