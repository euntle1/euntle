package dpds

import ()

type Dot struct {
	Id       uint64 `json:"-"` // ID of dot : 0 is the base root.
	ParentId uint64 `json:"-"` // ID of Parent : 0 is the base root.
	Name     string // Dot's name
	Value    string // Dot's value
}

type MetaDot struct {
	Dot
	ParentName        string           // Parent's name
	Depth             uint64           // Depth of dot from root.
	Children          uint64           // Number of children.
	RequestDotChannel chan *RequestDot // RequestDot channel
	DotRoute          string           // Route for this dot
}
