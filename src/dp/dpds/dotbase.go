package dpds

import (
)

type Dot struct {
	Id       uint64 `json:"-"`   // ID of dot : 0 is the base root.
	ParentId uint64 `json:"-"`   // ID of Parent : 0 is the base root.
	Name     string              // Dot's name
	Value    string              // Dot's value
}
