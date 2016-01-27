package euntfs

import (
	"bytes"
	"strconv"
	"github.com/golang/glog"
	"encoding/json"
	dp "dp/dpds"
)

type Dot struct {
	Id       uint64 `json:"-"`   // ID of dot
	ParentId uint64 `json:"-"`   // ID of Parent
	Name     string              // Dot's name
	Value    string              // Dot's value
}

type MetaDot struct {
	Dot
	ParentName     string   // Parent's name
	Depth          uint64   // Depth of dot from root.
	Children       uint64   // Number of children.
}

type DotTree struct {
	dotProvider dp.DotProvider         // Dot Provider Interface
	dotMap      map[uint64]*MetaDot    // Map of meta dots
	nameIdMap   map[string]uint64      // Map of name->ids
}

func GetInstance() *DotTree {
	dt := &DotTree { }
	dt.dotProvider = dp.GetInstance()
	
	dt.dotProvider.Init()
	
	return dt
}

// Loads a block of dots.
func (dt *DotTree) LoadMetaDotBlock(dotMapBlock map[uint64]*MetaDot, lower int, upper int) bool {
	if  dt.dotProvider == nil {
		glog.Errorf("Source pool not available.")
		return false
	}
	
	result := dt.dotProvider.Begin(lower, upper)
	if !result {
		glog.Errorf("Couldn't get any dots.")
		return false
	}
    var err error

    for dt.dotProvider.HasMore() {
		metaDot := new(MetaDot)
		err = dt.dotProvider.Produce(&metaDot.Id, &metaDot.ParentId, &metaDot.Name, &metaDot.Value)

		if err != nil {
			glog.Error("Couldn't populate dot.")
			return false
		}

		metaDot.Children = 0
		dotMapBlock[metaDot.Id] = metaDot
	}

    if dt.dotProvider.Finalize() == false {
		glog.Error("Failure to clean up.")
		return false
    }

	return true
}

func (dt *DotTree) LoadMetaDots() bool {
	if  dt.dotProvider == nil {
		glog.Errorf("Dot provider not available.")
		return false
	}

	dt.dotMap = make(map[uint64]*MetaDot)
	
    dotMapChannel := make(chan map[uint64]*MetaDot)
	
	lower := 0
	upper := 100
	for upper <= 7000 {
        go func(lblock int, ublock int) {
	        var dotMapBlock = make(map[uint64]*MetaDot)
            blockLoaded := dt.LoadMetaDotBlock(dotMapBlock, lblock, ublock)
            if blockLoaded {
               dotMapChannel <- dotMapBlock
            } else {
                blockLoaded := dt.LoadMetaDotBlock(dotMapBlock, lblock, ublock)
                if blockLoaded {
                	dotMapChannel <- dotMapBlock
                } else {
                    glog.Error("Range load retry failure: "  + strconv.Itoa(lblock) + " " + strconv.Itoa(ublock))
                }
            }
        } (lower, upper)
        lower = lower + 100
        upper = upper + 100
	}

	var dotMapResult map[uint64]*MetaDot
	for len(dt.dotMap) < 7000 {
		dotMapResult = <- dotMapChannel

		for k, v := range dotMapResult { 
           dt.dotMap[k] = v
           dt.nameIdMap[v.Name] = k
        }
    }

	return true
}

func (dt *DotTree) CalculateMetaDotDepths() bool {
	var distanceCounter uint64
	for dotId := range dt.dotMap {
		var metaDot = dt.dotMap[dotId]
		distanceCounter = 0

		currentDotId := dotId
		currentParentId := (*metaDot).ParentId
		(*metaDot).ParentName = dt.dotMap[currentParentId].Name

		for currentDotId != currentParentId {
			currentDotId = currentParentId
			metaDotParent := dt.dotMap[currentParentId]
			currentParentId = (*metaDotParent).ParentId
			(*metaDotParent).Children++
			distanceCounter++
		}
		(*metaDot).Depth = distanceCounter
	}
	return true
}

func (dt *DotTree) ToJSON() string {
	firstDot := true
	var dotListBuffer bytes.Buffer
	dotListBuffer.WriteString("[")
	for dotId := range dt.dotMap {
		metaDotJson, err := json.Marshal(*dt.dotMap[dotId])
		if err != nil {
			return ""
		}
		if firstDot {
			dotListBuffer.WriteString(string(metaDotJson))
			firstDot = false
		} else {
			dotListBuffer.WriteString(", ")
			dotListBuffer.WriteString(string(metaDotJson))
		}
	}
	dotListBuffer.WriteString("]")

	return dotListBuffer.String()
}
