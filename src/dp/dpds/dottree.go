package dpds

import (
	"bytes"
	"strconv"
	"strings"
	"github.com/golang/glog"
	"encoding/json"
	"sync"
)

type DotTree struct {
	dotProvider DotProvider         // Dot Provider Interface
	dotMap      map[uint64]*MetaDot    // Map of meta dots
	nameIdMap   map[string]uint64      // Map of name->ids
	waiter      *sync.WaitGroup
}

type DotTreeFactory struct {
	dt     *DotTree // Dot Tree interface
}

func (dtf *DotTreeFactory) GetInstance() *DotTree  {
	if dtf.dt == nil {
		dt := &DotTree {nil,  make(map[uint64]*MetaDot),  make(map[string]uint64), new(sync.WaitGroup) }
		dt.dotProvider = GetProviderInstance()
	
		dt.dotProvider.Init()
		dt.loadMetaDots()
		dt.fillMetaDots()
	
		dtf.dt = dt
    }
    return dtf.dt
}

var DtFactory *DotTreeFactory = new(DotTreeFactory)

// Loads a block of dots.
func (dt *DotTree) loadMetaDotBlock(dotMapBlock map[uint64]*MetaDot, lower int, upper int) bool {
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

func (dt *DotTree) loadMetaDots() bool {
	if  dt.dotProvider == nil {
		glog.Errorf("Dot provider not available.")
		return false
	}

	dt.dotMap = make(map[uint64]*MetaDot)
	
    dotMapChannel := make(chan map[uint64]*MetaDot, 20)
	
	lower := 0
	upper := 100
	done := false
	loadHadErrors := false
	blockcount := 0

	for !done {
		if blockcount > 0 && blockcount % 10 == 0 {
			dt.waiter.Wait()
			var dotMapResult map[uint64]*MetaDot
			for {
				if done {
					break
				}
			    dotMapResult = <- dotMapChannel
			    blockcount--
			    if dotMapResult == nil {
					glog.Error("Empty result")
				    break
			    }

			    for k, v := range dotMapResult {
			    	glog.Error(k)
			    	glog.Error(v)
	               dt.dotMap[k] = v
	               dt.nameIdMap[v.Name] = k
	            }
			    if loadHadErrors {
				    return false
			    }
	        }
		}
		dt.waiter.Add(1)
		blockcount++
        go func(lblock int, ublock int) {
	        var dotMapBlock = make(map[uint64]*MetaDot)
            blockLoaded := dt.loadMetaDotBlock(dotMapBlock, lblock, ublock)

            if blockLoaded {
            	if len(dotMapBlock) == 0 {
            		done = true;
            	} else {
            		dotMapChannel <- dotMapBlock
            	}
            } else {
                blockLoaded := dt.loadMetaDotBlock(dotMapBlock, lblock, ublock)
                if blockLoaded {
		            if len(dotMapBlock) == 0 {
		            	done = true;
		            } else {
		        	    dotMapChannel<-dotMapBlock
		            }
                } else {
                	done = true;
                	loadHadErrors = true
                    glog.Error("Range load retry failure: "  + strconv.Itoa(lblock) + " " + strconv.Itoa(ublock))
                }
            }
            dt.waiter.Done()
        } (lower, upper)
        lower = lower + 100
        upper = upper + 100
	}

    go func() {
       dt.waiter.Wait()
       dotMapChannel<-nil
    }()

	var dotMapResult map[uint64]*MetaDot
	for {
		dotMapResult = <- dotMapChannel
		if loadHadErrors {
			return false
		}
		if dotMapResult == nil {
			break
		}
		
		for k, v := range dotMapResult {
           dt.dotMap[k] = v
           dt.nameIdMap[v.Name] = k
        }
    }

	return true
}

// Flushes out the data in the individual meta dots with extra useful data.
func (dt *DotTree) fillMetaDots() bool {
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

func (dt *DotTree) GenerateRoutes() map[string]string {
	routeMap  := make(map[string]string)      // Map of name->value
    
	for _, dot := range dt.dotMap {
		if dot.Id == 0 {
			// Skip root dot.
			continue
		}
		route := []string{}
		value := dot.Value
		for {
			route = append([]string{dot.Name}, route...)
			
			if dot.ParentId == 0 {
				completeRoute := "/" + strings.Join(route[:],"/")
				routeMap[completeRoute] = value
				break
			} else {
				dot = dt.dotMap[dot.ParentId]
			}
		}
	}
	return routeMap
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
