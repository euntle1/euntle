package dpds

import (
	"bytes"
	"encoding/json"
	"github.com/golang/glog"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

type DotTree struct {
	dotMap    map[uint64]*MetaDot // Map of meta dots
	nameIdMap map[string]uint64   // Map of name->ids
	waiter    *sync.WaitGroup
	routeMap  map[string]*MetaDot // Map of routes to attached dot.
	regexMap  map[string]*regexp.Regexp
}

type DotTreeFactory struct {
	dt *DotTree // Dot Tree interface
}

func (dtf *DotTreeFactory) GetInstance() *DotTree {
	if dtf.dt == nil {
		dt := &DotTree{make(map[uint64]*MetaDot),
			make(map[string]uint64),
			new(sync.WaitGroup),
			make(map[string]*MetaDot),
			make(map[string]*regexp.Regexp)}

		dt.loadMetaDots()
		dt.fillMetaDots()

		dtf.dt = dt
	}
	return dtf.dt
}

var DtFactory *DotTreeFactory = new(DotTreeFactory)

// Given a route, returns the corresponding dot.
func (dt *DotTree) GetDot(route string) *MetaDot {
	return dt.routeMap[route]
}

// Loads a block of dots.
func (dt *DotTree) loadMetaDotBlock(dotMapBlock map[uint64]*MetaDot, lower int, upper int) bool {
	dotProvider := GetProviderInstance("enju")

	if dotProvider == nil {
		glog.Errorf("Source pool not available.")
		return false
	}

	queryFields := []string{"Id", "ParentId", "Name", "Value"}
	whereFields := []string{"id >= ?", "and", "id < ?"}
	dotProvider.InitFields("dots", queryFields, whereFields, nil, nil, lower, upper)
	result := dotProvider.Begin()
	if !result {
		glog.Errorf("Couldn't get any dots.")
		return false
	}
	var err error

	for dotProvider.HasMore() {
		metaDot := new(MetaDot)
		metaDot.RequestDotChannel = make(chan *RequestDot, 100)
		err = dotProvider.Produce(&metaDot.Id, &metaDot.ParentId, &metaDot.Name, &metaDot.Value)

		if err != nil {
			glog.Error("Couldn't populate dot: " + err.Error())
			return false
		}

		metaDot.Children = 0
		dotMapBlock[metaDot.Id] = metaDot
	}

	if dotProvider.Finalize() == false {
		glog.Error("Failure to clean up.")
		return false
	}

	ReturnProviderInstance(dotProvider)

	return true
}

// Loads all metaDots from the Dot Data Source.
func (dt *DotTree) loadMetaDots() bool {
	dt.dotMap = make(map[uint64]*MetaDot)

	dotMapChannel := make(chan map[uint64]*MetaDot, 20)

	lower := 0
	upper := 100
	done := false
	loadHadErrors := false
	blockcount := 0
	currentBlockCount := 1
	previousBlockCount := 0

	for !done {
		if blockcount > 0 && blockcount%currentBlockCount == 0 {
			dt.waiter.Wait()
			for {
				if done {
					break
				}
				dotMapResult := <-dotMapChannel
				blockcount--
				if dotMapResult == nil {
					glog.Error("Empty result")
					break
				}

				for k, v := range dotMapResult {
					dt.dotMap[k] = v
					dt.nameIdMap[v.Name] = k
				}
				if loadHadErrors {
					return false
				}
				if len(dotMapResult) < (upper - lower) {
					return true
				}
				if blockcount == 0 {
					currentBlockCount = currentBlockCount + previousBlockCount
					previousBlockCount = currentBlockCount
				}
				break
			}
		}
		dt.waiter.Add(1)
		blockcount++
		go func(lblock int, ublock int) {
			var dotMapBlock = make(map[uint64]*MetaDot)
			blockLoaded := dt.loadMetaDotBlock(dotMapBlock, lblock, ublock)

			if blockLoaded {
				if len(dotMapBlock) == 0 {
					done = true
				} else {
					dotMapChannel <- dotMapBlock
				}
			} else {
				blockLoaded := dt.loadMetaDotBlock(dotMapBlock, lblock, ublock)
				if blockLoaded {
					if len(dotMapBlock) == 0 {
						done = true
					} else {
						dotMapChannel <- dotMapBlock
					}
				} else {
					done = true
					loadHadErrors = true
					glog.Error("Range load retry failure: " + strconv.Itoa(lblock) + " " + strconv.Itoa(ublock))
				}
			}
			dt.waiter.Done()
		}(lower, upper)
		lower = lower + 100
		upper = upper + 100
	}

	go func() {
		dt.waiter.Wait()
		dotMapChannel <- nil
	}()

	var dotMapResult map[uint64]*MetaDot
	for {
		dotMapResult = <-dotMapChannel
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

// Generates all routes and initializes DotProcessors for each Dot.
func (dt *DotTree) GenerateRoutes() map[string]*MetaDot {

	// Set up Chains of Responsibility for all Dots.
	for _, dot := range dt.dotMap {
		dotLocal := dot
		if dotLocal.Id == 0 {
			// Skip any root dots.
			continue
		}
		route := []string{}
		dpc := &DotProcessorChannel{}
		// Create a route all the way to the root for the present dot.
		for {
			route = append([]string{dotLocal.Name}, route...)

			if dotLocal.ParentId == 0 {
				sourceDotRoute := "/" + strings.Join(route[:], "/")

				glog.Error("Entering new route: " + sourceDotRoute)
				dpc.InitListener(dt, dot, sourceDotRoute)
				dt.routeMap[sourceDotRoute] = dot
				break
			} else {
				dotLocal = dt.dotMap[dotLocal.ParentId]
			}
		}
	}
	return dt.routeMap
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
