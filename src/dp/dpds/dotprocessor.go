package dpds

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/golang/glog"
	"regexp"
	"strings"
)

type DotProcessor interface {
    Process(dt *DotTree, sourceDot *MetaDot, sourceDotRoute string)
}

type DotProcessorChannel struct {
	
}

func serializeError(dotError *DotError) string {
	dotErrorJsonBytes, err := json.Marshal(dotError)
	dotErrorJson := string(dotErrorJsonBytes)
	if err != nil {
		glog.Error("DotError serialization error.")
		dotErrorJson = "System error"
	}

	return dotErrorJson
}

func processSerializedError(dotError *DotError, dotId uint64, errorDescription string) string {
	glog.Error(errorDescription)
	dotError.Id = dotId
	dotError.errors = append(dotError.errors, errors.New(errorDescription))
	return serializeError(dotError)
}

func processQSet(dt *DotTree, rd *RequestDot, sourceDot *MetaDot, dotError *DotError, qsetMap map[string]interface{}) {
	qArray := qsetMap["q"].([](interface{}))
	var dataSource string
	var tableName string
	queryFields := make([]string, len(qArray))
	queryValues := make([]string, len(qArray))
	hasErrors := false
	idx := 0

	for _, q := range qArray {
		qMap := q.(map[string]interface{})
		qname := qMap["name"].(string)
		qvalue, qValOk := rd.QueryParams[qname]
		if qMap["type"] == "required" {
			if !qValOk || len(qvalue[0]) == 0 {
				// Missing required parameter.
				dotErrorJson := processSerializedError(dotError, sourceDot.Id, "Missing required parameter: "+qname)
				rd.WriteResult(dotErrorJson)
			}
		}

		if qMap["filter"] != nil {
			filterMap := qMap["filter"].(map[string]interface{})
			if filterMap["type"] == "regex.gate" {
				if !qValOk || len(qvalue[0]) == 0 {
					// No value to evaluate and this param wasn't required.
					// All done with this parameter.
					continue
				}
				regex := filterMap["value"].(string)
				compiledRegex, cachedRegex := dt.regexMap[regex]
				if !cachedRegex {
					var cErr error
					compiledRegex, cErr = regexp.Compile(regex)
					if cErr != nil {
						glog.Error("Invalid regex for filter: " + regex)
					}
					dt.regexMap[regex] = compiledRegex
				}
				match := compiledRegex.MatchString(qvalue[0])
				if !match {
					processSerializedError(dotError, sourceDot.Id, "Incorrect parameter format: "+qname)
				}
			}
			if qvalue == nil || len(qvalue) == 0 {
				// Missing required parameter.
				processSerializedError(dotError, sourceDot.Id, "Missing required parameter: "+qname)
			}
		}

		if qMap["cdest"] != nil {
			cDestMap := qMap["cdest"].(map[string]interface{})

			if tableName == "" {
				if _, hasTable := cDestMap["table"]; hasTable {
					tableName = cDestMap["table"].(string)
				} else {
					processSerializedError(dotError, sourceDot.Id, fmt.Sprintf("Missing required table for dot: %d", sourceDot.Name))
					hasErrors = true
				}
			} else if tableName != cDestMap["table"] {
				processSerializedError(dotError, sourceDot.Id, "dot defined with multiple object destinations: "+qname)
				continue
			}

			if dataSource == "" {
				if _, hasSource := cDestMap["ds"]; hasSource {
					dataSource = cDestMap["ds"].(string)
				} else {
					processSerializedError(dotError, sourceDot.Id, fmt.Sprintf("Missing required datasource for dot: %d", sourceDot.Name))
					hasErrors = true
				}
			} else if dataSource != cDestMap["ds"] {
				processSerializedError(dotError, sourceDot.Id, "dot defined with multiple object destinations: "+qname)
				hasErrors = true
				continue
			}
			//fmt.Sprintf
			queryFields[idx] = qname
			queryValues[idx] = qvalue[0]
			idx += 1
		}
		// Initialize fields and create.
		if !hasErrors {
			dotProvider := GetProviderInstance(dataSource)
			dotProvider.InitFields(tableName, queryFields, queryValues, 0, 0)

			success := dotProvider.Create()
			if !success {
				processSerializedError(dotError, sourceDot.Id, fmt.Sprintf("Failure to create table: %s", tableName))
			}
			ReturnProviderInstance(dotProvider)
		}

		glog.Error(q)
	}
}

func processDSet(dt *DotTree, rd *RequestDot, sourceDot *MetaDot, dotError *DotError, cDestMap map[string]interface{}) {
	dArray := cDestMap["d"].([](interface{}))
	var dataSource string
	var tableName string

	if _, hasTable := cDestMap["table"]; hasTable {
		tableName = cDestMap["table"].(string)
	} else {
		processSerializedError(dotError, sourceDot.Id, fmt.Sprintf("Missing required table for dot: %d", sourceDot.Name))
		return
	}

	if _, hasSource := cDestMap["ds"]; hasSource {
		dataSource = cDestMap["ds"].(string)
	} else {
		// datasource required.
		processSerializedError(dotError, sourceDot.Id, fmt.Sprintf("Missing required datasource for dot: %d", sourceDot.Name))
		return
	}

	params := make([]string, len(dArray))
	i := 0
	hasErrors := false

	for _, d := range dArray {
		dMap := d.(map[string]interface{})
		dName := dMap["name"].(string)
		dType := dMap["type"].(string)

		if len(dName) == 0 || len(dType) == 0 {
			// Missing required parameter.
			hasErrors = true
			processSerializedError(dotError, sourceDot.Id, fmt.Sprintf("Failure to create table: %s %s %s", tableName, dName, dType))
			break
		}
		params[i] = fmt.Sprintf("%s %s", dName, dType)
		i += 1
	}
	// Initialize fields and create.
	if !hasErrors {
		dotProvider := GetProviderInstance(dataSource)

		dotProvider.InitFields(tableName, params, nil, 0, 0)
		success := dotProvider.Construct()
		if !success {
			processSerializedError(dotError, sourceDot.Id, fmt.Sprintf("Failure to create table: %s", tableName))
		}
		ReturnProviderInstance(dotProvider)
	}

}

func (dpc *DotProcessorChannel) Process(dt *DotTree, sourceDot *MetaDot, sourceDotRoute string) {

	go func(sourceDot *MetaDot, dotRoute string) {
		dotError := new(DotError)
		// Set up a listener to listen
		for {
			// listen for events on the dot context channel forever.
			select {
			case rd := <-sourceDot.RequestDotChannel:
				currentSubRoute := rd.currentSubRoute
				if strings.HasSuffix(currentSubRoute, "dot.json") {
					// dot.json is only ever internally referanceable.
					dotErrorJson := processSerializedError(dotError, sourceDot.Id, "Subroute should should never directly reference dot.json.")
					rd.WriteResult(dotErrorJson)
					continue
				} else {
					currentSubRoute = currentSubRoute + "/dot.json"
				}
				if rd.currentSubRoute != dotRoute {
					// This should never happen.
					dotErrorJson := processSerializedError(dotError, sourceDot.Id, "Subroute should always match the current dot route.")
					rd.WriteResult(dotErrorJson)
					continue
				} else {
					dotMolder := dt.routeMap[currentSubRoute]

					var dotConfig map[string]interface{}

					if err := json.Unmarshal([]byte(dotMolder.Value), &dotConfig); err != nil {
						dotErrorJson := processSerializedError(dotError, sourceDot.Id, "Invalid dot config: "+currentSubRoute)
						rd.WriteResult(dotErrorJson)
						continue
					}

					dotMap := dotConfig["dot"].(map[string]interface{})
					_, hasQset := dotMap["qset"]
					if hasQset {
						qsetMap := dotMap["qset"].(map[string]interface{})
						processQSet(dt, rd, sourceDot, dotError, qsetMap)
					} else {
						// Look for construction set.
						dsetMap := dotMap["dset"].(map[string]interface{})
						glog.Error(dsetMap)
						processDSet(dt, rd, sourceDot, dotError, dsetMap)
					}

					if rd.currentSubRoute == rd.routeComplete {
						// Result (if any) should be in dotContext now.. Just return
						// TODO: put result on a waiting result channel?
						rd.WriteResult(rd.routeComplete)
						continue
					}

					if len(dotError.errors) > 0 {
						dotErrorJson := serializeError(dotError)
						rd.WriteResult(dotErrorJson)
						continue
					}

					// Push to next dot.
					routeChunks := strings.Split(rd.currentSubRoute, "/")
					nextRouteChunks := strings.SplitN(rd.routeComplete, "/", len(routeChunks)+1)
					nextRoute := strings.Join(nextRouteChunks[:], "/")
					glog.Error("Looking for next route: " + nextRoute)
					nextDot := dt.routeMap[nextRoute]
					rd.currentSubRoute = nextRoute
					go func(rdl *RequestDot) {
						nextDot.RequestDotChannel <- rdl
					}(rd)
				}
			}
		}
	}(sourceDot, sourceDotRoute)
}
