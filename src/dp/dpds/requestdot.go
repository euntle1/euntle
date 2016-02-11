package dpds

type RequestDot struct {
	Dot
	QueryParams       map[string][]string   // Modifiable map of query Parameters.
	currentSubRoute   string                // Current subroute
	routeComplete     string                // Complete requested route
	resultChannel     chan string           // encoded result
}

// Initializes the context.
func (rd *RequestDot) Init(queryParams map[string][]string, currentSubRoute string, routeComplete string, result string) {
	rd.QueryParams = make(map[string][]string)
    for k,v := range queryParams {
       rd.QueryParams[k] = v
    }

	rd.currentSubRoute = currentSubRoute
	rd.routeComplete = routeComplete
	rd.resultChannel = make(chan string, 1)
}

// Get result
func (rd *RequestDot) GetResult() string {
	select {
		case result := <-rd.resultChannel:
			return result
	}
}

// Get result
func (rd *RequestDot) WriteResult(result string) {
	rd.resultChannel<-result
}
