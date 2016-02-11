package dpds

type DotProvider interface {
	Init(dbSource string)                // Initialize the provider.
    GetSource() string                   // Get current data source used to init provider.
	InitFields(tableName string, queryFields []string, whereFields []string, lowerBound int, upperBound int)
    Construct() bool                     // Enable ability to construct a table.
    Create() bool                        // Enable ability to insert/create into a table.
	Begin() bool                         // Begin providing all available dots.
	HasMore() bool                       // Are more dots available?
	Produce(params ...interface{}) error // Produces and populates dot data fields.
	Finalize() bool                      // Cleanup and shut down.
}

type DotProviderFactory struct {
	dp     DotProvider // Dot Provider interface
}


var dpf DotProviderFactory

func GetProviderInstance(dbSource string) DotProvider {
	return nil
}

func ReturnProviderInstance(dotProvider DotProvider) {
}
