package dpds

type DotProvider interface {
	Init(dbSource string)                // Initialize the provider.
    GetSource() string                   // Get current data source used to init provider.
	InitFields(tableName string, queryFields []string, valueFields []string, whereFields []string, preCommit func() (canCommit bool, err error), lowerBound int, upperBound int)
    Construct() bool                     // Enable ability to construct a dot provider data store.
    Create() bool                        // Enable ability to insert/create dot provider source.
    Update() bool                        // Enable ability to update a dot.
	Destroy() bool                       // Enable ability to destroy a provider source.
	Begin() bool                         // Begin providing all available dots.
	HasMore() bool                       // Are more dots available?
	Produce(params ...interface{}) error // Produces and populates dot data fields.
	Finalize() bool                      // Cleanup and shut down.
}

type DotProviderFactory struct {
	dp DotProvider // Dot Provider interface
}

var dpf DotProviderFactory

func GetProviderInstance(dbSource string) DotProvider {
	return nil
}

func ReturnProviderInstance(dotProvider DotProvider) {
}
