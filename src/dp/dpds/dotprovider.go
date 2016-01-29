package dpds

type DotProvider interface {
	Init()                                // Initialize the provider.
	Begin(lower int, upper int) bool      // Begin providing dots. 
	HasMore() bool                        // Are more dots available?
	Produce(params ...interface{}) error  // Produces and populates dot data fields.
	Finalize() bool                       // Cleanup and shut down.
}

type DotProviderFactory struct {
	dp     DotProvider // Dot Provider interface
}

func (dpf DotProviderFactory) GetInstance() DotProvider {
	return nil
}

var dpf DotProviderFactory

func GetProviderInstance() DotProvider {
	return dpf.GetInstance()
}

