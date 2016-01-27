package dpds

type DotProvider interface {
	Init()                                // Initialize the provider.
	Begin(lower int, upper int) bool      // Begin providing dots. 
	HasMore() bool                        // Are more dots available?
	Populate(params ...interface{}) error // Pointer to meta dot.
	Finalize() bool                       // Cleanup and shut down.
}

type DotProviderFactory struct {
	dp     DotProvider // Dot Provider interface
}

func (dpf DotProviderFactory) GetInstance() DotProvider {
	return nil
}


var dpf DotProviderFactory

func GetInstance() DotProvider {
	return dpf.GetInstance()
}

