package router

type Routes struct {
	NobuType map[string]*TargetRoute
	Target   map[string]*TargetRoute
}

func NewRoutes() *Routes {
	return &Routes{}
}

type TargetRoute struct {
	Topic string
}

// NobuTypeToTargetRoute TODO: pending to implement
func (r *Routes) NobuTypeToTargetRoute(NobuType string) *TargetRoute {
	return nil
}
