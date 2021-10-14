package server

type Storage interface {
	Save(Config)
	Get(namespace, name string) Config
	List(namespace, name string) []Config
}
