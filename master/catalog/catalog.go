package catalog

import (
	"sync"
)

type Catalog struct {
	Name       string
	CreateTime int64
	spaces     map[string]*Space
	mutex      sync.RWMutex
}
