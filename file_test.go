package lungo

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/256dpi/lungo/mongokit"
)

func TestFile(t *testing.T) {
	catalog := NewCatalog()
	assert.NotNil(t, catalog)

	catalog.Namespaces[Oplog] = mongokit.NewCollection(false)
	catalog.Namespaces[Handle{"test", "foo.bar"}] = mongokit.NewCollection(false)

	file := BuildFile(catalog)
	assert.NotNil(t, file)

	catalog2, err := file.BuildCatalog()
	assert.Nil(t, err)
	assert.NotNil(t, catalog2)
}
