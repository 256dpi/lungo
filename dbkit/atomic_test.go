package dbkit

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAtomicWriteFile(t *testing.T) {
	r := strings.NewReader("foo")

	err := AtomicWriteFile("./atomic", r, 0)
	assert.NoError(t, err)

	data, err := ioutil.ReadFile("./atomic")
	assert.NoError(t, err)
	assert.Equal(t, "foo", string(data))

	r = strings.NewReader("bar")

	err = AtomicWriteFile("./atomic", r, 0)
	assert.NoError(t, err)

	data, err = ioutil.ReadFile("./atomic")
	assert.NoError(t, err)
	assert.Equal(t, "bar", string(data))

	err = os.Remove("./atomic")
	assert.NoError(t, err)
}
