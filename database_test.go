package lungo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDatabaseClient(t *testing.T) {
	clientTest(t, func(t *testing.T, c IClient) {
		assert.Equal(t, c, c.Database("").Client())
	})
}

func TestDatabaseName(t *testing.T) {
	databaseTest(t, func(t *testing.T, d IDatabase) {
		assert.Equal(t, testDB, d.Name())
	})
}
