module github.com/256dpi/lungo

go 1.15

require (
	github.com/shopspring/decimal v1.2.0
	github.com/stretchr/testify v1.6.1
	github.com/tidwall/btree v0.5.0
	go.mongodb.org/mongo-driver v1.5.1
)

replace github.com/tidwall/btree v0.5.0 => github.com/256dpi/btree v0.0.0-20210519180815-0ff0e98e051e
