package lungo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestCollectionClone(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		c2, err := c.Clone()
		assert.NoError(t, err)
		assert.NotNil(t, c2)
	})
}

func TestCollectionCountDocuments(t *testing.T) {
	// missing database
	clientTest(t, func(t *testing.T, client IClient) {
		c := client.Database("not-existing").Collection("not-existing")
		num, err := c.CountDocuments(nil, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, int64(0), num)
	})

	// missing collection
	databaseTest(t, func(t *testing.T, d IDatabase) {
		num, err := d.Collection("not-existing").CountDocuments(nil, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, int64(0), num)
	})

	collectionTest(t, func(t *testing.T, c ICollection) {
		id1 := primitive.NewObjectID()
		id2 := primitive.NewObjectID()

		res1, err := c.InsertMany(nil, bson.A{
			bson.M{
				"_id": id1,
				"foo": "bar",
			},
			bson.M{
				"_id": id2,
				"bar": "baz",
			},
		})
		assert.NoError(t, err)
		assert.Len(t, res1.InsertedIDs, 2)

		// count all
		num, err := c.CountDocuments(nil, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, int64(2), num)

		// count first
		num, err = c.CountDocuments(nil, bson.M{}, options.Count().SetLimit(1))
		assert.NoError(t, err)
		assert.Equal(t, int64(1), num)

		// skip first
		num, err = c.CountDocuments(nil, bson.M{}, options.Count().SetSkip(1))
		assert.NoError(t, err)
		assert.Equal(t, int64(1), num)
	})
}

func TestCollectionDatabase(t *testing.T) {
	databaseTest(t, func(t *testing.T, d IDatabase) {
		assert.Equal(t, d, d.Collection("").Database())
	})
}

func TestCollectionDeleteMany(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		id1 := primitive.NewObjectID()
		id2 := primitive.NewObjectID()

		res1, err := c.InsertMany(nil, bson.A{
			bson.M{
				"_id": id1,
				"foo": "bar",
			},
			bson.M{
				"_id": id2,
				"bar": "baz",
			},
		})
		assert.NoError(t, err)
		assert.Len(t, res1.InsertedIDs, 2)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "bar",
			},
			{
				"_id": id2,
				"bar": "baz",
			},
		}, dumpCollection(c, false))

		// delete none
		res2, err := c.DeleteMany(nil, bson.M{
			"_id": "foo",
		})
		assert.NoError(t, err)
		assert.Equal(t, int64(0), res2.DeletedCount)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "bar",
			},
			{
				"_id": id2,
				"bar": "baz",
			},
		}, dumpCollection(c, false))

		// delete matching
		res2, err = c.DeleteMany(nil, bson.M{
			"_id": bson.M{"$in": bson.A{id1, id2}},
		})
		assert.NoError(t, err)
		assert.Equal(t, int64(2), res2.DeletedCount)
		assert.Equal(t, []bson.M{}, dumpCollection(c, false))
	})
}

func TestCollectionDeleteOne(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		id := primitive.NewObjectID()

		res1, err := c.InsertOne(nil, bson.M{
			"_id": id,
			"foo": "bar",
		})
		assert.NoError(t, err)
		assert.True(t, !res1.InsertedID.(primitive.ObjectID).IsZero())
		assert.Equal(t, []bson.M{
			{
				"_id": id,
				"foo": "bar",
			},
		}, dumpCollection(c, false))

		// delete none
		res2, err := c.DeleteOne(nil, bson.M{
			"_id": "foo",
		})
		assert.NoError(t, err)
		assert.Equal(t, int64(0), res2.DeletedCount)
		assert.Equal(t, []bson.M{
			{
				"_id": id,
				"foo": "bar",
			},
		}, dumpCollection(c, false))

		// delete one
		res2, err = c.DeleteOne(nil, bson.M{
			"_id": id,
		})
		assert.NoError(t, err)
		assert.Equal(t, int64(1), res2.DeletedCount)
		assert.Equal(t, []bson.M{}, dumpCollection(c, false))
	})
}

func TestCollectionDistinct(t *testing.T) {
	// missing database
	clientTest(t, func(t *testing.T, client IClient) {
		c := client.Database("not-existing").Collection("not-existing")
		res, err := c.Distinct(nil, "foo", bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, []interface{}{}, res)
	})

	// missing collection
	databaseTest(t, func(t *testing.T, d IDatabase) {
		res, err := d.Collection("not-existing").Distinct(nil, "foo", bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, []interface{}{}, res)
	})

	collectionTest(t, func(t *testing.T, c ICollection) {
		id1 := primitive.NewObjectID()
		id2 := primitive.NewObjectID()

		res1, err := c.InsertMany(nil, []interface{}{
			bson.M{
				"_id": id1,
				"foo": "bar",
			},
			bson.M{
				"_id": id2,
				"foo": "baz",
			},
		})
		assert.NoError(t, err)
		assert.Len(t, res1.InsertedIDs, 2)

		// distinct values
		res, err := c.Distinct(nil, "foo", bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, []interface{}{"bar", "baz"}, res)
	})
}

func TestCollectionDrop(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		_, err := c.InsertOne(nil, bson.M{
			"foo": "bar",
		})
		assert.NoError(t, err)
		assert.Equal(t, []bson.M{
			{
				"foo": "bar",
			},
		}, dumpCollection(c, true))

		err = c.Drop(nil)
		assert.NoError(t, err)
		assert.Equal(t, []bson.M{}, dumpCollection(c, true))
	})
}

func TestCollectionEstimatedDocumentCount(t *testing.T) {
	// missing database
	clientTest(t, func(t *testing.T, client IClient) {
		c := client.Database("not-existing").Collection("not-existing")
		num, err := c.EstimatedDocumentCount(nil)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), num)
	})

	// missing collection
	databaseTest(t, func(t *testing.T, d IDatabase) {
		num, err := d.Collection("not-existing").EstimatedDocumentCount(nil)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), num)
	})

	// with documents
	collectionTest(t, func(t *testing.T, c ICollection) {
		id1 := primitive.NewObjectID()
		id2 := primitive.NewObjectID()

		res1, err := c.InsertMany(nil, bson.A{
			bson.M{
				"_id": id1,
				"foo": "bar",
			},
			bson.M{
				"_id": id2,
				"bar": "baz",
			},
		})
		assert.NoError(t, err)
		assert.Len(t, res1.InsertedIDs, 2)

		num, err := c.EstimatedDocumentCount(nil)
		assert.NoError(t, err)
		assert.Equal(t, int64(2), num)
	})
}

func TestCollectionFind(t *testing.T) {
	// missing database
	clientTest(t, func(t *testing.T, client IClient) {
		c := client.Database("not-existing").Collection("not-existing")
		csr, err := c.Find(nil, bson.M{})
		assert.NoError(t, err)
		assert.NotNil(t, csr)
		assert.Equal(t, []bson.M{}, readAll(csr))
	})

	// missing collection
	databaseTest(t, func(t *testing.T, d IDatabase) {
		csr, err := d.Collection("not-existing").Find(nil, bson.M{})
		assert.NoError(t, err)
		assert.NotNil(t, csr)
		assert.Equal(t, []bson.M{}, readAll(csr))
	})

	collectionTest(t, func(t *testing.T, c ICollection) {
		id1 := primitive.NewObjectID()
		id2 := primitive.NewObjectID()

		res1, err := c.InsertMany(nil, bson.A{
			bson.M{
				"_id": id1,
				"foo": "bar",
			},
			bson.M{
				"_id": id2,
				"foo": "baz",
			},
		})
		assert.NoError(t, err)
		assert.Len(t, res1.InsertedIDs, 2)

		// find all
		csr, err := c.Find(nil, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "bar",
			},
			{
				"_id": id2,
				"foo": "baz",
			},
		}, readAll(csr))

		// find first
		csr, err = c.Find(nil, bson.M{}, options.Find().SetLimit(1))
		assert.NoError(t, err)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "bar",
			},
		}, readAll(csr))

		// sort all
		csr, err = c.Find(nil, bson.M{}, options.Find().SetSort(bson.M{
			"foo": -1,
		}))
		assert.NoError(t, err)
		assert.Equal(t, []bson.M{
			{
				"_id": id2,
				"foo": "baz",
			},
			{
				"_id": id1,
				"foo": "bar",
			},
		}, readAll(csr))

		// skip first
		csr, err = c.Find(nil, bson.M{}, options.Find().SetSkip(1))
		assert.NoError(t, err)
		assert.Equal(t, []bson.M{
			{
				"_id": id2,
				"foo": "baz",
			},
		}, readAll(csr))
	})
}

func TestCollectionFindOne(t *testing.T) {
	// missing database
	clientTest(t, func(t *testing.T, client IClient) {
		c := client.Database("not-existing").Collection("not-existing")
		res := c.FindOne(nil, bson.M{})
		assert.Error(t, res.Err())
		assert.Equal(t, mongo.ErrNoDocuments, res.Err())
	})

	// missing collection
	databaseTest(t, func(t *testing.T, d IDatabase) {
		res := d.Collection("not-existing").FindOne(nil, bson.M{})
		assert.Error(t, res.Err())
		assert.Equal(t, mongo.ErrNoDocuments, res.Err())
	})

	collectionTest(t, func(t *testing.T, c ICollection) {
		id1 := primitive.NewObjectID()
		id2 := primitive.NewObjectID()

		_, err := c.InsertMany(nil, bson.A{
			bson.M{
				"_id": id1,
				"foo": "bar",
			},
			bson.M{
				"_id": id2,
				"foo": "baz",
			},
		})
		assert.NoError(t, err)

		// fine one by id
		var doc bson.M
		err = c.FindOne(nil, bson.M{
			"_id": id1,
		}).Decode(&doc)
		assert.NoError(t, err)
		assert.Equal(t, bson.M{
			"_id": id1,
			"foo": "bar",
		}, doc)

		// find first
		err = c.FindOne(nil, bson.M{}).Decode(&doc)
		assert.NoError(t, err)
		assert.Equal(t, bson.M{
			"_id": id1,
			"foo": "bar",
		}, doc)

		// find first, sorted
		err = c.FindOne(nil, bson.M{}, options.FindOne().SetSort(bson.M{
			"foo": -1,
		})).Decode(&doc)
		assert.NoError(t, err)
		assert.Equal(t, bson.M{
			"_id": id2,
			"foo": "baz",
		}, doc)

		// skip first
		err = c.FindOne(nil, bson.M{}, options.FindOne().SetSkip(1)).Decode(&doc)
		assert.NoError(t, err)
		assert.Equal(t, bson.M{
			"_id": id2,
			"foo": "baz",
		}, doc)
	})
}

func TestCollectionFindOneAndDelete(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		id := primitive.NewObjectID()

		res1, err := c.InsertOne(nil, bson.M{
			"_id": id,
			"foo": "bar",
		})
		assert.NoError(t, err)
		assert.True(t, !res1.InsertedID.(primitive.ObjectID).IsZero())
		assert.Equal(t, []bson.M{
			{
				"_id": id,
				"foo": "bar",
			},
		}, dumpCollection(c, false))

		// missing
		err = c.FindOneAndDelete(nil, bson.M{
			"_id": "foo",
		}).Err()
		assert.Error(t, err)
		assert.Equal(t, mongo.ErrNoDocuments, err)
		assert.Equal(t, []bson.M{
			{
				"_id": id,
				"foo": "bar",
			},
		}, dumpCollection(c, false))

		// specific
		var doc bson.M
		err = c.FindOneAndDelete(nil, bson.M{
			"_id": id,
		}).Decode(&doc)
		assert.NoError(t, err)
		assert.Equal(t, bson.M{
			"_id": id,
			"foo": "bar",
		}, doc)
		assert.Equal(t, []bson.M{}, dumpCollection(c, false))
	})

	collectionTest(t, func(t *testing.T, c ICollection) {
		id1 := primitive.NewObjectID()
		id2 := primitive.NewObjectID()

		res1, err := c.InsertMany(nil, bson.A{
			bson.M{
				"_id": id1,
				"foo": "bar",
			},
			bson.M{
				"_id": id2,
				"foo": "baz",
			},
		})
		assert.NoError(t, err)
		assert.Len(t, res1.InsertedIDs, 2)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "bar",
			},
			{
				"_id": id2,
				"foo": "baz",
			},
		}, dumpCollection(c, false))

		// first from end
		var doc bson.M
		err = c.FindOneAndDelete(nil, bson.M{}, options.FindOneAndDelete().SetSort(bson.M{
			"foo": -1,
		})).Decode(&doc)
		assert.NoError(t, err)
		assert.Equal(t, bson.M{
			"_id": id2,
			"foo": "baz",
		}, doc)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "bar",
			},
		}, dumpCollection(c, false))
	})
}

func TestCollectionFindOneAndReplace(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		id := primitive.NewObjectID()

		res1, err := c.InsertOne(nil, bson.M{
			"_id": id,
			"foo": "bar",
		})
		assert.NoError(t, err)
		assert.True(t, !res1.InsertedID.(primitive.ObjectID).IsZero())
		assert.Equal(t, []bson.M{
			{
				"_id": id,
				"foo": "bar",
			},
		}, dumpCollection(c, false))

		// missing
		err = c.FindOneAndReplace(nil, bson.M{
			"_id": "foo",
		}, bson.M{}).Err()
		assert.Error(t, err)
		assert.Equal(t, mongo.ErrNoDocuments, err)
		assert.Equal(t, []bson.M{
			{
				"_id": id,
				"foo": "bar",
			},
		}, dumpCollection(c, false))

		// specific
		var doc bson.M
		err = c.FindOneAndReplace(nil, bson.M{
			"_id": id,
		}, bson.M{
			"_id": id,
			"foo": "baz",
		}).Decode(&doc)
		assert.NoError(t, err)
		assert.Equal(t, bson.M{
			"_id": id,
			"foo": "bar",
		}, doc)
		assert.Equal(t, []bson.M{
			{
				"_id": id,
				"foo": "baz",
			},
		}, dumpCollection(c, false))
	})

	collectionTest(t, func(t *testing.T, c ICollection) {
		id1 := primitive.NewObjectID()
		id2 := primitive.NewObjectID()

		res1, err := c.InsertMany(nil, bson.A{
			bson.M{
				"_id": id1,
				"foo": "bar",
			},
			bson.M{
				"_id": id2,
				"foo": "baz",
			},
		})
		assert.NoError(t, err)
		assert.Len(t, res1.InsertedIDs, 2)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "bar",
			},
			{
				"_id": id2,
				"foo": "baz",
			},
		}, dumpCollection(c, false))

		// first from end
		var doc bson.M
		err = c.FindOneAndReplace(nil, bson.M{}, bson.M{
			"foo": "quz",
		}, options.FindOneAndReplace().SetSort(bson.M{
			"foo": -1,
		})).Decode(&doc)
		assert.NoError(t, err)
		assert.Equal(t, bson.M{
			"_id": id2,
			"foo": "baz",
		}, doc)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "bar",
			},
			{
				"_id": id2,
				"foo": "quz",
			},
		}, dumpCollection(c, false))
	})
}

func TestCollectionFindOneAndUpdate(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		id := primitive.NewObjectID()

		res1, err := c.InsertOne(nil, bson.M{
			"_id": id,
			"foo": "bar",
		})
		assert.NoError(t, err)
		assert.True(t, !res1.InsertedID.(primitive.ObjectID).IsZero())
		assert.Equal(t, []bson.M{
			{
				"_id": id,
				"foo": "bar",
			},
		}, dumpCollection(c, false))

		// missing
		err = c.FindOneAndUpdate(nil, bson.M{
			"_id": "foo",
		}, bson.M{
			"$set": bson.M{
				"foo": "baz",
			},
		}).Err()
		assert.Error(t, err)
		assert.Equal(t, mongo.ErrNoDocuments, err)
		assert.Equal(t, []bson.M{
			{
				"_id": id,
				"foo": "bar",
			},
		}, dumpCollection(c, false))

		// specific
		var doc bson.M
		err = c.FindOneAndUpdate(nil, bson.M{
			"_id": id,
		}, bson.M{
			"$set": bson.M{
				"foo": "baz",
			},
		}).Decode(&doc)
		assert.NoError(t, err)
		assert.Equal(t, bson.M{
			"_id": id,
			"foo": "bar",
		}, doc)
		assert.Equal(t, []bson.M{
			{
				"_id": id,
				"foo": "baz",
			},
		}, dumpCollection(c, false))
	})

	collectionTest(t, func(t *testing.T, c ICollection) {
		id1 := primitive.NewObjectID()
		id2 := primitive.NewObjectID()

		res1, err := c.InsertMany(nil, bson.A{
			bson.M{
				"_id": id1,
				"foo": "bar",
			},
			bson.M{
				"_id": id2,
				"foo": "baz",
			},
		})
		assert.NoError(t, err)
		assert.Len(t, res1.InsertedIDs, 2)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "bar",
			},
			{
				"_id": id2,
				"foo": "baz",
			},
		}, dumpCollection(c, false))

		// first from end
		var doc bson.M
		err = c.FindOneAndUpdate(nil, bson.M{}, bson.M{
			"$set": bson.M{
				"foo": "quz",
			},
		}, options.FindOneAndUpdate().SetSort(bson.M{
			"foo": -1,
		})).Decode(&doc)
		assert.NoError(t, err)
		assert.Equal(t, bson.M{
			"_id": id2,
			"foo": "baz",
		}, doc)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "bar",
			},
			{
				"_id": id2,
				"foo": "quz",
			},
		}, dumpCollection(c, false))
	})
}

func TestCollectionInsertMany(t *testing.T) {
	// generated id
	collectionTest(t, func(t *testing.T, c ICollection) {
		res, err := c.InsertMany(nil, bson.A{
			bson.M{
				"foo": "bar",
			},
			bson.M{
				"bar": "baz",
			},
		})
		assert.NoError(t, err)
		assert.Len(t, res.InsertedIDs, 2)
		assert.Equal(t, []bson.M{
			{
				"foo": "bar",
			},
			{
				"bar": "baz",
			},
		}, dumpCollection(c, true))
	})

	// provided _id
	collectionTest(t, func(t *testing.T, c ICollection) {
		id1 := primitive.NewObjectID()
		id2 := primitive.NewObjectID()

		res, err := c.InsertMany(nil, bson.A{
			bson.M{
				"_id": id1,
				"foo": "bar",
			},
			bson.M{
				"_id": id2,
				"bar": "baz",
			},
		})
		assert.NoError(t, err)
		assert.Len(t, res.InsertedIDs, 2)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "bar",
			},
			{
				"_id": id2,
				"bar": "baz",
			},
		}, dumpCollection(c, false))

		// duplicate key
		res, err = c.InsertMany(nil, bson.A{
			bson.M{
				"_id": id1,
				"foo": "bar",
			},
		})
		assert.Error(t, err)
		// assert.Nli(t, res)  // TODO: mongo returns all ids in any case, bug?
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "bar",
			},
			{
				"_id": id2,
				"bar": "baz",
			},
		}, dumpCollection(c, false))
	})

	// complex _id
	collectionTest(t, func(t *testing.T, c ICollection) {
		id1 := bson.M{
			"some-id": "a",
		}

		res, err := c.InsertMany(nil, bson.A{
			bson.M{
				"_id": id1,
				"foo": "bar",
			},
		})
		assert.NoError(t, err)
		assert.Len(t, res.InsertedIDs, 1)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "bar",
			},
		}, dumpCollection(c, false))

		// duplicate key
		res, err = c.InsertMany(nil, bson.A{
			bson.M{
				"_id": id1,
				"foo": "bar",
			},
		})
		assert.Error(t, err)
		// assert.Nil(t, res) // TODO: mongo returns all ids in any case, bug?
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "bar",
			},
		}, dumpCollection(c, false))
	})

	// TODO: Test duplicate ids in request.
}

func TestCollectionInsertOne(t *testing.T) {
	// generated id
	collectionTest(t, func(t *testing.T, c ICollection) {
		res, err := c.InsertOne(nil, bson.M{
			"foo": "bar",
		})
		assert.NoError(t, err)
		assert.True(t, !res.InsertedID.(primitive.ObjectID).IsZero())
		assert.Equal(t, []bson.M{
			{
				"foo": "bar",
			},
		}, dumpCollection(c, true))
	})

	// provided _id
	collectionTest(t, func(t *testing.T, c ICollection) {
		id := primitive.NewObjectID()

		res, err := c.InsertOne(nil, bson.M{
			"_id": id,
			"foo": "bar",
		})
		assert.NoError(t, err)
		assert.True(t, !res.InsertedID.(primitive.ObjectID).IsZero())
		assert.Equal(t, []bson.M{
			{
				"_id": id,
				"foo": "bar",
			},
		}, dumpCollection(c, false))
	})

	// duplicate _id key
	collectionTest(t, func(t *testing.T, c ICollection) {
		id := primitive.NewObjectID()

		_, err := c.InsertOne(nil, bson.M{
			"_id": id,
			"foo": "bar",
		})
		assert.NoError(t, err)

		_, err = c.InsertOne(nil, bson.M{
			"_id": id,
			"foo": "baz",
		})
		assert.Error(t, err)

		assert.Equal(t, []bson.M{
			{
				"_id": id,
				"foo": "bar",
			},
		}, dumpCollection(c, false))
	})
}

func TestCollectionName(t *testing.T) {
	databaseTest(t, func(t *testing.T, d IDatabase) {
		assert.Equal(t, "foo", d.Collection("foo").Name())
	})
}

func TestCollectionReplaceOne(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		id1 := primitive.NewObjectID()
		id2 := primitive.NewObjectID()

		res1, err := c.InsertMany(nil, bson.A{
			bson.M{
				"_id": id1,
				"foo": "bar",
			},
			bson.M{
				"_id": id2,
				"bar": "baz",
			},
		})
		assert.NoError(t, err)
		assert.Len(t, res1.InsertedIDs, 2)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "bar",
			},
			{
				"_id": id2,
				"bar": "baz",
			},
		}, dumpCollection(c, false))

		// replace first document
		res2, err := c.ReplaceOne(nil, bson.M{}, bson.M{
			"_id": id1,
			"foo": "baz",
		})
		assert.NoError(t, err)
		assert.Equal(t, int64(1), res2.MatchedCount)
		assert.Equal(t, int64(1), res2.ModifiedCount)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "baz",
			},
			{
				"_id": id2,
				"bar": "baz",
			},
		}, dumpCollection(c, false))

		// replace second document
		res2, err = c.ReplaceOne(nil, bson.M{
			"_id": id2,
		}, bson.M{
			"_id": id2,
			"bar": "quz",
		})
		assert.NoError(t, err)
		assert.Equal(t, int64(1), res2.MatchedCount)
		assert.Equal(t, int64(1), res2.ModifiedCount)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "baz",
			},
			{
				"_id": id2,
				"bar": "quz",
			},
		}, dumpCollection(c, false))

		// duplicate key
		res2, err = c.ReplaceOne(nil, bson.M{
			"_id": id2,
		}, bson.M{
			"_id": id1,
			"bar": "qux",
		})
		assert.Error(t, err)
		assert.Nil(t, res2)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "baz",
			},
			{
				"_id": id2,
				"bar": "quz",
			},
		}, dumpCollection(c, false))
	})
}

func TestCollectionUpdateMany(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		id1 := primitive.NewObjectID()
		id2 := primitive.NewObjectID()

		res1, err := c.InsertMany(nil, bson.A{
			bson.M{
				"_id": id1,
				"foo": "bar",
			},
			bson.M{
				"_id": id2,
				"foo": "baz",
			},
		})
		assert.NoError(t, err)
		assert.Len(t, res1.InsertedIDs, 2)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "bar",
			},
			{
				"_id": id2,
				"foo": "baz",
			},
		}, dumpCollection(c, false))

		// update single document
		res2, err := c.UpdateMany(nil, bson.M{
			"foo": "bar",
		}, bson.M{
			"$set": bson.M{
				"foo": "baz",
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, int64(1), res2.MatchedCount)
		assert.Equal(t, int64(1), res2.ModifiedCount)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "baz",
			},
			{
				"_id": id2,
				"foo": "baz",
			},
		}, dumpCollection(c, false))

		// update all documents
		res2, err = c.UpdateMany(nil, bson.M{}, bson.M{
			"$set": bson.M{
				"foo": "quz",
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, int64(2), res2.MatchedCount)
		assert.Equal(t, int64(2), res2.ModifiedCount)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "quz",
			},
			{
				"_id": id2,
				"foo": "quz",
			},
		}, dumpCollection(c, false))
	})
}

func TestCollectionUpdateOne(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		id1 := primitive.NewObjectID()
		id2 := primitive.NewObjectID()

		res1, err := c.InsertMany(nil, bson.A{
			bson.M{
				"_id": id1,
				"foo": "bar",
			},
			bson.M{
				"_id": id2,
				"foo": "baz",
			},
		})
		assert.NoError(t, err)
		assert.Len(t, res1.InsertedIDs, 2)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "bar",
			},
			{
				"_id": id2,
				"foo": "baz",
			},
		}, dumpCollection(c, false))

		// update specific document
		res2, err := c.UpdateOne(nil, bson.M{
			"foo": "bar",
		}, bson.M{
			"$set": bson.M{
				"foo": "baz",
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, int64(1), res2.MatchedCount)
		assert.Equal(t, int64(1), res2.ModifiedCount)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "baz",
			},
			{
				"_id": id2,
				"foo": "baz",
			},
		}, dumpCollection(c, false))

		// update first documents
		res2, err = c.UpdateOne(nil, bson.M{}, bson.M{
			"$set": bson.M{
				"foo": "quz",
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, int64(1), res2.MatchedCount)
		assert.Equal(t, int64(1), res2.ModifiedCount)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "quz",
			},
			{
				"_id": id2,
				"foo": "baz",
			},
		}, dumpCollection(c, false))
	})
}
