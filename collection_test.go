package lungo

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestCollectionBulkWrite(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		id1 := bson.NewObjectID()
		id2 := bson.NewObjectID()

		models := []mongo.WriteModel{
			mongo.NewInsertOneModel().SetDocument(bson.M{
				"_id": id1,
				"foo": "bar",
			}),
			mongo.NewUpdateOneModel().SetFilter(bson.M{
				"foo": "bar",
			}).SetUpdate(bson.M{
				"$set": bson.M{
					"foo": "baz",
				},
			}),
			mongo.NewUpdateManyModel().SetFilter(bson.M{
				"bar": "baz",
			}).SetUpdate(bson.M{
				"$set": bson.M{
					"_id": id2,
				},
			}).SetUpsert(true),
			mongo.NewReplaceOneModel().SetFilter(bson.M{
				"bar": "baz",
			}).SetReplacement(bson.M{
				"baz": "quz",
			}),
		}

		res, err := c.BulkWrite(nil, models)
		assert.NoError(t, err)
		res.Acknowledged = false
		assert.Equal(t, &mongo.BulkWriteResult{
			InsertedCount: 1,
			MatchedCount:  2,
			ModifiedCount: 2,
			UpsertedCount: 1,
			UpsertedIDs: map[int64]interface{}{
				2: id2,
			},
		}, res)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "baz",
			},
			{
				"_id": id2,
				"baz": "quz",
			},
		}, dumpCollection(c, false))

		models = []mongo.WriteModel{
			mongo.NewDeleteOneModel().SetFilter(bson.M{
				"foo": "baz",
			}),
			mongo.NewDeleteManyModel().SetFilter(bson.M{
				"baz": "quz",
			}),
		}

		res, err = c.BulkWrite(nil, models)
		assert.NoError(t, err)
		res.Acknowledged = false
		assert.Equal(t, &mongo.BulkWriteResult{
			DeletedCount: 2,
			UpsertedIDs:  map[int64]interface{}{},
		}, res)
		assert.Equal(t, []bson.M{}, dumpCollection(c, false))

		models = []mongo.WriteModel{
			mongo.NewInsertOneModel().SetDocument(bson.M{
				"_id": id1,
				"foo": "bar",
			}),
			mongo.NewUpdateOneModel().SetFilter(bson.M{
				"foo": "bar",
			}).SetUpdate(bson.M{
				"$foo": bson.M{
					"foo": "baz",
				},
			}),
		}

		res, err = c.BulkWrite(nil, models)
		assert.Error(t, err)
		assert.Equal(t, &mongo.BulkWriteResult{
			InsertedCount: 1,
			UpsertedIDs:   map[int64]interface{}{},
		}, res)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "bar",
			},
		}, dumpCollection(c, false))
	})
}

func TestCollectionClone(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		c2 := c.Clone()
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
		id1 := bson.NewObjectID()
		id2 := bson.NewObjectID()

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
	// missing database
	clientTest(t, func(t *testing.T, client IClient) {
		c := client.Database("not-existing").Collection("not-existing")
		res, err := c.DeleteMany(nil, bson.M{})
		assert.NoError(t, err)
		assert.NotNil(t, res)
		res.Acknowledged = false
		assert.Equal(t, &mongo.DeleteResult{}, res)
	})

	// missing collection
	databaseTest(t, func(t *testing.T, d IDatabase) {
		res, err := d.Collection("not-existing").DeleteMany(nil, bson.M{})
		assert.NoError(t, err)
		assert.NotNil(t, res)
		res.Acknowledged = false
		assert.Equal(t, &mongo.DeleteResult{}, res)
	})

	collectionTest(t, func(t *testing.T, c ICollection) {
		id1 := bson.NewObjectID()
		id2 := bson.NewObjectID()

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
	// missing database
	clientTest(t, func(t *testing.T, client IClient) {
		c := client.Database("not-existing").Collection("not-existing")
		res, err := c.DeleteOne(nil, bson.M{})
		assert.NoError(t, err)
		assert.NotNil(t, res)
		res.Acknowledged = false
		assert.Equal(t, &mongo.DeleteResult{}, res)
	})

	// missing collection
	databaseTest(t, func(t *testing.T, d IDatabase) {
		res, err := d.Collection("not-existing").DeleteOne(nil, bson.M{})
		assert.NoError(t, err)
		assert.NotNil(t, res)
		res.Acknowledged = false
		assert.Equal(t, &mongo.DeleteResult{}, res)
	})

	collectionTest(t, func(t *testing.T, c ICollection) {
		id := bson.NewObjectID()

		res1, err := c.InsertOne(nil, bson.M{
			"_id": id,
			"foo": "bar",
		})
		assert.NoError(t, err)
		assert.True(t, !res1.InsertedID.(bson.ObjectID).IsZero())
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
		id1 := bson.NewObjectID()
		id2 := bson.NewObjectID()

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
		id1 := bson.NewObjectID()
		id2 := bson.NewObjectID()

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
		id1 := bson.NewObjectID()
		id2 := bson.NewObjectID()
		id3 := bson.NewObjectID()

		res1, err := c.InsertMany(nil, bson.A{
			bson.M{
				"_id": id1,
				"n":   2,
				"foo": "bar",
			},
			bson.M{
				"_id": id2,
				"n":   3,
				"foo": "baz",
			},
			bson.M{
				"_id": id3,
				"n":   1,
				"foo": "qux",
			},
		})
		assert.NoError(t, err)
		assert.Len(t, res1.InsertedIDs, 3)

		// find all
		csr, err := c.Find(nil, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"n":   int32(2),
				"foo": "bar",
			},
			{
				"_id": id2,
				"n":   int32(3),
				"foo": "baz",
			},
			{
				"_id": id3,
				"n":   int32(1),
				"foo": "qux",
			},
		}, readAll(csr))

		// find first
		csr, err = c.Find(nil, bson.M{}, options.Find().SetLimit(1))
		assert.NoError(t, err)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"n":   int32(2),
				"foo": "bar",
			},
		}, readAll(csr))

		// sort all
		csr, err = c.Find(nil, bson.M{}, options.Find().SetSort(bson.M{
			"n": 1,
		}))
		assert.NoError(t, err)
		assert.Equal(t, []bson.M{
			{
				"_id": id3,
				"n":   int32(1),
				"foo": "qux",
			},
			{
				"_id": id1,
				"n":   int32(2),
				"foo": "bar",
			},
			{
				"_id": id2,
				"n":   int32(3),
				"foo": "baz",
			},
		}, readAll(csr))

		// skip first
		csr, err = c.Find(nil, bson.M{}, options.Find().SetSkip(1))
		assert.NoError(t, err)
		assert.Equal(t, []bson.M{
			{
				"_id": id2,
				"n":   int32(3),
				"foo": "baz",
			},
			{
				"_id": id3,
				"n":   int32(1),
				"foo": "qux",
			},
		}, readAll(csr))

		// filter, sort, skip and limit
		csr, err = c.Find(nil, bson.M{
			"n": bson.M{
				"$gt": 1,
			},
		}, options.Find().SetSort(bson.M{
			"n": 1,
		}).SetSkip(1).SetLimit(1))
		assert.NoError(t, err)
		assert.Equal(t, []bson.M{
			{
				"_id": id2,
				"n":   int32(3),
				"foo": "baz",
			},
		}, readAll(csr))

		// cursor
		var m bson.M
		csr, err = c.Find(nil, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, 3, csr.RemainingBatchLength())
		err = csr.Decode(&m)
		assert.Equal(t, io.EOF, err)
		assert.Nil(t, m)
		assert.NoError(t, csr.Err())
		i := 0
		for csr.Next(nil) {
			err = csr.Decode(&m)
			assert.NoError(t, err)
			assert.NotEqual(t, bson.M{}, m)
			assert.NoError(t, csr.Err())
			i++
		}
		assert.Equal(t, 3, i)
		assert.Equal(t, 0, csr.RemainingBatchLength())
		err = csr.Decode(&m)
		assert.NoError(t, err)
		assert.NotEqual(t, bson.M{}, m)
		assert.NoError(t, csr.Err())
		err = csr.Close(nil)
		assert.NoError(t, err)
		assert.NoError(t, csr.Err())
		err = csr.Decode(&m)
		assert.NoError(t, err)
		assert.NotEqual(t, bson.M{}, m)
		assert.NoError(t, csr.Err())

		// project all
		csr, err = c.Find(nil, bson.M{}, options.Find().SetProjection(bson.M{"_id": 0, "foo": 1}))
		assert.NoError(t, err)
		assert.Equal(t, []bson.M{
			{
				"foo": "bar",
			},
			{
				"foo": "baz",
			},
			{
				"foo": "qux",
			},
		}, readAll(csr))
	})
}

func TestCollectionFindSortArrayValuedField(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		_, err := c.InsertMany(nil, bson.A{
			bson.D{{Key: "tag", Value: "a"}, {Key: "v", Value: bson.A{int32(3), int32(1)}}},
			bson.D{{Key: "tag", Value: "b"}, {Key: "v", Value: bson.A{int32(2)}}},
			bson.D{{Key: "tag", Value: "c"}, {Key: "v", Value: bson.A{int32(5), int32(0)}}},
		})
		assert.NoError(t, err)

		// ascending: min(a)=1, min(b)=2, min(c)=0 → c, a, b
		csr, err := c.Find(nil, bson.M{}, options.Find().SetSort(bson.D{{Key: "v", Value: 1}}))
		assert.NoError(t, err)
		got := readAll(csr)
		tags := make([]string, len(got))
		for i, d := range got {
			tags[i] = d["tag"].(string)
		}
		assert.Equal(t, []string{"c", "a", "b"}, tags)

		// descending: max(a)=3, max(b)=2, max(c)=5 → c, a, b
		csr, err = c.Find(nil, bson.M{}, options.Find().SetSort(bson.D{{Key: "v", Value: -1}}))
		assert.NoError(t, err)
		got = readAll(csr)
		tags = make([]string, len(got))
		for i, d := range got {
			tags[i] = d["tag"].(string)
		}
		assert.Equal(t, []string{"c", "a", "b"}, tags)
	})
}

func TestCollectionFindOne(t *testing.T) {
	// missing database
	clientTest(t, func(t *testing.T, client IClient) {
		c := client.Database("not-existing").Collection("not-existing")
		res := c.FindOne(nil, bson.M{})
		assert.Error(t, res.Err())
		assert.Equal(t, ErrNoDocuments, res.Err())
	})

	// missing collection
	databaseTest(t, func(t *testing.T, d IDatabase) {
		res := d.Collection("not-existing").FindOne(nil, bson.M{})
		assert.Error(t, res.Err())
		assert.Equal(t, ErrNoDocuments, res.Err())
	})

	collectionTest(t, func(t *testing.T, c ICollection) {
		id1 := bson.NewObjectID()
		id2 := bson.NewObjectID()

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
		doc = nil
		err = c.FindOne(nil, bson.M{}).Decode(&doc)
		assert.NoError(t, err)
		assert.Equal(t, bson.M{
			"_id": id1,
			"foo": "bar",
		}, doc)

		// find first, sorted
		doc = nil
		err = c.FindOne(nil, bson.M{}, options.FindOne().SetSort(bson.M{
			"foo": -1,
		})).Decode(&doc)
		assert.NoError(t, err)
		assert.Equal(t, bson.M{
			"_id": id2,
			"foo": "baz",
		}, doc)

		// skip first
		doc = nil
		err = c.FindOne(nil, bson.M{}, options.FindOne().SetSkip(1)).Decode(&doc)
		assert.NoError(t, err)
		assert.Equal(t, bson.M{
			"_id": id2,
			"foo": "baz",
		}, doc)

		// project
		doc = nil
		err = c.FindOne(nil, bson.M{}, options.FindOne().SetProjection(bson.M{"_id": 0})).Decode(&doc)
		assert.NoError(t, err)
		assert.Equal(t, bson.M{
			"foo": "bar",
		}, doc)
	})
}

func TestCollectionFindOneAndDelete(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		id := bson.NewObjectID()

		res1, err := c.InsertOne(nil, bson.M{
			"_id": id,
			"foo": "bar",
		})
		assert.NoError(t, err)
		assert.True(t, !res1.InsertedID.(bson.ObjectID).IsZero())
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
		assert.Equal(t, ErrNoDocuments, err)
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
		id1 := bson.NewObjectID()
		id2 := bson.NewObjectID()

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

	collectionTest(t, func(t *testing.T, c ICollection) {
		id := bson.NewObjectID()

		_, err := c.InsertOne(nil, bson.M{
			"_id": id,
			"foo": "bar",
		})
		assert.NoError(t, err)

		// project
		var doc bson.M
		err = c.FindOneAndDelete(nil, bson.M{
			"_id": id,
		}, options.FindOneAndDelete().SetProjection(bson.M{"_id": 0})).Decode(&doc)
		assert.NoError(t, err)
		assert.Equal(t, bson.M{
			"foo": "bar",
		}, doc)
		assert.Equal(t, []bson.M{}, dumpCollection(c, false))
	})
}

func TestCollectionFindOneAndReplace(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		id := bson.NewObjectID()

		res1, err := c.InsertOne(nil, bson.M{
			"_id": id,
			"foo": "bar",
		})
		assert.NoError(t, err)
		assert.True(t, !res1.InsertedID.(bson.ObjectID).IsZero())
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
		assert.Equal(t, ErrNoDocuments, err)
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

		// specific after
		doc = nil
		err = c.FindOneAndReplace(nil, bson.M{
			"_id": id,
		}, bson.M{
			"_id": id,
			"foo": "quz",
		}, options.FindOneAndReplace().SetReturnDocument(options.After)).Decode(&doc)
		assert.NoError(t, err)
		assert.Equal(t, bson.M{
			"_id": id,
			"foo": "quz",
		}, doc)
		assert.Equal(t, []bson.M{
			{
				"_id": id,
				"foo": "quz",
			},
		}, dumpCollection(c, false))
	})

	collectionTest(t, func(t *testing.T, c ICollection) {
		id1 := bson.NewObjectID()
		id2 := bson.NewObjectID()

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

	collectionTest(t, func(t *testing.T, c ICollection) {
		id := bson.NewObjectID()

		_, err := c.InsertOne(nil, bson.M{
			"_id": id,
			"foo": "bar",
		})
		assert.NoError(t, err)

		// project before
		var doc bson.M
		err = c.FindOneAndReplace(nil, bson.M{
			"_id": id,
		}, bson.M{
			"_id": id,
			"foo": "baz",
		}, options.FindOneAndReplace().SetProjection(bson.M{"_id": 0})).Decode(&doc)
		assert.NoError(t, err)
		assert.Equal(t, bson.M{
			"foo": "bar",
		}, doc)

		// project after
		doc = nil
		err = c.FindOneAndReplace(nil, bson.M{
			"_id": id,
		}, bson.M{
			"_id": id,
			"foo": "quz",
		}, options.FindOneAndReplace().SetReturnDocument(options.After).SetProjection(bson.M{"_id": 0})).Decode(&doc)
		assert.NoError(t, err)
		assert.Equal(t, bson.M{
			"foo": "quz",
		}, doc)
	})
}

func TestCollectionFindOneAndReplaceUpsert(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		id1 := bson.NewObjectID()
		id2 := bson.NewObjectID()

		// generated id before
		var out bson.M
		err := c.FindOneAndReplace(nil, bson.M{
			"_id": id1,
		}, bson.M{
			"_id": id1,
			"bar": "baz",
		}, options.FindOneAndReplace().SetUpsert(true)).Decode(&out)
		assert.Error(t, err)
		assert.Equal(t, ErrNoDocuments, err)
		assert.Nil(t, out)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"bar": "baz",
			},
		}, dumpCollection(c, false))

		// generated id after
		out = nil
		err = c.FindOneAndReplace(nil, bson.M{
			"_id": id2,
		}, bson.M{
			"_id": id2,
			"bar": "baz",
		}, options.FindOneAndReplace().SetUpsert(true).SetReturnDocument(options.After)).Decode(&out)
		assert.NoError(t, err)
		assert.Equal(t, bson.M{
			"_id": id2,
			"bar": "baz",
		}, out)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"bar": "baz",
			},
			{
				"_id": id2,
				"bar": "baz",
			},
		}, dumpCollection(c, false))
	})
}

func TestCollectionFindOneAndUpdate(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		id := bson.NewObjectID()

		res1, err := c.InsertOne(nil, bson.M{
			"_id": id,
			"foo": "bar",
		})
		assert.NoError(t, err)
		assert.True(t, !res1.InsertedID.(bson.ObjectID).IsZero())
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
		assert.Equal(t, ErrNoDocuments, err)
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

		// specific after
		doc = nil
		err = c.FindOneAndUpdate(nil, bson.M{
			"_id": id,
		}, bson.M{
			"$set": bson.M{
				"foo": "quz",
			},
		}, options.FindOneAndUpdate().SetReturnDocument(options.After)).Decode(&doc)
		assert.NoError(t, err)
		assert.Equal(t, bson.M{
			"_id": id,
			"foo": "quz",
		}, doc)
		assert.Equal(t, []bson.M{
			{
				"_id": id,
				"foo": "quz",
			},
		}, dumpCollection(c, false))
	})

	collectionTest(t, func(t *testing.T, c ICollection) {
		id1 := bson.NewObjectID()
		id2 := bson.NewObjectID()

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

	collectionTest(t, func(t *testing.T, c ICollection) {
		id := bson.NewObjectID()

		_, err := c.InsertOne(nil, bson.M{
			"_id": id,
			"foo": "bar",
		})
		assert.NoError(t, err)

		// project before
		var doc bson.M
		err = c.FindOneAndUpdate(nil, bson.M{
			"_id": id,
		}, bson.M{
			"$set": bson.M{
				"foo": "baz",
			},
		}, options.FindOneAndUpdate().SetProjection(bson.M{"_id": 0})).Decode(&doc)
		assert.NoError(t, err)
		assert.Equal(t, bson.M{
			"foo": "bar",
		}, doc)

		// project after
		doc = nil
		err = c.FindOneAndUpdate(nil, bson.M{
			"_id": id,
		}, bson.M{
			"$set": bson.M{
				"foo": "quz",
			},
		}, options.FindOneAndUpdate().SetProjection(bson.M{"_id": 0}).SetReturnDocument(options.After)).Decode(&doc)
		assert.NoError(t, err)
		assert.Equal(t, bson.M{
			"foo": "quz",
		}, doc)
		assert.Equal(t, []bson.M{
			{
				"_id": id,
				"foo": "quz",
			},
		}, dumpCollection(c, false))
	})
}

func TestCollectionFindOneAndUpdateUpsert(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		id1 := bson.NewObjectID()
		id2 := bson.NewObjectID()

		// generated id before
		var out bson.M
		err := c.FindOneAndUpdate(nil, bson.M{
			"_id": id1,
		}, bson.M{
			"$set": bson.M{
				"bar": "baz",
			},
			"$setOnInsert": bson.M{
				"baz": "quz",
			},
		}, options.FindOneAndUpdate().SetUpsert(true)).Decode(&out)
		assert.Error(t, err)
		assert.Equal(t, ErrNoDocuments, err)
		assert.Nil(t, out)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"bar": "baz",
				"baz": "quz",
			},
		}, dumpCollection(c, false))

		// generated id after
		out = nil
		err = c.FindOneAndUpdate(nil, bson.M{
			"_id": id2,
		}, bson.M{
			"$set": bson.M{
				"bar": "baz",
			},
			"$setOnInsert": bson.M{
				"baz": "quz",
			},
		}, options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)).Decode(&out)
		assert.NoError(t, err)
		assert.Equal(t, bson.M{
			"_id": id2,
			"bar": "baz",
			"baz": "quz",
		}, out)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"bar": "baz",
				"baz": "quz",
			},
			{
				"_id": id2,
				"bar": "baz",
				"baz": "quz",
			},
		}, dumpCollection(c, false))

		// update after
		out = nil
		err = c.FindOneAndUpdate(nil, bson.M{
			"_id": id2,
		}, bson.M{
			"$set": bson.M{
				"bar": "baaz",
			},
			"$setOnInsert": bson.M{
				"baz": "quuz",
			},
		}, options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)).Decode(&out)
		assert.NoError(t, err)
		assert.Equal(t, bson.M{
			"_id": id2,
			"bar": "baaz",
			"baz": "quz",
		}, out)
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
		id1 := bson.NewObjectID()
		id2 := bson.NewObjectID()

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

		// existing _id
		res, err = c.InsertMany(nil, bson.A{
			bson.M{
				"_id": id1,
				"foo": "bar",
			},
		})
		assert.Error(t, err)
		assert.Len(t, res.InsertedIDs, 0)
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

		// existing _id
		res, err = c.InsertMany(nil, bson.A{
			bson.M{
				"_id": id1,
				"foo": "bar",
			},
		})
		assert.Error(t, err)
		assert.Len(t, res.InsertedIDs, 0)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "bar",
			},
		}, dumpCollection(c, false))
	})

	// duplicate _id ordered
	collectionTest(t, func(t *testing.T, c ICollection) {
		id1 := bson.NewObjectID()
		id2 := bson.NewObjectID()

		res, err := c.InsertMany(nil, bson.A{
			bson.M{
				"_id": id1,
				"foo": "bar",
			},
			bson.M{
				"_id": id1,
				"foo": "bar",
			},
			bson.M{
				"_id": id2,
				"bar": "baz",
			},
		})
		assert.Error(t, err)
		assert.Len(t, res.InsertedIDs, 1)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "bar",
			},
		}, dumpCollection(c, false))
	})

	// duplicate _id unordered
	collectionTest(t, func(t *testing.T, c ICollection) {
		id1 := bson.NewObjectID()
		id2 := bson.NewObjectID()

		res, err := c.InsertMany(nil, bson.A{
			bson.M{
				"_id": id1,
				"foo": "bar",
			},
			bson.M{
				"_id": id1,
				"foo": "bar",
			},
			bson.M{
				"_id": id2,
				"bar": "baz",
			},
		}, options.InsertMany().SetOrdered(false))
		assert.Error(t, err)
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
	})
}

func TestCollectionInsertOne(t *testing.T) {
	// generated id
	collectionTest(t, func(t *testing.T, c ICollection) {
		res, err := c.InsertOne(nil, bson.M{
			"foo": "bar",
		})
		assert.NoError(t, err)
		assert.True(t, !res.InsertedID.(bson.ObjectID).IsZero())
		assert.Equal(t, []bson.M{
			{
				"foo": "bar",
			},
		}, dumpCollection(c, true))
	})

	// provided _id
	collectionTest(t, func(t *testing.T, c ICollection) {
		id := bson.NewObjectID()

		res, err := c.InsertOne(nil, bson.M{
			"_id": id,
			"foo": "bar",
		})
		assert.NoError(t, err)
		assert.True(t, !res.InsertedID.(bson.ObjectID).IsZero())
		assert.Equal(t, []bson.M{
			{
				"_id": id,
				"foo": "bar",
			},
		}, dumpCollection(c, false))
	})

	// duplicate _id key
	collectionTest(t, func(t *testing.T, c ICollection) {
		id := bson.NewObjectID()

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
		id1 := bson.NewObjectID()
		id2 := bson.NewObjectID()

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

		// invalid _id mutation
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

		// operator-style replacement must be rejected
		res2, err = c.ReplaceOne(nil, bson.M{
			"_id": id1,
		}, bson.M{
			"$set": bson.M{"foo": "qux"},
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

func TestCollectionReplaceOneUpsert(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		id := bson.NewObjectID()

		// generated id
		res, err := c.ReplaceOne(nil, bson.M{
			"_id": id,
		}, bson.M{
			"_id": id,
			"bar": "baz",
		}, options.Replace().SetUpsert(true))
		assert.NoError(t, err)
		res.Acknowledged = false
		assert.Equal(t, &mongo.UpdateResult{
			UpsertedCount: 1,
			UpsertedID:    id,
		}, res)
		assert.Equal(t, []bson.M{
			{
				"_id": id,
				"bar": "baz",
			},
		}, dumpCollection(c, false))
	})
}

func TestCollectionUpdateByID(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		id1 := bson.NewObjectID()
		id2 := bson.NewObjectID()

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
		res2, err := c.UpdateByID(nil, id1, bson.M{
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
	})
}

func TestCollectionUpdateMany(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		id1 := bson.NewObjectID()
		id2 := bson.NewObjectID()

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

		// invalid _id mutation
		res2, err = c.UpdateMany(nil, bson.M{
			"_id": id1,
		}, bson.M{
			"$set": bson.M{
				"_id": id2,
			},
		})
		assert.Error(t, err)
		// assert.Nil(t, res2) <-- mongo returns result as well
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

func TestCollectionUpdateManyUpsert(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		id := bson.NewObjectID()

		// generated id
		res, err := c.UpdateMany(nil, bson.M{
			"_id": id,
		}, bson.M{
			"$set": bson.M{
				"bar": "baz",
			},
			"$setOnInsert": bson.M{
				"baz": "quz",
			},
		}, options.UpdateMany().SetUpsert(true))
		assert.NoError(t, err)
		res.Acknowledged = false
		assert.Equal(t, &mongo.UpdateResult{
			UpsertedCount: 1,
			UpsertedID:    id,
		}, res)
		assert.Equal(t, []bson.M{
			{
				"_id": id,
				"bar": "baz",
				"baz": "quz",
			},
		}, dumpCollection(c, false))
	})
}

func TestCollectionUpdateOne(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		id1 := bson.NewObjectID()
		id2 := bson.NewObjectID()

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

		// invalid _id mutation
		res2, err = c.UpdateOne(nil, bson.M{
			"_id": id1,
		}, bson.M{
			"$set": bson.M{
				"_id": id2,
			},
		})
		assert.Error(t, err)
		assert.Nil(t, res2)
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

func TestCollectionUpdateOneUpsert(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		id := bson.NewObjectID()

		// generated id
		res, err := c.UpdateOne(nil, bson.M{
			"_id": id,
		}, bson.M{
			"$set": bson.M{
				"bar": "baz",
			},
			"$setOnInsert": bson.M{
				"baz": "quz",
			},
		}, options.UpdateOne().SetUpsert(true))
		assert.NoError(t, err)
		res.Acknowledged = false
		assert.Equal(t, &mongo.UpdateResult{
			UpsertedCount: 1,
			UpsertedID:    id,
		}, res)
		assert.Equal(t, []bson.M{
			{
				"_id": id,
				"bar": "baz",
				"baz": "quz",
			},
		}, dumpCollection(c, false))
	})
}

func TestCollectionUpdateOneUpsertNoOpMatch(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		id := bson.NewObjectID()

		_, err := c.InsertOne(nil, bson.D{
			{Key: "_id", Value: id},
			{Key: "foo", Value: "bar"},
		})
		assert.NoError(t, err)

		// $set to an identical value with upsert=true: must match (not insert)
		// and must report ModifiedCount=0
		res, err := c.UpdateOne(nil, bson.D{{Key: "_id", Value: id}}, bson.D{
			{Key: "$set", Value: bson.D{{Key: "foo", Value: "bar"}}},
		}, options.UpdateOne().SetUpsert(true))
		assert.NoError(t, err)
		res.Acknowledged = false
		assert.Equal(t, &mongo.UpdateResult{
			MatchedCount: 1,
		}, res)
		assert.Equal(t, []bson.M{
			{
				"_id": id,
				"foo": "bar",
			},
		}, dumpCollection(c, false))
	})
}

func TestCollectionReplaceOneUpsertNoOpMatch(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		id := bson.NewObjectID()

		_, err := c.InsertOne(nil, bson.D{
			{Key: "_id", Value: id},
			{Key: "foo", Value: "bar"},
		})
		assert.NoError(t, err)

		// replace with a byte-identical document with upsert=true: must match
		// (not insert) and must report ModifiedCount=0
		res, err := c.ReplaceOne(nil, bson.D{{Key: "_id", Value: id}}, bson.D{
			{Key: "_id", Value: id},
			{Key: "foo", Value: "bar"},
		}, options.Replace().SetUpsert(true))
		assert.NoError(t, err)
		res.Acknowledged = false
		assert.Equal(t, &mongo.UpdateResult{
			MatchedCount: 1,
		}, res)
		assert.Equal(t, []bson.M{
			{
				"_id": id,
				"foo": "bar",
			},
		}, dumpCollection(c, false))
	})
}

func TestCollectionUpdateManyUpsertNoOpMatch(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		_, err := c.InsertMany(nil, bson.A{
			bson.D{{Key: "foo", Value: "bar"}},
			bson.D{{Key: "foo", Value: "bar"}},
			bson.D{{Key: "foo", Value: "bar"}},
		})
		assert.NoError(t, err)

		// $set to an identical value with upsert=true: must match all three
		// (not insert) and must report ModifiedCount=0
		res, err := c.UpdateMany(nil, bson.D{{Key: "foo", Value: "bar"}}, bson.D{
			{Key: "$set", Value: bson.D{{Key: "foo", Value: "bar"}}},
		}, options.UpdateMany().SetUpsert(true))
		assert.NoError(t, err)
		res.Acknowledged = false
		assert.Equal(t, &mongo.UpdateResult{
			MatchedCount: 3,
		}, res)
	})
}

// TODO: Test upsert with zero object id.
