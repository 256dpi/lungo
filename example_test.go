package lungo

import (
	"github.com/kr/pretty"
	"go.mongodb.org/mongo-driver/bson"
)

func Example() {
	type post struct {
		Title string `bson:"title"`
	}

	// open database
	client, err := Open(nil, AltClientOptions{
		Store: NewMemoryStore(),
	})
	if err != nil {
		panic(err)
	}

	// get db
	foo := client.Database("foo")

	// get collection
	bar := foo.Collection("bar")

	// insert post
	_, err = bar.InsertOne(nil, &post{
		Title: "Hello World!",
	})
	if err != nil {
		panic(err)
	}

	// query posts
	csr, err := bar.Find(nil, bson.M{})
	if err != nil {
		panic(err)
	}

	// decode posts
	var posts []post
	err = csr.All(nil, &posts)
	if err != nil {
		panic(err)
	}

	// print documents
	pretty.Println(posts)

	// Output:
	// []lungo.post{
	//     {Title:"Hello World!"},
	// }
}
