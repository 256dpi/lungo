package bsonkit

type Set struct {
	List  List        `bson:"list"`
	Index map[Doc]int `bson:"index"`
}

func NewSet(list List) *Set {
	// create set
	set := &Set{
		Index: map[Doc]int{},
	}

	// add documents
	for _, doc := range list {
		set.Add(doc)
	}

	return set
}

func (s *Set) Add(doc Doc) bool {
	// check if already added
	if _, ok := s.Index[doc]; ok {
		return false
	}

	// append document
	s.List = append(s.List, doc)
	s.Index[doc] = len(s.List) - 1

	return true
}

func (s *Set) Replace(d1, d2 Doc) bool {
	// get index
	index, ok := s.Index[d1]
	if !ok {
		return false
	}

	// check existence
	if _, ok := s.Index[d2]; ok {
		return false
	}

	// replace document
	s.List[index] = d2

	// update index
	delete(s.Index, d1)
	s.Index[d2] = index

	return true
}

func (s *Set) Remove(doc Doc) bool {
	// check if document has been added
	i, ok := s.Index[doc]
	if !ok {
		return false
	}

	// remove document
	s.List = append(s.List[:i], s.List[i+1:]...)
	delete(s.Index, doc)

	// update index
	for ; i < len(s.List); i++ {
		s.Index[s.List[i]] = i
	}

	return true
}

func (s *Set) Clone() *Set {
	// prepare clone
	clone := &Set{
		List:  make(List, len(s.List)),
		Index: map[Doc]int{},
	}

	// copy list
	copy(clone.List, s.List)

	// copy index
	for doc, index := range s.Index {
		clone.Index[doc] = index
	}

	return clone
}
