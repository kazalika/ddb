package main

import "errors"

var operations = map[OperationType](func(Operation, DBProvider) (*string, error)){
	GET: func(o Operation, db DBProvider) (*string, error) {
		val, err := db.Read(*o.Key)
		if err != nil {
			return nil, errors.New("This key doesn't exist")
		}
		return val, nil
	},
	POST: func(o Operation, db DBProvider) (*string, error) {
		ok := db.Create(*o.Key, o.Value)
		if !ok {
			return nil, errors.New("This key already exists")
		}
		return &OK, nil
	},
	PUT: func(o Operation, db DBProvider) (*string, error) {
		ok := db.Update(*o.Key, o.Value)
		if !ok {
			return nil, errors.New("This key doesn't exist")
		}
		return &OK, nil
	},
	PATCH: func(o Operation, db DBProvider) (*string, error) {
		val, err := db.Read(*o.Key)
		if err != nil {
			return nil, errors.New("This key doesn't exist")
		}
		if val != o.Cond {
			return val, nil
		}
		ok := db.Update(*o.Key, o.Value)
		if !ok {
			return nil, errors.New("Somebody delete key between read and write operations")
		}
		return val, nil
	},
	DELETE: func(o Operation, db DBProvider) (*string, error) {
		ok := db.Delete(*o.Key)
		if !ok {
			return nil, errors.New("This key doesn't exist")
		}
		return &OK, nil
	},
	INIT: func(o Operation, db DBProvider) (*string, error) {
		return nil, nil
	},
}
