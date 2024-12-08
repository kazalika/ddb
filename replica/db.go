package main

import "errors"

type dbProviderImpl struct {
	kv map[ResourceID]string
}

func NewDBProvider() DBProvider {
	return &dbProviderImpl{
		kv: make(map[ResourceID]string),
	}
}

func (d *dbProviderImpl) Create(key ResourceID, value *string) bool {
	if _, ok := d.kv[key]; ok {
		return false
	}
	d.kv[key] = *value
	return true
}

func (d *dbProviderImpl) Read(key ResourceID) (*string, error) {
	if v, ok := d.kv[key]; ok {
		return &v, nil
	}
	return nil, errors.New("What u gonna read mf?")
}

func (d *dbProviderImpl) Update(key ResourceID, value *string) bool {
	if _, ok := d.kv[key]; !ok {
		return false
	}
	d.kv[key] = *value
	return true
}

func (d *dbProviderImpl) Delete(key ResourceID) bool {
	if _, ok := d.kv[key]; !ok {
		return false
	}
	delete(d.kv, key)
	return true
}
