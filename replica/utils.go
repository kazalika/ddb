package main

import (
	"io"
	"net/http"
	"time"
)

type ResourceID string

type Operation struct {
	T                OperationType `json:"operation_type"`
	Key              *ResourceID   `json:"key"`
	Value            *string       `json:"value"`
	Cond             *string       `json:"cond"`
	WhoShouldExecute string        `json:"who_should_execute"`
}

type OperationType int

const (
	GET OperationType = iota
	POST
	PUT
	PATCH
	DELETE
	INIT
)

var OK = "OK"

func readRequestBody(r *http.Request) (string, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func updateTimer(timer *time.Timer, timeout time.Duration, callback func()) *time.Timer {
	if timer != nil {
		timer.Stop()
	}
	return time.AfterFunc(timeout, callback)
}
