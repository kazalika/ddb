package main_test

import (
	"context"
	"fmt"
	"log"
	"net/http"
	replica "replica"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type TestServer struct {
	Replica *replica.Server
	Server  *http.Server
}
type TestCluster struct {
	Nodes []*TestServer
}

var j = 0

func NewTestCluster(size int) *TestCluster {
	peers := []string{}
	for i := 0; i < size; i++ {
		peers = append(peers, "0.0.0.0:520"+strconv.Itoa(j))
		j++
	}
	log.Println(peers)
	cluster := TestCluster{}
	for _, peer := range peers {
		replica := replica.NewServer(peer, peers)
		server := http.Server{
			Addr:    peer,
			Handler: replica.Mux,
		}
		cluster.Nodes = append(cluster.Nodes, &TestServer{
			Replica: replica,
			Server:  &server,
		})
	}
	return &cluster
}

func (c *TestCluster) Start() {
	for _, node := range c.Nodes {
		StartServer(node.Server)
	}
	time.Sleep(3 * time.Duration(len(c.Nodes)) * time.Second)
}

func (c *TestCluster) Stop() {
	for _, node := range c.Nodes {
		StopNode(node)
	}
}

func StartServer(server *http.Server) {
	go func() {
		log.Println("Starting server ", server.Addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("Error starting server: %v\n", err)
		}
	}()

	return
}

func StopServer(server *http.Server) {
	fmt.Println("Stopping server...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		fmt.Printf("Error shutting down server: %v\n", err)
	} else {
		fmt.Println("Server stopped gracefully")
	}
}

func StopNode(server *TestServer) {
	server.Replica.ReplicationProvider.Stop()
	// StopServer(server.Server)
}

func StartNode(server *TestServer) {
	server.Replica.ReplicationProvider.Start()
	// StartServer(server.Server)
}

func (c *TestCluster) GetServerByName(addr string) *TestServer {
	for _, s := range c.Nodes {
		if s.Replica.Address == addr {
			return s
		}
	}
	return nil
}

func (c *TestCluster) GetLeader(exclude []string) string {
	for _, node := range c.Nodes {
		has := false
		addr := node.Replica.Address
		for _, s := range exclude {
			if s == addr {
				has = true
			}
		}
		if !has {
			return node.Replica.ReplicationProvider.GetLeader()
		}
	}
	return ""
}

func TestLeaderElection(t *testing.T) {
	cluster := NewTestCluster(3)
	cluster.Start()

	leader := cluster.GetLeader([]string{})
	assert.NotEqual(t, leader, "")

	StopNode(cluster.GetServerByName(leader))
	time.Sleep(15 * time.Second)

	newLeader := cluster.GetLeader([]string{leader})
	assert.NotEqual(t, newLeader, "")
	assert.NotEqual(t, newLeader, leader)

	cluster.Stop()
}

func TestLogReplication(t *testing.T) {
	cluster := NewTestCluster(3)
	cluster.Start()

	leader := cluster.GetLeader([]string{})

	key := replica.ResourceID("1")
	value := "value"
	res, err := cluster.GetServerByName(leader).Replica.ReplicationProvider.ApplyOperation(replica.Operation{
		T:     replica.POST,
		Key:   &key,
		Value: &value,
	})
	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.EqualValues(t, "OK", *res)

	time.Sleep(5 * time.Second)

	for i := 0; i < 3; i++ {
		assert.Equal(t, 2, cluster.Nodes[i].Replica.ReplicationProvider.GetLogLength())
	}

	cluster.Stop()
}

func TestLogSync(t *testing.T) {
	cluster := NewTestCluster(5)
	cluster.Start()

	oldLeader := cluster.GetLeader([]string{})
	assert.NotEqual(t, oldLeader, "")

	StopNode(cluster.GetServerByName(oldLeader))

	time.Sleep(20 * time.Second)
	newLeader := cluster.GetLeader([]string{oldLeader})
	assert.NotEqual(t, newLeader, "")
	assert.NotEqual(t, newLeader, oldLeader)

	for i := 1; i <= 10; i++ {
		key := replica.ResourceID(strconv.Itoa(i))
		value := "value"
		res, err := cluster.GetServerByName(newLeader).Replica.ReplicationProvider.ApplyOperation(replica.Operation{
			T:     replica.POST,
			Key:   &key,
			Value: &value,
		})
		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.EqualValues(t, "OK", *res)
	}

	time.Sleep(5 * time.Second)

	for i := 0; i < 5; i++ {
		if cluster.Nodes[i].Replica.Address == oldLeader {
			continue
		}
		assert.Equal(t, 11, cluster.Nodes[i].Replica.ReplicationProvider.GetLogLength())
	}

	StartNode(cluster.GetServerByName(oldLeader))
	time.Sleep(10 * time.Second)

	assert.Equal(t, 11, cluster.GetServerByName(oldLeader).Replica.ReplicationProvider.GetLogLength())
	cluster.Stop()
}

func TestOperations(t *testing.T) {
	cluster := NewTestCluster(3)
	cluster.Start()
	leader := cluster.GetLeader([]string{})

	// CREATE
	key := replica.ResourceID("key")
	value := "value"
	res, err := cluster.GetServerByName(leader).Replica.ReplicationProvider.ApplyOperation(replica.Operation{
		T:     replica.POST,
		Key:   &key,
		Value: &value,
	})

	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.EqualValues(t, "OK", *res)

	// GET
	res, err = cluster.GetServerByName(leader).Replica.ReplicationProvider.ApplyOperation(replica.Operation{
		T:   replica.GET,
		Key: &key,
	})

	assert.NoError(t, err)
	assert.NotNil(t, res)
	resArray := strings.Split((*res), " ")
	if len(resArray) == 2 {
		actionNum, err := strconv.Atoi(resArray[0])
		assert.NoError(t, err)
		nodeID := resArray[1]

		res, err = cluster.GetServerByName(nodeID).Replica.ReplicationProvider.GetExecutionResult(actionNum)
		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.EqualValues(t, "value", *res)
	} else {
		assert.EqualValues(t, "value", *res)
	}

	// UPDATE
	newValue := "new value"
	res, err = cluster.GetServerByName(leader).Replica.ReplicationProvider.ApplyOperation(replica.Operation{
		T:     replica.PUT,
		Key:   &key,
		Value: &newValue,
	})

	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.EqualValues(t, "OK", *res)
	readRes, err := cluster.GetServerByName(leader).Replica.DB.Read(replica.ResourceID("key"))
	assert.NoError(t, err)
	assert.NotNil(t, readRes)
	assert.EqualValues(t, "new value", *readRes)

	// CAS
	oldValue := newValue
	newValue = "new value 2"
	res, err = cluster.GetServerByName(leader).Replica.ReplicationProvider.ApplyOperation(replica.Operation{
		T:     replica.PATCH,
		Key:   &key,
		Value: &newValue,
		Cond:  &oldValue,
	})

	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.EqualValues(t, "new value", *res)
	readRes, err = cluster.GetServerByName(leader).Replica.DB.Read(replica.ResourceID("key"))
	assert.NoError(t, err)
	assert.NotNil(t, readRes)
	assert.EqualValues(t, "new value 2", *readRes)

	// FAILED CAS
	newValue3 := "new value 3"
	res, err = cluster.GetServerByName(leader).Replica.ReplicationProvider.ApplyOperation(replica.Operation{
		T:     replica.PATCH,
		Key:   &key,
		Value: &newValue3,
		Cond:  &oldValue,
	})
	assert.NoError(t, err)
	assert.EqualValues(t, "new value 2", *res)

	// DELETE
	res, err = cluster.GetServerByName(leader).Replica.ReplicationProvider.ApplyOperation(replica.Operation{
		T:   replica.DELETE,
		Key: &key,
	})
	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.EqualValues(t, "OK", *res)

	readRes, err = cluster.GetServerByName(leader).Replica.DB.Read(replica.ResourceID("key"))
	assert.Error(t, err)

	cluster.Stop()
}
