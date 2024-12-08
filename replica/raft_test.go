package main_test

import (
	"context"
	"fmt"
	"log"
	"net/http"
	replica "replica"
	"strconv"
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

	time.Sleep(10 * time.Second)
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
	time.Sleep(15 * time.Second)

	leader := cluster.Nodes[0].Replica.ReplicationProvider.GetLeader()

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
	time.Sleep(20 * time.Second)

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
