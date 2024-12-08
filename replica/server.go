package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
)

type RaftParticipant interface {
	ApplyOperation(Operation) (*string, error)
	Stop()
	Start()
	GetExecutionResult(int) (*string, error)
	GetLeader() string
	GetLogLength() int
}

type DBProvider interface {
	Create(key ResourceID, value *string) bool
	Read(key ResourceID) (*string, error)
	Update(key ResourceID, value *string) bool
	Delete(key ResourceID) bool
}

type Server struct {
	Mux                 *chi.Mux
	ReplicationProvider RaftParticipant
	DB                  DBProvider
	Address             string
}

func NewServer(addrInput string, peersInput []string) *Server {
	addr := "http://" + addrInput
	peers := []string{}
	for i := 0; i < len(peersInput); i++ {
		peers = append(peers, "http://"+peersInput[i])
	}
	s := &Server{
		Mux:     chi.NewRouter(),
		DB:      NewDBProvider(),
		Address: addr,
	}
	s.ReplicationProvider = NewRaftParticipant(addr, peers, s.DB, s.Mux)

	s.Mux.Get("/kv/{id}", s.Get)
	s.Mux.Get("/kv/redirected/{exec-id}", s.RedirectedGet)
	s.Mux.Post("/kv/{id}", s.Post)
	s.Mux.Put("/kv/{id}", s.Put)
	s.Mux.Patch("/kv/{id}", s.Patch)
	s.Mux.Delete("/kv/{id}", s.Delete)

	return s
}

func (s *Server) Get(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("id")
	if key == "" {
		http.Error(w, "Key is required", http.StatusBadRequest)
		return
	}

	res, err := s.ReplicationProvider.ApplyOperation(Operation{
		T:   GET,
		Key: (*ResourceID)(&key),
	})

	if err != nil {
		http.Error(w, fmt.Sprintf("Something went wrong: %v", err.Error()), http.StatusInternalServerError)
		return
	}
	w.Write([]byte(fmt.Sprint(*res)))
}

func (s *Server) RedirectedGet(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("exec-id")
	if idStr == "" {
		http.Error(w, "Query ID is required", http.StatusBadRequest)
		return
	}
	id, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, "ID should be int", http.StatusBadRequest)
		return
	}
	res, err := s.ReplicationProvider.GetExecutionResult(id)
	if err != nil {
		http.Error(w, fmt.Sprintf("Something went wrong: %v", err.Error()), http.StatusInternalServerError)
		return
	}
	w.Write([]byte(*res))
}

func (s *Server) Post(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("id")
	if key == "" {
		http.Error(w, "Key is required", http.StatusBadRequest)
		return
	}
	data, err := readRequestBody(r)
	if err != nil {
		http.Error(w, "Can't read body of request", http.StatusBadRequest)
		return
	}

	res, err := s.ReplicationProvider.ApplyOperation(Operation{
		T:     POST,
		Key:   (*ResourceID)(&key),
		Value: &data,
	})

	if err != nil {
		http.Error(w, fmt.Sprintf("Something went wrong: %v", err.Error()), http.StatusInternalServerError)
		return
	}
	w.Write([]byte(fmt.Sprint(*res)))
}

func (s *Server) Put(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("id")
	if key == "" {
		http.Error(w, "Key is required", http.StatusBadRequest)
		return
	}
	data, err := readRequestBody(r)
	if err != nil {
		http.Error(w, "Can't read body of request", http.StatusBadRequest)
		return
	}

	res, err := s.ReplicationProvider.ApplyOperation(Operation{
		T:     PUT,
		Key:   (*ResourceID)(&key),
		Value: &data,
	})

	if err != nil {
		http.Error(w, fmt.Sprintf("Something went wrong: %v", err.Error()), http.StatusInternalServerError)
		return
	}
	w.Write([]byte(fmt.Sprint(*res)))
}

func (s *Server) Patch(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("id")
	if key == "" {
		http.Error(w, "Key is required", http.StatusBadRequest)
		return
	}
	var patchReq PatchRequest

	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&patchReq)
	if err != nil {
		http.Error(w, "Ошибка декодирования JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	res, err := s.ReplicationProvider.ApplyOperation(Operation{
		T:     POST,
		Key:   (*ResourceID)(&key),
		Value: &patchReq.Value,
		Cond:  &patchReq.Cond,
	})

	if err != nil {
		http.Error(w, fmt.Sprintf("Something went wrong: %v", err.Error()), http.StatusInternalServerError)
		return
	}
	w.Write([]byte(fmt.Sprint(res)))
}

func (s *Server) Delete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("id")
	if key == "" {
		http.Error(w, "Key is required", http.StatusBadRequest)
		return
	}
	data, err := readRequestBody(r)
	if err != nil {
		http.Error(w, "Can't read body of request", http.StatusBadRequest)
		return
	}

	res, err := s.ReplicationProvider.ApplyOperation(Operation{
		T:     PUT,
		Key:   (*ResourceID)(&key),
		Value: &data,
	})

	if err != nil {
		http.Error(w, fmt.Sprintf("Something went wrong: %v", err.Error()), http.StatusInternalServerError)
		return
	}
	w.Write([]byte(fmt.Sprint(res)))
}

func main() {
	var addr string
	flag.StringVar(&addr, "addr", "localhost:5252", "Replica address")
	flag.Parse()

	log.Println("Addr =", addr)

	s := NewServer(addr, []string{"0.0.0.0:52525", "0.0.0.0:52535", "0.0.0.0:52545"})
	log.Print("Start HTTP Server...")
	log.Fatal(http.ListenAndServe(addr, s.Mux))
}
