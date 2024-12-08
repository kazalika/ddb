package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
)

type execution struct {
	op            Operation
	resultChannel chan executionResult
}

type executionResult struct {
	res *string
	err error
}

type RaftParticipantStatus int

const (
	LEADER RaftParticipantStatus = iota
	FOLLOWER
	CANDIDATE
)

type logEntry struct {
	Term int       `json:"term"`
	Id   int       `json:"id"`
	Op   Operation `json:"operation"`
}

type raftParticipantImpl struct {
	mux *chi.Mux

	status            RaftParticipantStatus
	leader            string
	log               []logEntry
	executions        []execution
	peers             []string
	peersQueryIndexes map[string]int
	me                string
	lastVote          string

	mu              sync.Mutex
	queryIndex      int
	commitIndex     int
	appliedIndex    int
	roundRobinIndex int
	term            int

	electionTimeout time.Duration
	electionTimer   *time.Timer

	heartbeatTimeout time.Duration
	heartbeatTimer   *time.Timer

	db        DBProvider
	isRunning bool
}

func NewRaftParticipant(me string, peers []string, db DBProvider, mux *chi.Mux) RaftParticipant {
	raft := &raftParticipantImpl{
		mux:               mux,
		status:            FOLLOWER,
		peersQueryIndexes: make(map[string]int),
		me:                me,
		db:                db,
		peers:             peers,
		queryIndex:        1,
		log: []logEntry{
			{
				Term: 0,
				Id:   0,
				Op: Operation{
					T: INIT,
				},
			},
		},
		executions: []execution{
			{
				op: Operation{
					T: INIT,
				},
				resultChannel: make(chan executionResult, 1),
			},
		},
		heartbeatTimeout: time.Second * 1,
		electionTimeout:  time.Second * time.Duration(2+rand.IntN(6)),
		isRunning:        true,
	}

	for _, peer := range peers {
		raft.peersQueryIndexes[peer] = 0
	}

	raft.mux.Post("/vote", raft.RequestVote)
	raft.mux.Post("/entries", raft.AppendEntries)
	raft.mux.Post("/heartbeat", raft.Heartbeat)

	raft.electionTimer = updateTimer(raft.electionTimer, raft.electionTimeout, raft.StartLeaderElection)

	return raft
}

func (raft *raftParticipantImpl) sendAppendOperations(peer string, acks chan bool) {
	raft.mu.Lock()
	if !raft.isRunning {
		raft.mu.Unlock()
		return
	}
	req, err := json.Marshal(AppendEntriesRequest{
		Leader:      raft.me,
		CommitIndex: raft.commitIndex,
		LogEntries:  raft.log[raft.peersQueryIndexes[peer]:],
	})
	raft.mu.Unlock()
	if err != nil {
		return
	}
	resp, err := http.Post(peer+"/entries", "application/json", bytes.NewReader(req))

	if err != nil || resp.StatusCode != http.StatusOK {
		acks <- false
		return
	} else {
		raft.mu.Lock()
		defer raft.mu.Unlock()
		body, err := io.ReadAll(resp.Body)

		if err == nil && len(body) != 0 {
			var number int
			_, err = fmt.Sscanf(string(body), "%d", number)
			if err != nil {
				log.Println("Wrong response from `/entries`")
				return
			}
			raft.peersQueryIndexes[peer] = number
			acks <- false
		} else {
			raft.peersQueryIndexes[peer] = raft.queryIndex
			acks <- true
		}
	}
}

func (raft *raftParticipantImpl) ApplyOperation(op Operation) (*string, error) {
	raft.mu.Lock()
	if !raft.isRunning {
		raft.mu.Unlock()
		return nil, nil
	}
	log.Println("Apply operation!")

	if raft.status != LEADER {
		raft.mu.Unlock()
		return nil, errors.New("I'm not leader :C")
	}

	entry := logEntry{
		Term: raft.term,
		Id:   raft.queryIndex,
		Op:   op,
	}
	if op.T == GET {
		entry.Op.WhoShouldExecute = raft.peers[raft.roundRobinIndex]
		raft.roundRobinIndex = (raft.roundRobinIndex + 1) % len(raft.peers)
	}
	raft.log = append(raft.log, entry)
	log.Println("Append new operation to log ", len(raft.log))
	exec := execution{
		op:            op,
		resultChannel: make(chan executionResult, 1),
	}
	raft.executions = append(raft.executions, exec)
	raft.queryIndex++
	raft.mu.Unlock()

	acks := make(chan bool, len(raft.peers))
	for _, peer := range raft.peers {
		if peer != raft.me {
			go raft.sendAppendOperations(peer, acks)
		}
	}

	counter := 1 // me
	for i := 0; i < len(raft.peers)-1; i++ {
		if <-acks {
			counter++
		}
		if counter > len(raft.peers)/2 {
			break
		}
	}

	log.Println("Got ", counter, " acks")

	raft.mu.Lock()

	if counter > len(raft.peers)/2 {
		raft.commitIndex = raft.queryIndex
		go raft.applyCommittedEntries()
	}

	raft.mu.Unlock()

	res := <-exec.resultChannel
	return res.res, res.err
}

func (raft *raftParticipantImpl) GetExecutionResult(id int) (*string, error) {
	if len(raft.executions) < id {
		return nil, errors.New("Inconsistent replica")
	}
	res := <-raft.executions[id].resultChannel
	return res.res, res.err
}

func (raft *raftParticipantImpl) applyCommittedEntries() {
	raft.mu.Lock()
	if !raft.isRunning {
		return
	}
	defer raft.mu.Unlock()

	log.Println("Apply committed entries")

	for i := raft.appliedIndex; i < raft.commitIndex; i++ {
		log.Println("Applying: ", i)
		op := raft.log[i].Op
		if op.T == GET && op.WhoShouldExecute != "" && op.WhoShouldExecute != raft.me {
			log.Println("This GET not for me :C")
			str := strconv.Itoa(i) + " " + op.WhoShouldExecute
			raft.executions[i].resultChannel <- executionResult{
				res: &str,
				err: nil,
			}
			continue
		}
		res, err := operations[op.T](op, raft.db)
		raft.executions[i].resultChannel <- executionResult{
			res: res,
			err: err,
		}
	}
	raft.appliedIndex = raft.commitIndex
}

func sendVoteResponse(resp VoteResponse, w http.ResponseWriter) error {
	b, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	w.Write(b)
	return nil
}

func (raft *raftParticipantImpl) RequestVote(w http.ResponseWriter, req *http.Request) {
	raft.mu.Lock()
	if !raft.isRunning {
		raft.mu.Unlock()
		return
	}
	raft.mu.Unlock()

	log.Println("Request vote!")

	var voteReq VoteRequest

	decoder := json.NewDecoder(req.Body)
	err := decoder.Decode(&voteReq)
	if err != nil {
		http.Error(w, "Ошибка декодирования JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer req.Body.Close()

	if voteReq.Term < raft.term {
		log.Println("Bad candidate with low term :C")
		if sendVoteResponse(VoteResponse{
			Term:       raft.term,
			Guaranteed: false,
		}, w) != nil {
			http.Error(w, "JSON error", http.StatusInternalServerError)
			return
		}
		return
	}

	if (raft.lastVote == "" || raft.lastVote == voteReq.Candidate) && voteReq.HighestQueryIndex >= raft.queryIndex && voteReq.HighestQueryIndex >= raft.log[len(raft.log)-1].Term {
		raft.term = voteReq.Term
		raft.lastVote = voteReq.Candidate
		raft.electionTimer = updateTimer(raft.electionTimer, raft.electionTimeout, raft.StartLeaderElection)
		log.Println("Good candidate!")
		if sendVoteResponse(VoteResponse{
			Term:       raft.term,
			Guaranteed: true,
		}, w) != nil {
			http.Error(w, "JSON error", http.StatusInternalServerError)
			return
		}
		return
	}

	log.Println("Bad candidate :C")
	if sendVoteResponse(VoteResponse{
		Term:       raft.term,
		Guaranteed: false,
	}, w) != nil {
		http.Error(w, "JSON error", http.StatusInternalServerError)
		return
	}
}

func (raft *raftParticipantImpl) AppendEntries(w http.ResponseWriter, req *http.Request) {
	raft.mu.Lock()
	if !raft.isRunning {
		raft.mu.Unlock()
		return
	}
	raft.mu.Unlock()

	log.Println("Append entries!")

	var body AppendEntriesRequest

	decoder := json.NewDecoder(req.Body)
	err := decoder.Decode(&body)
	if err != nil {
		http.Error(w, "Ошибка декодирования JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer req.Body.Close()

	raft.mu.Lock()
	defer raft.mu.Unlock()
	if len(body.LogEntries) == 0 || body.LogEntries[len(body.LogEntries)-1].Term < raft.term {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if body.LogEntries[0].Id > raft.queryIndex {
		fmt.Fprintln(w, raft.queryIndex)
		return
	}

	raft.leader = body.Leader
	raft.lastVote = ""
	raft.status = FOLLOWER
	raft.term = body.LogEntries[len(body.LogEntries)-1].Term
	raft.electionTimer = updateTimer(raft.electionTimer, raft.electionTimeout, raft.StartLeaderElection)

	i := 0
	for i < len(body.LogEntries) && body.LogEntries[i].Id < len(raft.log) && body.LogEntries[i].Term == raft.log[body.LogEntries[i].Id].Term {
		i++
	}
	if i < len(body.LogEntries) {
		raft.log = append(raft.log[:body.LogEntries[i].Id], body.LogEntries[i:]...)
		for ; i < len(body.LogEntries); i++ {
			exec := execution{
				op:            body.LogEntries[i].Op,
				resultChannel: make(chan executionResult, 1),
			}
			if body.LogEntries[i].Id < raft.queryIndex {
				raft.executions[body.LogEntries[i].Id] = exec
			} else {
				raft.executions = append(raft.executions, exec)
			}
		}
	}

	raft.queryIndex = len(raft.log)
	raft.commitIndex = body.CommitIndex
	go raft.applyCommittedEntries()
	w.WriteHeader(http.StatusOK)
}

func (raft *raftParticipantImpl) StartLeaderElection() {
	raft.mu.Lock()
	if !raft.isRunning {
		raft.mu.Unlock()
		raft.ResetElectionTimer()
		return
	}
	raft.mu.Unlock()

	log.Println(raft.me, "Start leader election!")

	raft.mu.Lock()
	raft.term++
	raft.status = CANDIDATE
	raft.lastVote = raft.me
	votes := 1

	log.Println("New term =", raft.term)
	log.Println(raft.peers)
	raft.mu.Unlock()

	for _, peer := range raft.peers {
		log.Println(raft.me, peer)
		if peer == raft.me {
			continue
		}
		raft.mu.Lock()
		req := VoteRequest{
			Term:              raft.term,
			Candidate:         raft.me,
			HighestQueryTerm:  raft.log[len(raft.log)-1].Term,
			HighestQueryIndex: raft.queryIndex,
		}
		raft.mu.Unlock()

		b, err := json.Marshal(req)
		if err != nil {
			continue
		}
		resp, err := http.Post(peer+"/vote", "application/json", bytes.NewReader(b))
		if err != nil {
			log.Println("Something is not ok: ", err)
			continue
		}
		d := json.NewDecoder(resp.Body)
		var voteResp VoteResponse
		err = d.Decode(&voteResp)
		if err != nil {
			log.Println("Something is not ok: ", err)
			continue
		}

		raft.mu.Lock()
		if voteResp.Guaranteed {
			votes++
		} else {
			log.Println("Vote denied")
			raft.term = max(raft.term, voteResp.Term+1)
		}

		log.Println("Votes: ", votes)
		if votes > len(raft.peers)/2 && raft.status == CANDIDATE {
			raft.BecomeLeader()
		}
		raft.mu.Unlock()
	}
	log.Println("Got votes: ", votes)
	if raft.status != LEADER {
		raft.status = FOLLOWER
		raft.lastVote = ""
		raft.electionTimer = updateTimer(raft.electionTimer, raft.electionTimeout, raft.StartLeaderElection)
	}
}

func (raft *raftParticipantImpl) BecomeLeader() {
	log.Println("I am a leader now!")

	raft.status = LEADER
	raft.leader = raft.me

	go raft.sendHeartbeats()
}

func (raft *raftParticipantImpl) ResetHeartbeatTimer() {
	raft.heartbeatTimer = updateTimer(raft.heartbeatTimer, raft.heartbeatTimeout, raft.sendHeartbeats)
}

func (raft *raftParticipantImpl) ResetElectionTimer() {
	raft.electionTimer = updateTimer(raft.electionTimer, raft.electionTimeout, raft.StartLeaderElection)
}

func (raft *raftParticipantImpl) sendHeartbeats() {
	if raft.status != LEADER {
		return
	}

	raft.mu.Lock()
	if raft.isRunning {
		req := Heartbeat{
			From:              raft.me,
			HighestQueryTerm:  raft.log[len(raft.log)-1].Term,
			HighestQueryIndex: raft.queryIndex,
		}
		raft.mu.Unlock()
		b, err := json.Marshal(req)
		if err != nil {
			return
		}

		log.Println("Gonna send hearbeats!")
		for _, peer := range raft.peers {
			if peer != raft.me {
				resp, err := http.Post(peer+"/heartbeat", "", bytes.NewReader(b))
				if err != nil {
					log.Printf("Seems like %s is dead", peer)
					continue
				}
				body, err := io.ReadAll(resp.Body)
				if err != nil {
					fmt.Printf("Peer %s is up date", peer)
					continue
				}

				var number int
				_, err = fmt.Sscanf(string(body), "%d", &number)
				if err != nil {
					fmt.Println("Error parsing number:", err)
					continue
				}
				raft.mu.Lock()

				if raft.peersQueryIndexes[peer] > number {
					raft.peersQueryIndexes[peer] = number
					raft.mu.Unlock()
					go raft.sendAppendOperations(peer, make(chan bool, 1))
					raft.mu.Lock()
				}
				raft.peersQueryIndexes[peer] = number
				raft.mu.Unlock()
			}
		}
	} else {
		raft.mu.Unlock()
	}
	raft.ResetHeartbeatTimer()
}

func (raft *raftParticipantImpl) Heartbeat(w http.ResponseWriter, req *http.Request) {
	raft.mu.Lock()
	if !raft.isRunning {
		raft.mu.Unlock()
		raft.ResetElectionTimer()
		return
	}
	raft.mu.Unlock()

	log.Println(raft.me, "Heartbeat recieved")

	var body Heartbeat
	decoder := json.NewDecoder(req.Body)
	err := decoder.Decode(&body)
	if err != nil {
		http.Error(w, "Ошибка декодирования JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer req.Body.Close()

	raft.mu.Lock()
	defer raft.mu.Unlock()

	if body.From == raft.lastVote {
		raft.leader = raft.lastVote
	}
	if body.From != raft.leader {
		log.Println(raft.me, "Heartbeat recieved not from a leader hmm... leader =", raft.leader, "sender =", body.From)

		if body.HighestQueryIndex >= raft.queryIndex && body.HighestQueryIndex >= raft.log[len(raft.log)-1].Term {
			raft.leader = body.From
			log.Println("New leader", raft.leader)
		} else {
			return
		}
	}

	fmt.Fprintf(w, "%d", raft.commitIndex)

	raft.ResetElectionTimer()
}

func (raft *raftParticipantImpl) GetLeader() string {
	return raft.leader
}

func (raft *raftParticipantImpl) GetLogLength() int {
	raft.mu.Lock()
	defer raft.mu.Unlock()
	return len(raft.log)
}

func (raft *raftParticipantImpl) Start() {
	raft.mu.Lock()
	defer raft.mu.Unlock()
	raft.isRunning = true
}

func (raft *raftParticipantImpl) Stop() {
	raft.mu.Lock()
	defer raft.mu.Unlock()
	raft.isRunning = false
}
