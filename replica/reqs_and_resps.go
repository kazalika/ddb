package main

type AppendEntriesRequest struct {
	Leader      string     `json:"leader"`
	CommitIndex int        `json:"commit_index"`
	LogEntries  []logEntry `json:"log_entry"`
}

type VoteRequest struct {
	Term              int    `json:"term"`
	Candidate         string `json:"candidate"`
	HighestQueryTerm  int    `json:"highest_query_term"`
	HighestQueryIndex int    `json:"highest_query_index"`
}

type VoteResponse struct {
	Term       int  `json:"term"`
	Guaranteed bool `json:"guaranteed"`
}

type Heartbeat struct {
	From              string `json:"from"`
	HighestQueryTerm  int    `json:"term"`
	HighestQueryIndex int    `json:"query_index"`
}

type PatchRequest struct {
	Cond  string `json:"cond"`
	Value string `json:"value"`
}
