package main

import "strings"

// Record stores verified line record information.
type Record struct {
	Country    string `json:"country,omitempty"`
	IP         string `json:"ip,omitempty"`
	RecordType string `json:"type"`
	Timestamp  string `json:"timestamp"`
	UserID     string `json:"user_id"`
	VideoID    string `json:"video_id,omitempty"`
}

// add parses a feed line and converts into a Record.
func (r *Record) add(l string) error {

	if l == "" {
		return errInvalidInput
	}

	vals := strings.Split(l, " ")
	rType := vals[1]

	// Common parameters
	r.Timestamp = vals[0]
	r.RecordType = vals[1]
	r.UserID = vals[2]

	// Type-specific parameters
	if rType == "REGISTER" {
		r.Country = vals[3]
		r.IP = vals[4]
	} else {
		r.VideoID = vals[3]
	}

	return nil
}
