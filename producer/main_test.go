package main

import (
	"testing"
)

func TestRecordValidator_REGISTER(t *testing.T) {

	testRecord := "2014-08-12T02:14:33-04:00 REGISTER 49 SG 174.2.25.21"

	rType, err := recordValidator(testRecord)
	if err != nil {
		t.Fatalf("RecordValidator should pass validation, but returned %v", err)
	}

	if rType != "REGISTER" {
		t.Errorf("Expected %s type, got %s", "REGISTER", rType)
	}
}

func TestRecordValidator_UPLOAD(t *testing.T) {

	testRecord := "2014-09-13T04:17:47-04:00 UPLOAD 9261 33157"

	rType, err := recordValidator(testRecord)
	if err != nil {
		t.Fatalf("RecordValidator should pass validation, but returned %v", err)
	}

	if rType != "UPLOAD" {
		t.Errorf("Expected %s type, got %s", "UPLOAD", rType)
	}
}

func TestRecordValidator_WATCH(t *testing.T) {

	testRecord := "2014-09-14T00:07:53-04:00 WATCH 7195 676807"

	rType, err := recordValidator(testRecord)
	if err != nil {
		t.Fatalf("RecordValidator should pass validation, but returned %v", err)
	}

	if rType != "WATCH" {
		t.Errorf("Expected %s type, got %s", "WATCH", rType)
	}
}

func TestRecordValidator_LIKE(t *testing.T) {

	testRecord := "2014-09-14T21:17:01-04:00 LIKE 8648 765662"

	rType, err := recordValidator(testRecord)
	if err != nil {
		t.Fatalf("RecordValidator should pass validation, but returned %v", err)
	}

	if rType != "LIKE" {
		t.Errorf("Expected %s type, got %s", "LIKE", rType)
	}
}

func TestRecordValidator_INVALID_RECORD(t *testing.T) {

	var testRecord string
	var err error

	testRecord = ""
	_, err = recordValidator(testRecord)
	if err == nil {
		t.Error("RecordValidator should return an error on invalid records (empty)")
	}

	testRecord = "2014-09-14T21:17:01-04:00 REGISTER 8648"
	_, err = recordValidator(testRecord)
	if err == nil {
		t.Error("RecordValidator should return an error on invalid records (incorrect parameter qty)")
	}

	testRecord = "2014-09-14T21:17:01-04:00 UNKNOWNTYPE 8648 765662"
	_, err = recordValidator(testRecord)
	if err == nil {
		t.Error("RecordValidator should return an error when type is unknown")
	}
}
