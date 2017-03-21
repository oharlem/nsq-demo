package main

import (
	"reflect"
	"testing"
)

// regular record format
func TestAddLine_UPLOAD(t *testing.T) {

	testRecord := "2014-08-12T02:14:33-04:00 UPLOAD 9261 33157"

	expected := Record{
		Timestamp:  "2014-08-12T02:14:33-04:00",
		RecordType: "UPLOAD",
		UserID:     "9261",
		VideoID:    "33157",
	}

	actual := Record{}
	err := actual.add(testRecord)

	if err != nil {
		t.Fatalf("addLine should return no errors, but returned %v", err)
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Error("Failed UPLOAD line parser validation!")
		t.Log("Expected:")
		t.Logf("%+v\n", expected)

		t.Log("Actual:")
		t.Logf("%+v\n", actual)
	}
}

// registration-specific record format
func TestAddLine_REGISTER(t *testing.T) {

	testRecord := "2014-08-12T02:14:33-04:00 REGISTER 49 SG 174.2.25.21"

	expected := Record{
		Timestamp:  "2014-08-12T02:14:33-04:00",
		RecordType: "REGISTER",
		UserID:     "49",
		Country:    "SG",
		IP:         "174.2.25.21",
	}

	actual := Record{}
	err := actual.add(testRecord)

	if err != nil {
		t.Fatalf("addLine should return no errors, but returned %v", err)
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Error("Failed REGISTER line parser validation!")
		t.Log("Expected:")
		t.Logf("%+v\n", expected)

		t.Log("Actual:")
		t.Logf("%+v\n", actual)
	}
}
