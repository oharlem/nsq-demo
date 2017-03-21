package main

import (
	"reflect"
	"testing"
)

func TestConvertStrIDs(t *testing.T) {

	testString := "101,66,1,1979"
	expected := []int{101, 66, 1, 1979}

	actual, err := convertStrIDs(testString)
	if err != nil {
		t.Fatalf("ConvertStrIDs should pass validation, but returned %v", err)
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v type, got %v", expected, actual)
	}

}
