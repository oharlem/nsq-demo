package main

import (
	"testing"
)

func TestIsSvcMsg(t *testing.T) {

	expected := false
	actual := IsSvcMsg("foo")

	if actual != expected {
		t.Errorf("Expected %v, got %v", expected, actual)
	}

	expected = true
	actual = IsSvcMsg(sessStartMsg)

	if actual != expected {
		t.Errorf("Expected %v, got %v", expected, actual)
	}
}
