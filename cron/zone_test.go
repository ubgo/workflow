package cron

import (
	"errors"
	"testing"
)

func TestLoadZone(t *testing.T) {
	loc, err := LoadZone(" Asia/Kolkata ")
	if err != nil || loc.String() != "Asia/Kolkata" {
		t.Fatalf("LoadZone = %v, %v", loc, err)
	}
	if loc, err := LoadZone("UTC"); err != nil || loc.String() != "UTC" {
		t.Fatalf("UTC: %v, %v", loc, err)
	}
	for _, name := range []string{"", "  ", "Local", "Mars/Olympus", "Europe/Berlinn"} {
		if _, err := LoadZone(name); !errors.Is(err, ErrZone) {
			t.Errorf("LoadZone(%q) err = %v, want ErrZone", name, err)
		}
	}
}
