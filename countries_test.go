package countries

import (
	"fmt"
	"testing"

	application "github.com/ralph-nijpels/geography-application"
)

func TestGetList(t *testing.T) {
	fmt.Println("Testing GetList..")

	var tests = []struct {
		fromCountryCode  string
		untilCountryCode string
		ExpectFound      bool
	}{
		{"", "", true},
		{"", "AZ", true},
		{"US", "", true},
		{"NL", "NL", true}}

	context, err := application.GetContext()
	if err != nil {
		t.Errorf("Internal error: [%v]", err)
	}

	countries := NewCountries(context)

	for _, test := range tests {
		_, err := countries.GetList(test.fromCountryCode, test.untilCountryCode)
		if test.ExpectFound && err != nil {
			t.Errorf("Expected [%s..%s] to have results", test.fromCountryCode, test.untilCountryCode)
		}
		if !(test.ExpectFound) && err == nil {
			t.Errorf("Expected [%s..%s] to have no results", test.fromCountryCode, test.untilCountryCode)
		}
	}
}

func TestGetByCountryCode(t *testing.T) {
	fmt.Println("Testing GetByCountryCode..")

	var tests = []struct {
		CountryCode string
		ExpectFound bool
	}{
		{"AD", true},
		{"US", true},
		{"XX", false},
	}

	context, err := application.GetContext()
	if err != nil {
		t.Errorf("Internal error: [%v]", err)
	}

	countries := NewCountries(context)

	for _, test := range tests {
		country, err := countries.GetByCountryCode(test.CountryCode)
		if test.ExpectFound && err != nil {
			t.Errorf("Expected [%s] to exist", test.CountryCode)
			if country.CountryCode != test.CountryCode {
				t.Errorf("Expected [%s], found [%s]", test.CountryCode, country.CountryCode)
			}
		}
		if !(test.ExpectFound) && err == nil {
			t.Errorf("Expected [%s] to not exist", test.CountryCode)
		}
	}
}
