package countries

import (
	"fmt"
	"testing"

	application "github.com/ralph-nijpels/geography-application/v2"
)

// TestGetList checks if finding a list of countries will work.
// pre-condition: the database has been filled!
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

	context, err := application.CreateAppContext()
	if err != nil {
		t.Errorf("Internal error: [%v]", err)
	}
	defer context.Destroy()

	countries, err:= NewCountries(context)
	if err != nil {
		t.Errorf("Internal error: [%v]", err)
	}

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

// TestGetByCountryCode checks if finding a specific country will work.
// pre-condition: the database has been filled!
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

	context, err := application.CreateAppContext()
	if err != nil {
		t.Errorf("Internal error: [%v]", err)
	}
	defer context.Destroy()

	countries, err := NewCountries(context)
	if err != nil {
		t.Errorf("Internal error: [%v]", err)
	}

	for _, test := range tests {
		country, err := countries.GetByCountryCode(test.CountryCode)
		if test.ExpectFound && err != nil {
			t.Errorf("Expected [%s] to exist got [%v]", test.CountryCode, err)
			if country.CountryCode != test.CountryCode {
				t.Errorf("Expected [%s], found [%s]", test.CountryCode, country.CountryCode)
			}
		}
		if !(test.ExpectFound) && err == nil {
			t.Errorf("Expected [%s] to not exist", test.CountryCode)
		}
	}
}

// TestImportCountriesCSV checks if the import function works. It assumes that the CSV itself is ready to go
// in the S3 bucket and will change the data in the MongoDB (it will actually import stuff)
//
// TODO: it uses the complete import file so will likely crash due time-out of the test
func TestImportCountriesCSV(t *testing.T) {
	fmt.Println("Testing ImportCountriesCSV..")

	var tests = []struct {
		CountryCode string
		ExpectFound bool
	}{
		{"AD", true},
		{"US", true},
		{"XX", false},
	}

	context, err := application.CreateAppContext()
	if err != nil {
		t.Errorf("Internal error: [%v]", err)
	}
	defer context.Destroy()

	countries, err := NewCountries(context)
	if err != nil {
		t.Errorf("Internal error: [%v]", err)
	}

	// Do the import
	err = countries.ImportCSV()
	if err != nil {
		t.Errorf("Import failed: [%v]", err)
	}

	// Check if the connection is still fine
	mongo, err := context.DBOpen()
	if err != nil {
		t.Errorf("cannot access mongo: %v", err)
	}

	err = mongo.DBClient.Ping(mongo.DBContext, nil)
	if err != nil {
		t.Errorf("ping mongo failed: %v", err)
	}

	// See if the expected ones are there
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

	mongo.DBClose()
}
