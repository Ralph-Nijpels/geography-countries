package countries

import (
	"testing"

	application "github.com/ralph-nijpels/geography-application/v2"
)

// TestImportRegionsCSV checks if the import of the regions file will work. It assumes
// that the CSV is readyin the S3 bucket, all necessary countries are already there  and will
// change the mongo DB (it actually imports stuff)
//
// TODO: it uses the complete import file so will likely crash due time-out of the test
func TestImportRegionsCSV(t *testing.T) {

	context, err := application.CreateAppContext()
	if err != nil {
		t.Errorf("Internal error: [%v]", err)
	}
	defer context.Destroy()

	// Set-up the datastructure
	countries, err := NewCountries(context)
	if err != nil {
		t.Errorf("Internal error: [%v]", err)
	}

	regions := countries.NewRegions()

	// Do the import
	err = regions.ImportCSV()
	if err != nil {
		t.Errorf("Import failed: %v", err)
	}
}
