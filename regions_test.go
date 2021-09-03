package countries

import (
	"testing"

	application "github.com/ralph-nijpels/geography-application"
)

// TestImportRegionsCSV checks if the import of the regions file will work. It assumes
// that the CSV is readyin the S3 bucket, all necessary countries are already there  and will
// change the mongo DB (it actually imports stuff)
func TestImportRegionsCSV(t *testing.T) {

	context, err := application.CreateAppContext()
	if err != nil {
		t.Errorf("Internal error: [%v]", err)
	}
	defer context.Destroy()

	// Set-up the datastructure
	countries := NewCountries(context)
	regions := countries.NewRegions()

	// Do the import
	err = regions.ImportCSV()
	if err != nil {
		t.Errorf("Import failed: %v", err)
	}

	// Check if the connection is still fine
	err = context.DBClient.Ping(context.DBContext, nil)
	if err != nil {
		t.Errorf("Connection fucked up")
	}

}
