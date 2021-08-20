package countries

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"

	"github.com/minio/minio-go"

	"go.mongodb.org/mongo-driver/bson"

	"../application"
	"../datatypes"
)

// regions implements the regions data

// Regions represents the connection to the database
type Regions struct {
	context *application.Context
	parent  *Countries
}

// Region is the external representation for an ISO-Region including both a bson (for mongo)
// and a json (for REST/GRAPHQL) representation
type Region struct {
	RegionCode string `bson:"iso-region-code" json:"iso-region-code"`
	RegionName string `bson:"region-name" json:"region-name"`
	Wikipedia  string `bson:"wikipedia" json:"wikipedia,omitempty"`
}

// NewRegions establishes the connection to the database
func (countries *Countries) NewRegions() *Regions {
	regions := Regions{
		context: countries.context,
		parent:  countries,
	}

	return &regions
}

// RetrieveFromURL downloads the file into the etc directory
func (regions *Regions) RetrieveFromURL() error {
	// Get the data
	resp, err := http.Get(regions.context.RegionsURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Copy the file to S3
	s3Client := regions.context.S3Client
	_, err = s3Client.PutObject("csv", "regions", resp.Body, -1,
		minio.PutObjectOptions{ContentType: "text/csv"})

	return err
}

func (regions *Regions) importCSVLine(line []string, lineNumber int) error {
	// Skipping empty lines
	if len(line) == 0 {
		return nil
	}

	// Check Region Code
	regionCode, err := datatypes.ISORegionCode(line[2], false, false)
	if err != nil {
		return fmt.Errorf("Regions[%d].RegionCode(%s): %v", lineNumber, line[1], err)
	}

	// Check CountryID
	country, err := regions.parent.GetByCountryCode(line[5])
	if err != nil {
		return fmt.Errorf("Regions[%d].CountryCode(%s): %v", lineNumber, line[5], err)
	}

	// Build internal representation
	region := Region{
		RegionCode: regionCode,
		RegionName: line[3],
		Wikipedia:  line[6]}

	// replace or add region...
	found := false
	for i := range country.Regions {
		if country.Regions[i].RegionCode == region.RegionCode {
			country.Regions[i] = &region
			found = true
			break
		}
	}
	if !found {
		country.Regions = append(country.Regions, &region)
	}

	// Dump in mongo
	_, err = regions.parent.collection.UpdateOne(
		regions.context.DBContext,
		bson.D{{Key: "iso-country-code", Value: country.CountryCode}},
		bson.M{"$set": country})

	if err != nil {
		return fmt.Errorf("Regions[%d]: %v", lineNumber, err)
	}

	return nil
}

// ImportCSV initializes the database from a CSV-file
func (regions *Regions) ImportCSV() error {
	// Open the regions.csv file
	s3Client := regions.context.S3Client
	csvFile, err := s3Client.GetObject(
		"csv", "regions",
		minio.GetObjectOptions{})
	if err != nil {
		return err
	}
	defer csvFile.Close()

	// Open the logfile
	_, err = regions.context.LogFile("regions")
	if err != nil {
		return err
	}
	defer regions.context.LogClose()

	regions.context.LogPrintln("Start Import")

	// Skip the headerline
	reader := csv.NewReader(bufio.NewReader(csvFile))
	line, err := reader.Read()
	if err != nil {
		return err
	}

	// Read the data
	// line Numbers start at 1 and we've done the header, hence 2
	lineNumber := 2
	line, err = reader.Read()
	for err == nil {
		err = regions.importCSVLine(line, lineNumber)
		regions.context.LogError(err)
		line, err = reader.Read()
		lineNumber++
	}

	if err != io.EOF {
		return err
	}

	regions.context.LogPrintln("End Import")
	return nil
}
