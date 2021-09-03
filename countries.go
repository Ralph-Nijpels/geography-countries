package countries

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"

	"github.com/minio/minio-go"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	application "github.com/ralph-nijpels/geography-application"
	datatypes "github.com/ralph-nijpels/geography-datatypes"
)

// Countries implements the datamodel for countries

// Countries is the representation of the Countries Collection in the database
type Countries struct {
	context    *application.AppContext
	collection *mongo.Collection
}

// Country is the external representation for an ISO-Country including both a bson (for mongo)
// and a json (for REST/GRAPHQL) representation
type Country struct {
	Country     primitive.ObjectID `bson:"_id" json:"-"`
	CountryCode string             `bson:"iso-country-code" json:"iso-country-code"`
	CountryName string             `bson:"country-name" json:"country-name"`
	Continent   string             `bson:"continent" json:"continent"`
	Wikipedia   string             `bson:"wikipedia" json:"wikipedia,omitempty"`
	Regions     []*Region          `bson:"regions" json:"regions,omitempty"`
}

// NewCountries instantiates the connection to the database collection
func NewCountries(application *application.AppContext) *Countries {
	countries := Countries{context: application}
	
	// Country Collection
	countries.collection = application.DBClient.Database("flight-schedule").Collection("countries")
	countryIndex := mongo.IndexModel{Keys: bson.M{"iso-country-code": 1}}
	countries.collection.Indexes().CreateOne(application.DBContext, countryIndex)

	return &countries
}

// GetByCountryCode retrieves a country based on a CountryCode.
func (countries *Countries) GetByCountryCode(countryCode string) (*Country, error) {
	var result Country

	countryCode, err := datatypes.ISOCountryCode(countryCode, false, false)
	if err != nil {
		return nil, err
	}

	err = countries.collection.FindOne(countries.context.DBContext,
		bson.D{{Key: "iso-country-code", Value: countryCode}}).Decode(&result)

	if err != nil {
		return nil, fmt.Errorf("not found")
	}

	return &result, nil
}

// GetList retrieves a list of countries [fromCountryCode .. untilCountryCode].
func (countries *Countries) GetList(fromCountryCode string, untilCountryCode string) ([]*Country, error) {
	var result []*Country
	var query = bson.D{{}}

	fromCountryCode, err := datatypes.ISOCountryCode(fromCountryCode, true, true)
	if err != nil {
		return nil, fmt.Errorf("fromCountry(%s): %v", fromCountryCode, err)
	}
	if len(fromCountryCode) != 0 {
		query = append(query, bson.E{Key: "iso-country-code",
			Value: bson.D{{Key: "$gte", Value: fromCountryCode}}})
	}

	untilCountryCode, err = datatypes.ISOCountryCode(untilCountryCode, true, true)
	if err != nil {
		return nil, fmt.Errorf("untilCountry(%s): %v", untilCountryCode, err)
	}
	if len(untilCountryCode) != 0 {
		query = append(query, bson.E{Key: "iso-country-code",
			Value: bson.D{{Key: "$lte", Value: untilCountryCode}}})
	}

	findOptions := options.Find()
	findOptions.SetLimit(countries.context.MaxResults + 1)

	cur, err := countries.collection.Find(countries.context.DBContext, query, findOptions)
	if err != nil {
		return nil, fmt.Errorf("not found")
	}

	for cur.Next(countries.context.DBContext) {
		var country Country
		cur.Decode(&country)
		result = append(result, &country)
	}

	cur.Close(countries.context.DBContext)

	if int64(len(result)) > countries.context.MaxResults {
		return nil, fmt.Errorf("too many results")
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("not found")
	}

	return result, nil
}

// RetrieveFromURL downloads the file into the etc directory
func (countries *Countries) RetrieveFromURL() error {
	// Get the data
	resp, err := http.Get(countries.context.CountriesURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Copy the file to S3
	s3Client := countries.context.S3Client
	_, err = s3Client.PutObject("csv", "countries", resp.Body, -1,
		minio.PutObjectOptions{ContentType: "text/csv"})

	return err
}

func (countries *Countries) importCSVLine(line []string, lineNumber int) error {
	// Skipping empty lines
	if len(line) == 0 {
		return nil
	}

	// Check Country Code
	countryCode, err := datatypes.ISOCountryCode(line[1], false, false)
	if err != nil {
		return fmt.Errorf("Countries[%d].CountryCode(%s): %v", lineNumber, line[1], err)
	}

	// The insert type ommits the ID to prevent race conditions in upserting
	type insertCountry struct {
		CountryCode string `bson:"iso-country-code"`
		CountryName string `bson:"country-name"`
		Continent   string `bson:"continent"`
		Wikipedia   string `bson:"wikipedia"`
	}

	// Build internal representation
	country := insertCountry{
		CountryCode: countryCode,
		CountryName: line[2],
		Continent:   line[3],
		Wikipedia:   line[4],
	}

	// Dump in mongo
	_, err = countries.collection.UpdateOne(countries.context.DBContext,
		bson.D{{Key: "iso-country-code", Value: country.CountryCode}},
		bson.M{"$set": country},
		options.Update().SetUpsert(true))

	if err != nil {
		return fmt.Errorf("countries[%d]: %v", lineNumber, err)
	}

	return nil
}

// ImportCSV imports a list of countries from a CSV-file
func (countries *Countries) ImportCSV() error {
	// Open the country.csv file
	s3Client := countries.context.S3Client
	csvFile, err := s3Client.GetObject("csv", "countries",
		minio.GetObjectOptions{})
	if err != nil {
		return err
	}
	defer csvFile.Close()

	// Open the logfile
	_, err = countries.context.LogFile("countries")
	if err != nil {
		return err
	}
	defer countries.context.LogClose()

	countries.context.LogPrintln("Start Import")

	// Skip the headerline
	reader := csv.NewReader(bufio.NewReader(csvFile))
	_, err = reader.Read()
	if err != nil {
		return err
	}

	// Read the data
	// LineNumbers start at 1 and we've done the header (hence 2)
	lineNumber := 2
	line, err := reader.Read()
	for err == nil {
		err = countries.importCSVLine(line, lineNumber)
		countries.context.LogError(err)
		line, err = reader.Read()
		lineNumber++
	}

	if err != io.EOF {
		return err
	}

	countries.context.LogPrintln("End Import")
	return nil
}
