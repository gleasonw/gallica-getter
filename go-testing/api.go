package main

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
	"github.com/danielgtaylor/huma/v2/humacli"
)

// Root struct for the entire response
type SearchRetrieveResponse struct {
	XMLName                     xml.Name                    `xml:"searchRetrieveResponse" json:"-"`
	Version                     string                      `xml:"version" json:"version"`
	EchoedSearchRetrieveRequest EchoedSearchRetrieveRequest `xml:"echoedSearchRetrieveRequest" json:"echoedSearchRetrieveRequest"`
	NumberOfRecords             int                         `xml:"numberOfRecords" json:"numberOfRecords"`
	Records                     Records                     `xml:"records" json:"records"`
	NextRecordPosition          int                         `xml:"nextRecordPosition" json:"nextRecordPosition"`
}

type EchoedSearchRetrieveRequest struct {
	Query   string `xml:"query" json:"query"`
	Version string `xml:"version" json:"version"`
}

type Records struct {
	Record []Record `xml:"record" json:"record"`
}

type Record struct {
	RecordSchema     string          `xml:"recordSchema" json:"recordSchema"`
	RecordPacking    string          `xml:"recordPacking" json:"recordPacking"`
	RecordData       RecordData      `xml:"recordData" json:"recordData"`
	RecordIdentifier string          `xml:"recordIdentifier" json:"recordIdentifier"`
	RecordPosition   int             `xml:"recordPosition" json:"recordPosition"`
	ExtraRecordData  ExtraRecordData `xml:"extraRecordData" json:"extraRecordData"`
}

type RecordData struct {
	DC DC `xml:"dc" json:"dc"`
}

type DC struct {
	Creator     string   `xml:"creator" json:"creator"`
	Date        string   `xml:"date" json:"date"`
	Description []string `xml:"description" json:"description"`
	Format      string   `xml:"format" json:"format"`
	Identifier  string   `xml:"identifier" json:"identifier"`
	Language    string   `xml:"language" json:"language"`
	Publisher   string   `xml:"publisher" json:"publisher"`
	Relation    []string `xml:"relation" json:"relation"`
	Rights      []string `xml:"rights" json:"rights"`
	Source      string   `xml:"source" json:"source"`
	Title       string   `xml:"title" json:"title"`
	Type        []string `xml:"type" json:"type"`
}

type ExtraRecordData struct {
	EpubFile   string `xml:"epubFile" json:"epubFile"`
	Highres    string `xml:"highres" json:"highres"`
	Link       string `xml:"link" json:"link"`
	Lowres     string `xml:"lowres" json:"lowres"`
	Medres     string `xml:"medres" json:"medres"`
	Nqamoyen   string `xml:"nqamoyen" json:"nqamoyen"`
	Provenance string `xml:"provenance" json:"provenance"`
	Thumbnail  string `xml:"thumbnail" json:"thumbnail"`
	Typedoc    string `xml:"typedoc" json:"typedoc"`
	Uri        string `xml:"uri" json:"uri"`
}

// Options for the CLI. Pass `--port` or set the `SERVICE_PORT` env var.
type Options struct {
	Port int `help:"Port to listen on" short:"p" default:"8888"`
}

// GreetingOutput represents the greeting operation response.
type GreetingOutput struct {
	Body SearchRetrieveResponse
}

func main() {
	// Create a CLI app which takes a port option.
	cli := humacli.New(func(hooks humacli.Hooks, options *Options) {
		// Create a new router & API
		router := http.NewServeMux()
		api := humago.New(router, huma.DefaultConfig("My API", "1.0.0"))

		// Add the operation handler to the API.
		huma.Get(api, "/testfetch", func(ctx context.Context, input *struct{}) (*GreetingOutput, error) {
			response, err := http.Get("https://gallica.bnf.fr/SRU?operation=searchRetrieve&exactSearch=True&version=1.2&startRecord=0&maximumRecords=10&collapsing=false&query=%28text+adj+%22brazza%22%29+and+dc.type+all+%22fascicule%22+or+dc.type+all+%22monographie%22")

			if err != nil {
				return nil, err
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil {
				return nil, err
			}
			srwResponse := SearchRetrieveResponse{}
			err = xml.Unmarshal(body, &srwResponse)
			if err != nil {
				return nil, err
			}
			return &GreetingOutput{Body: srwResponse}, nil
		})

		// Tell the CLI how to start your router.
		hooks.OnStart(func() {
			fmt.Println("Starting server on port", options.Port)
			http.ListenAndServe(fmt.Sprintf(":%d", options.Port), router)
		})

		hooks.OnStop(func() {
			fmt.Println("Stopping server")
		})
	})

	// Run the CLI. When passed no commands, it starts the server.
	cli.Run()
}
