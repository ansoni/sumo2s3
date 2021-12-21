package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Simple concrete error for reporting when a job is lost beyond repair
type LostError struct {
        When time.Time
        What string
}

func (e LostError) Error() string {
        return fmt.Sprintf("%v: %v", e.When, e.What)
}

// Job Status/Steps
type Status string
const (
        Unstarted   Status = "unstarted"
        Query              = "query"
        Downloading        = "downloading"
        Downloaded         = "downloaded"
        Uploading          = "uploading"
        Uploaded           = "uploaded"
        Cleanup            = "cleanup"
        Moved              = "moved"
        Done               = "done"
)

// a job
type Job struct {
	// unique id. gets initialized as a UUID
	Id               string    `json:"id"`
	// What Sumo things of the state. GATHERING/DONE GATHERING
	SumoJobState     string    `json:"sumo_job_state"`
	Status           Status    `json:"status"`
	// Files that we have created in this job
	Files            []string  `json:"files"`
	// S3 Objects that we have uploaded in this job
	Objects          []string  `json:"objects"`
	// What file number are we on
	Number           int       `json:"number"`
	CurrentFileSize  float64   `json:"currentFileSize"`
	// How many pages have we downloaded from Sumo
	// used mainly for determining when we should
	// shift from query->downloading
	Page             int64     `json:"page"`
	// How many Logs are available
	TotalLogs        int64     `json:"total_logs"`
	// How many Logs have we retrieved
	TotalLogsFetched int64     `json:"records_fetched"`
	StartDate        time.Time `json:"start_date"`
	EndDate          time.Time `json:"end_date"`
	Query            string    `json:"query"`
	// Sumo Job URL
	JobUrl           string    `json:"job_url"`
	// Last time we queried the sumo job
	lastQuery        time.Time
	// Our current ouputs
	currentWriter    *gzip.Writer
	currentFile      *os.File
}

// Resets jobs that are not in a recoverable state.
// Mainly mid-download jobs that would lose data
func (job *Job) reset(state *State) {
	state.lock.Lock()
	status := job.Status
	state.lock.Unlock()
	switch status {
	case Query:
	case Downloading:
		job.deleteJobFiles(state)
		state.lock.Lock()
		job.Status = Unstarted
		job.CurrentFileSize = 0
		job.Files = make([]string, 0)
		job.Objects = make([]string, 0)
		job.Number = 0
		job.Page = 0
		job.TotalLogs = 0
		job.TotalLogsFetched = 0
		state.lock.Unlock()
	}
}

// Delete Job Files
func (job *Job) deleteJobFiles(state *State) {
	state.lock.Lock()
	defer state.lock.Unlock()
	for _, f := range job.Files {
		err := os.Remove(f)
		if err != nil {
			fmt.Println("Error Removing: ", err)
			return
		}
	}
}

// close our current file
func (job *Job) close(state *State) {
	state.lock.Lock()
	if job.currentWriter != nil {
		job.currentWriter.Flush()
		job.currentWriter.Close()
		job.currentFile.Close()
		job.currentWriter = nil
		job.currentFile = nil
	}
	state.lock.Unlock()
}

// write job data. All file rolling business logic is handled here
func (job *Job) write(config *Config, state *State, data []byte) error {
	state.lock.Lock()
	if job.currentWriter == nil || job.CurrentFileSize > float64(MAX_FILE_SIZE) {
		if job.currentWriter != nil {
			job.currentWriter.Flush()
			job.currentWriter.Close()
			job.currentFile.Close()
		}
		fullFileName := job.nextFileName(config)
		fmt.Println("Open File: ", fullFileName)
		f, err := os.OpenFile(fullFileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0660)
		if err != nil {
			log.Printf("Error in Create\n")
			panic(err)
		}
		job.Files = append(job.Files, fullFileName)
		job.currentFile = f
		job.currentWriter = gzip.NewWriter(f)
		job.CurrentFileSize = 0
	}
	writer := job.currentWriter
	state.lock.Unlock()
	writer.Write(data)
	writer.Flush()
	state.lock.Lock()
	job.CurrentFileSize += float64(len(data))
	state.lock.Unlock()
	return nil
}

func (job *Job) nextFileName(config *Config) string {
	fullFileName := fmt.Sprintf("%s/%s-%d.gz", config.Cachefolder, job.StartDate, job.Number)
	job.Number += 1
	return fullFileName
}

func (job *Job) totalPages(state *State) int64 {
	state.lock.Lock()
	defer state.lock.Unlock()
	return job.TotalLogs / MAX_RECORDS
}

func (job *Job) pagesLeftToFetch(state *State) int64 {
	state.lock.Lock()
	page := job.Page
	state.lock.Unlock()
	return job.totalPages(state) - page
}

func (job *Job) logsLeft(state *State) int64 {
	state.lock.Lock()
	defer state.lock.Unlock()
	return job.TotalLogs - job.TotalLogsFetched
}


func (job *Job) kickoff(apiKeys chan [2]string, config *Config, state *State) {
	err := job.sumoCreateJob(apiKeys, config, state)
	if err != nil {
		// error kicking off jobs is not good!
		fmt.Println("Error: ", err)
		defer time.Sleep(5 * time.Second)
		return
	}
	state.lock.Lock()
	job.Status = Query
	state.lock.Unlock()
}

// query the job for data to download
func (job *Job) query(apiKeys chan [2]string, config *Config, state *State) {
	state.lock.Lock()
	lastQuery := job.lastQuery
	state.lock.Unlock()
	if !lastQuery.IsZero() && time.Now().Sub(lastQuery) < QUERY_BREAK {
		defer time.Sleep(5 * time.Second)
		return
	}
	sumoJobStatus, err := job.sumoQueryJob(apiKeys, config, state)
	if err != nil {
		fmt.Println("Query Error: ", err)
		if _, ok := err.(*LostError); ok {
			state.lock.Lock()
			job.Status = Unstarted
			state.lock.Unlock()
			defer time.Sleep(5 * time.Second)
			return
		}
		defer time.Sleep(5 * time.Second)
		return
	}
	state.lock.Lock()
	job.lastQuery = time.Now()
	job.SumoJobState = sumoJobStatus.State
	state.lock.Unlock()
	if sumoJobStatus.State == "CANCELLED" {
		state.lock.Lock()
		job.Status = Unstarted // re-kickoff
		state.lock.Unlock()
		return
	}
	state.lock.Lock()
	fmt.Println("Total Logs: ", sumoJobStatus.MessageCount)
	job.TotalLogs = sumoJobStatus.MessageCount
	state.lock.Unlock()
	pagesToFetch := job.pagesLeftToFetch(state)
	if pagesToFetch > 20 || sumoJobStatus.State == "DONE GATHERING RESULTS" {
		fmt.Printf("Pages to Fetch %d", pagesToFetch)
		state.lock.Lock()
		job.Status = Downloading
		state.lock.Unlock()
	}
}

// download pages
func (job *Job) download(apiKeys chan [2]string, config *Config, state *State) {
	err := job.sumoDownloadPages(apiKeys, config, state)
	//job.close()
	if err != nil {
		fmt.Println("Download Error: ", err)
		if _, ok := err.(*LostError); ok {
			state.lock.Lock()
			defer state.lock.Unlock()
			job.Status = Unstarted
		}
		return
	}
	state.lock.Lock()
	sumoState := job.SumoJobState
	state.lock.Unlock()
	if sumoState == "DONE GATHERING RESULTS" {
		state.lock.Lock()
		job.Status = Downloaded
		state.lock.Unlock()
		job.close(state)
		err := job.sumoDeleteJob(apiKeys, config, state)
		if err != nil {
			fmt.Println("Error Deleting Job", err)
		}
	} else {
		state.lock.Lock()
		job.Status = Query
		state.lock.Unlock()
	}
}

// upload to S3
func (job *Job) upload(config *Config, state *State) {
	svc := s3.New(session.New(&aws.Config{
		Region: aws.String(config.AwsRegion),
	}))
	state.lock.Lock()
	job.Status = Uploading
	objects := make([]string, 0)
	files := job.Files
	state.lock.Unlock()
	for _, f := range files {
		_, fileName := filepath.Split(f)
		key := fmt.Sprintf("%s/%s", config.S3TmpPrefix, fileName)
		fmt.Printf("S3 Put %s to %s/%s", f, config.S3Bucket, key)
		fh, err := os.Open(f)
		if err != nil {
			log.Println("Error in open", err)
			return
		}
		defer fh.Close()
		_, err = svc.PutObject(&s3.PutObjectInput{Bucket: aws.String(config.S3Bucket), Key: aws.String(key), Body: fh})
		if err != nil {
			fmt.Println("Upload Error: ", err)
			defer time.Sleep(5 * time.Second)
			return
		}
		objects = append(objects, key)
	}
	state.lock.Lock()
	job.Objects = objects
	job.Status = Uploaded
	state.lock.Unlock()
}

// move objects in S3
func (job *Job) move(config *Config, state *State) {
	svc := s3.New(session.New(&aws.Config{
		Region: aws.String(config.AwsRegion),
	}))
	state.lock.Lock()
	defer state.lock.Unlock()
	for _, k := range job.Objects {
		_, objectName := filepath.Split(k)
		key := fmt.Sprintf("%s/%d/%s/%d/%s", config.S3Prefix, job.StartDate.Year(), job.StartDate.Month(), job.StartDate.Day(), objectName)
		input := &s3.CopyObjectInput{
			Bucket:     aws.String(config.S3Bucket),
			CopySource: aws.String(url.QueryEscape(fmt.Sprintf("/%s/%s", config.S3Bucket, k))),
			Key:        aws.String(key),
		}
		fmt.Printf("S3 Move %s/%s to %s/%s", config.S3Bucket, k, config.S3Bucket, key)
		_, err := svc.CopyObject(input)
		if err != nil {
			fmt.Println("Move Error: ", err)
			defer time.Sleep(1 * time.Second)
			return
		}
	}
	for _, k := range job.Objects {
		input := &s3.DeleteObjectInput{
			Bucket: aws.String(config.S3Bucket),
			Key:    aws.String(k),
		}
		_, err := svc.DeleteObject(input)
		if err != nil {
			fmt.Println("Cleanup Failed: ", err)
			defer time.Sleep(1 * time.Second)
			job.Status = Uploading
			return
		}
	}
	job.Status = Moved
}

// Cleanup local caches
func (job *Job) cleanup(config *Config, state *State) {
	job.deleteJobFiles(state)
	state.lock.Lock()
	defer state.lock.Unlock()
	job.Status = Done
}

/*** Sumo related functions ***/

type SumoJobStatus struct {
	State        string `json:"state"`
	MessageCount int64  `json:"messageCount"`
	RecordCount  int64  `json:"recordCount"`
}

func (job *Job) sumoDownloadPages(apiKeys chan [2]string, config *Config, state *State) error {
	client := &http.Client{Timeout: 20 * time.Second}
	apiKey := <-apiKeys
	defer func() { apiKeys <- apiKey }()
	for job.pagesLeftToFetch(state) > 0 || job.SumoJobState == "DONE GATHERING RESULTS" {

		state.lock.Lock()
		offset := job.TotalLogsFetched
		jobUrl := job.JobUrl
		page := job.Page
		state.lock.Unlock()
		if offset != 0 {
			offset += 1
		}
		pageUrl := fmt.Sprintf("%s/messages?offset=%d&limit=%d", jobUrl, offset, MAX_RECORDS)
		fmt.Printf("Get Page %d/%d via %s (%s) (pages %d left)\n", page, job.totalPages(state), pageUrl, apiKey[0], job.pagesLeftToFetch(state))
		req, err := http.NewRequest("GET", pageUrl, nil)
		if err != nil {
			fmt.Printf("Error: ", err)
			return err
		}
		req.SetBasicAuth(apiKey[0], apiKey[1])
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		if resp.StatusCode == http.StatusOK {
			d, _ := ioutil.ReadAll(resp.Body)
			respJson := make(map[string][]interface{})
			err = json.Unmarshal(d, &respJson)
			if err != nil {
				fmt.Println("Error: ", err)
				return err
			}
			numMessages := len(respJson["messages"])
			fmt.Printf("%s yielded %d records\n", pageUrl, numMessages)
			if numMessages == 0 { // we bail if no messages are found. This should only happen on the "DONE GATHERING RESULTS" flow
				return nil
			}
			state.lock.Lock()
			job.TotalLogsFetched += int64(numMessages)
			state.lock.Unlock()
			for _, jsonContent := range respJson["messages"] {
				buffer, err := json.Marshal(jsonContent)
				if err != nil {
					return err
				}
				job.write(config, state, buffer)
				job.write(config, state, []byte("\n"))
			}
			state.lock.Lock()
			job.Page += 1
			state.lock.Unlock()
		} else if resp.StatusCode == 404 || resp.StatusCode == 400 {
			return &LostError{time.Now(), resp.Status}
		} else {
			errString := fmt.Sprintf("Unexpected Response: %d", resp.StatusCode)
			return errors.New(errString)
		}
	}
	return nil
}

func (job *Job) sumoDeleteJob(apiKeys chan [2]string, config *Config, state *State) error {
	client := &http.Client{Timeout: 20 * time.Second}
	apiKey := <-apiKeys
	defer func() { apiKeys <- apiKey }()
	state.lock.Lock()
	jobUrl := job.JobUrl
	state.lock.Unlock()
	req, err := http.NewRequest("DELETE", jobUrl, nil)
	fmt.Printf("DELETE Job %s (%s)\n", jobUrl, apiKey[0])
	req.SetBasicAuth(apiKey[0], apiKey[1])
	_, err = client.Do(req)
	if err != nil {
		return err
	}
	return nil
}

func (job *Job) sumoQueryJob(apiKeys chan [2]string, config *Config, state *State) (*SumoJobStatus, error) {
	client := &http.Client{Timeout: 20 * time.Second}
	apiKey := <-apiKeys
	state.lock.Lock()
	jobUrl := job.JobUrl
	state.lock.Unlock()
	defer func() { apiKeys <- apiKey }()
	req, err := http.NewRequest("GET", jobUrl, nil)
	req.Header.Add("Content-Type", "application/json")
	fmt.Printf("Query Job %s (%s)\n", jobUrl, apiKey[0])
	req.SetBasicAuth(apiKey[0], apiKey[1])
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusOK {
		sumoJobStatus := SumoJobStatus{}
		body, err := ioutil.ReadAll(resp.Body)
		fmt.Println(string(body))
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(body, &sumoJobStatus)
		if err != nil {
			return nil, err
		}
		return &sumoJobStatus, nil
	} else if resp.StatusCode == 404 || resp.StatusCode == 400 {
		return nil, &LostError{time.Now(), resp.Status}
	}
	fmt.Println("Sumo Response: ", resp.StatusCode, " ", resp.Status)
	return nil, errors.New(fmt.Sprintf("%d", resp.StatusCode))
}

func (job *Job) sumoCreateJob(apiKeys chan [2]string, config *Config, state *State) error {
	client := &http.Client{Timeout: 20 * time.Second}
	apiKey := <-apiKeys
	defer func() { apiKeys <- apiKey }()
	jsonContent := map[string]string{
		"query":    job.Query,
		"from":     job.StartDate.Format("2006-01-02T15:04:05"),
		"to":       job.EndDate.Format("2006-01-02T15:04:05"),
		"timeZone": "UTC",
	}
	buffer, err := json.Marshal(jsonContent)
	if err != nil {
		return err
	}
	fmt.Println("Payload: ", string(buffer), ", Auth: ", apiKey)
	req, err := http.NewRequest("POST", config.SumoApiJobs, bytes.NewReader(buffer))
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")
	req.SetBasicAuth(apiKey[0], apiKey[1])
	fmt.Printf("Create Job %s (%s)\n%s\n", config.SumoApiJobs, apiKey[0], jsonContent)
	resp, err := client.Do(req)
	if resp.StatusCode == http.StatusAccepted {
		state.lock.Lock()
		job.JobUrl = resp.Header.Get("Location")
		state.lock.Unlock()
		return nil
	}
	fmt.Println("Sumo Response: ", resp.StatusCode, " ", resp.Status)
	respContent, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return errors.New(fmt.Sprintf("Error from Sumo: ", string(respContent)))
}
