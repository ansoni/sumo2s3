package main

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/sasha-s/go-deadlock"
	"io/ioutil"
	"log"
	"os"
	"time"
)

// How often we will re-query the same Sumo Job
const QUERY_BREAK time.Duration = 30 * time.Second
//  Max output file size. This is a ceiling, not a gurantee. Depending on data availabiity, these files can be smaller.
const MAX_FILE_SIZE = 900 * MB
// Maximum records per download request. 10000 is the Sumo limit
const MAX_RECORDS = 10000

// Sumo Query Filter by Date
type Filter struct {
	StartDate time.Time `json:"start_date"`
	Query     string    `json:"query"`
}

// Our configuration file
type Config struct {
        // list of ApiKeys
	ApiKeys           [][2]string `json:"apiKeys"`
        // How much load to apply to each key. 4 is the safest value to avoid Sumo API Limits
	ApiKeyConcurrency int         `json:"apiKeyConcurrency"`
        // How many Search Jobs to run. 200 is the Sumo default limit
	OpenJobs          int         `json:"open_jobs"`
        // where to stage data that we retrieve
	Cachefolder       string      `json:"cachefolder"`
        // When to start querying data (2021-10-09T00:00:00Z)
	StartDate         time.Time   `json:"start_date"`
        // When to stop querying data (2021-10-10T00:00:00Z)
	EndDate           time.Time   `json:"end_date"`
        // How much data to query at a time (60s,4h,1d)
	Increment         Duration    `json:"increment"`
        // S3 Bucket to save to
	S3Bucket          string      `json:"s3_bucket"`
        // Final Object Prefix to place data
	S3Prefix          string      `json:"s3_prefix"`
        // Temporary Object Prefix to place data.
        // we do a CopyObject to S3Prefix once all data is uploaded
	S3TmpPrefix       string      `json:"s3_tmp_prefix"`
	AwsRegion         string      `json:"aws_region"`
        // What filters to apply by date
	Filters           []Filter    `json:"filters"`
        // What is the Jobs API to use. This allows you to point
        // to different Sumo Regions (https://api.sumologic.com/api/v1/search/jobs)
	SumoApiJobs       string      `json:"sumo_api_jobs"`
	// apiKeys           chan [2]string
}

// our state so we don't forget what we were doing and can resume
type State struct {
	Active       map[string]*Job `json:"active"`
	NextJobStart time.Time       `json:"next_job_start"`
	lock         deadlock.Mutex
}

func main() {
	// our config file. We load once.
        // you can safely kill the runtime and restart
        // at any time. Jobs that are querying Sumo will
        // reset
	configFile, err := ioutil.ReadFile("config")
	if err != nil {
		log.Fatal(err)
	}
	config := Config{}
	err = json.Unmarshal([]byte(configFile), &config)
	if err != nil {
		log.Fatal(err)
	}

	// state data
	stateFile, err := ioutil.ReadFile("statefile")
	if err != nil {
		log.Fatal(err)
	}
	state := State{}
	err = json.Unmarshal([]byte(stateFile), &state)
	if state.Active == nil {
		state.Active = make(map[string]*Job)
	}
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("ApiKeyConcurrency: ", config.ApiKeyConcurrency)
	fmt.Println("Open Jobs: ", config.OpenJobs)
	jobsPerKey := config.OpenJobs / len(config.ApiKeys)
	if jobsPerKey <= 0 {
		jobsPerKey = 1
	}
	fmt.Println("Jobs Per Key: ", jobsPerKey)
	fmt.Println("StartDate: ", config.StartDate)
	fmt.Println("EndDate: ", config.EndDate)
	fmt.Println("Increment: ", config.Increment)

	// references to our work channels so we can distribute old jobs to them
	var workChans []chan *Job

	// snapshot existing jobs
	var oldJobs []*Job
	for _, job := range state.Active {
		oldJobs = append(oldJobs, job)
	}

	// construct concurrency around ApiKeys (default max 4 concurrency executions)
	// The Query and Download must happen on the same api key
	for _, apiKey := range config.ApiKeys {
		// make sure our inbound work channel can support as much work
		// as we would ever throw at a single worker
		work := make(chan *Job, len(oldJobs)+jobsPerKey) // work to be done
		worked := make(chan *Job, jobsPerKey)            // work that is done
		apiKeys := make(chan [2]string, config.ApiKeyConcurrency) // apiKeys to do the work with
		workChans = append(workChans, work)

		// vend out some off our existing work if we have any
		var oldJob *Job
		for i := 0; i < jobsPerKey; i++ { 
			fmt.Println("Create New Job")
			// feed an existing job to this apiKey worker
			if len(oldJobs) > 0 {
				fmt.Println("Job from previous run")
				oldJob, oldJobs = oldJobs[0], oldJobs[1:]
				oldJob.reset(&state)
				work <- oldJob
			} else {
				fmt.Println("New Job")
				// create a job
				_, newJob := config.nextJob(&state)
				if newJob != nil {
					work <- newJob
				}
			}
		}

		// tickTock reacts to finished jobs. We only need one
		go config.tickTock(work, worked, &state, jobsPerKey)

		// launch our runners/workers
		for i := 0; i < config.ApiKeyConcurrency; i++ {
			apiKeys <- apiKey
			go config.runner(apiKeys, work, worked, &state)
		}
	}

	// feed any remaining oldJobs in as extra work
	// we stripe these across our work channels
	var oldJob *Job
	for len(oldJobs) > 0 {
		fmt.Println("Finding Workers for old jobs")
		frontWorkChan, workChans := workChans[len(workChans)-1], workChans[:len(workChans)-1]
		oldJob, oldJobs = oldJobs[0], oldJobs[1:]
		oldJob.reset(&state)
		frontWorkChan <- oldJob
		// send to back
		workChans = append(workChans, frontWorkChan)
	}

	// start our config saver
	fmt.Println("Launch State Save")
	config.stateSaver(&state)
}

// persist our config our from time to time
func (config *Config) stateSaver(state *State) {
	for {
		state.lock.Lock()
		file, err := json.MarshalIndent(state, "", " ")
		state.lock.Unlock()
		if err != nil {
			log.Fatal(err)
		}
		// fmt.Println("Save Statefile")
		err = ioutil.WriteFile("statefile.tmp", file, 0644)
		if err != nil {
			log.Fatal(err)
		}
		err = os.Rename("statefile.tmp", "statefile")
		if err != nil {
			log.Fatal(err)
		}
		time.Sleep(1 * time.Second)
	}
}

// Create the next job
func (config *Config) nextJob(state *State) (string, *Job) {
	id := uuid.New().String()
	state.lock.Lock()
	startDate := state.NextJobStart
	state.lock.Unlock()
	if startDate.IsZero() {
		startDate = config.StartDate
	}
	if config.EndDate.Before(startDate) || config.EndDate.Equal(startDate) {
		return "", nil
	}
	endDate := startDate.Add(time.Duration(config.Increment))
	job := Job{
		Id:        id,
		StartDate: startDate,
		EndDate:   endDate,
		Status:    Unstarted,
		Query: func() string {
			var filter *Filter
			for _, afilter := range config.Filters {
				if filter == nil || (afilter.StartDate.Before(startDate) && afilter.StartDate.After(filter.StartDate)) {
					filter = &afilter
				}
			}
			return filter.Query
		}(),
	}
	state.lock.Lock()
	state.NextJobStart = endDate
	state.Active[id] = &job
	state.lock.Unlock()
	return id, &job
}

// Process worked jobs (re-scheduling if necessary)
// This could be re-factor'd into runner()
func (config *Config) tickTock(work chan *Job, worked chan *Job, state *State, maxDepth int) {
	for {
		job := <-worked
		fmt.Println("tick: ", job.Id)
		state.lock.Lock()
		status := job.Status
		state.lock.Unlock()
		if status == Done {
			fmt.Println("Job is Done: ", job.Id)
			// remove current job
			state.lock.Lock()
			delete(state.Active, job.Id)
			state.lock.Unlock()
			// start another
			_, job := config.nextJob(state)
			if job == nil {
				fmt.Println("nextJob returned Null. No more jobs to start")
			} else {
				work <- job
			}
		} else {
			fmt.Println("tock: ", job.Id)
			// re-schedule for more work
			work <- job
		}
	}
}

// do work
func (config *Config) runner(apiKeys chan [2]string, work chan *Job, worked chan *Job, state *State) {
	for {
		job := <-work
		fmt.Println("Run Job: ", job.Id, ", Status: ", job.Status)
		state.lock.Lock()
		status := job.Status
		state.lock.Unlock()
		switch status {
		case Unstarted:
			job.kickoff(apiKeys, config, state)
		case Query:
			job.query(apiKeys, config, state)
		case Downloading:
			job.download(apiKeys, config, state)
		case Downloaded:
			job.upload(config, state)
		case Uploading:
			job.upload(config, state)
		case Uploaded:
			job.move(config, state)
		case Moved:
			job.cleanup(config, state)
		case Cleanup:
			job.cleanup(config, state)
		}
		fmt.Println("Job Worked: ", job.Id, ", Status: ", job.Status)
		worked <- job
	}
}
