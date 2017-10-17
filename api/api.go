package api

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/ajvb/kala/api/middleware"
	"github.com/ajvb/kala/job"

	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/ajvb/kala/job/fixed"
	"github.com/ajvb/kala/job/storage/boltdb"
	"github.com/codegangsta/negroni"
	"github.com/gorilla/mux"
	"github.com/rs/cors"
	"strings"
	"time"
)

const (
	// Base API v1 Path
	ApiUrlPrefix = "/api/v1/"

	JobPath    = "job/"
	ApiJobPath = ApiUrlPrefix + JobPath

	contentType     = "Content-Type"
	jsonContentType = "application/json;charset=UTF-8"
)

type KalaStatsResponse struct {
	Stats *job.KalaStats
}

// HandleKalaStatsRequest is the hanlder for getting system-level metrics
// /api/v1/stats
func HandleKalaStatsRequest(cache job.JobCache) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		resp := &KalaStatsResponse{
			Stats: job.NewKalaStats(cache),
		}

		w.Header().Set(contentType, jsonContentType)
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			log.Errorf("Error occured when marshalling response: %s", err)
			return
		}
	}
}

type ListJobStatsResponse struct {
	JobStats []*job.JobStat `json:"job_stats"`
}

// HandleListJobStatsRequest is the handler for getting job-specific stats
// /api/v1/job/stats/{id}
func HandleListJobStatsRequest(cache job.JobCache) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		id := mux.Vars(r)["id"]
		j, err := cache.Get(id)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		resp := &ListJobStatsResponse{
			JobStats: j.Stats,
		}

		w.Header().Set(contentType, jsonContentType)
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			log.Errorf("Error occured when marshalling response: %s", err)
			return
		}
	}
}

func unmarshalNewFixedJobs(r *http.Request, request *FixedJobsRequest) error {
	body, err := ioutil.ReadAll(io.LimitReader(r.Body, 1048576))
	if err != nil {
		log.Errorf("Error occured when reading r.Body: %s", err)
		return err
	}
	defer r.Body.Close()

	if err := json.Unmarshal(body, &request); err != nil {
		log.Errorf("Error occured when unmarshalling data: %s", err)
		return err
	}

	return nil
}

type FixedJobError struct {
	*fixed.Job
	Method string
	Error  string
}

type FixedJobsResponse struct {
	Deleted []*fixed.Job
	Added   []*fixed.Job
	Errors  []*FixedJobError
}

type FixedJobsRequest struct {
	Url  string
	Jobs []*fixed.Job
}

func HandleFixedJobsRequest(cache job.JobCache, db job.JobDB, fixedJobs *[]*fixed.Job) func(w http.ResponseWriter, r *http.Request) {
	const (
		MethodAdd    = "Add"
		MethodDelete = "Delete"
	)

	return func(w http.ResponseWriter, r *http.Request) {
		requestJobs := FixedJobsRequest{}
		err := unmarshalNewFixedJobs(r, &requestJobs)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		reqFixedJobs := requestJobs.Jobs

		errors := []*FixedJobError{}
		addingJobs := []*fixed.Job{}
		deletingJobs := []*fixed.Job{}
		for _, j := range reqFixedJobs {
			found := false
			var j2 *fixed.Job
			for _, j2 = range *fixedJobs {
				if j.Name == j2.Name {
					found = true
					break
				}
			}
			if !found {
				addingJobs = append(addingJobs, j)
			}
		}
		for _, j2 := range *fixedJobs {
			found := false
			for _, j := range reqFixedJobs {
				if j.Name == j2.Name {
					found = true
					break
				}
			}
			if !found {
				deletingJobs = append(deletingJobs, j2)
			}
		}

		for _, fixedJ := range deletingJobs {
			j, err := cache.Get(fixedJ.ID)
			if err != nil {
				log.Error("failed to get fixed job", fixedJ.ID, err)
				continue
			}

			j.Delete(cache, db)
			for i, j := range *fixedJobs {
				if j.ID == fixedJ.ID {
					*fixedJobs = append((*fixedJobs)[:i], (*fixedJobs)[i+1:]...)
				}
			}
		}

		for _, addingJ := range addingJobs {
			now := time.Now()
			now = now.Add(10 * time.Second)

			var intervalBetweenRuns string
			if strings.HasSuffix(addingJ.Name, "-site-crawler") {
				intervalBetweenRuns = fixed.DefaultSiteCrawlerInterval
			} else if strings.HasSuffix(addingJ.Name, "-product-pages-crawler") {
				intervalBetweenRuns = fixed.DefaultProductPagesCrawlerInterval
			} else {
				errStr := fmt.Sprintf(`job suffix must be "-site-crawler" OR "-product-pages-crawler"`)
				log.Error("job name: "+addingJ.Name, errStr)
				errors = append(errors, &FixedJobError{
					Job:    addingJ,
					Method: "Add",
					Error:  errStr,
				})
				continue
			}

			newJob := &job.Job{
				Name:     addingJ.Name,
				Schedule: fmt.Sprintf("R/%s/", now.Format(time.RFC3339), intervalBetweenRuns),
				JobType:  job.RemoteJob,
				RemoteProperties: job.RemoteProperties{
					Url:                   requestJobs.Url,
					Method:                "POST",
					Headers:               make(http.Header),
					Body:                  fmt.Sprintf(`{"job":"%s"}`, addingJ.Name),
					Timeout:               30,
					ExpectedResponseCodes: []int{200, 201},
				},
				Retries: 2,
				Epsilon: "PT1M", // retry per 1 minute
			}

			err = newJob.Init(cache)
			if err != nil {
				errStr := fmt.Sprintf("Error occured when initializing the job: %s", err)
				log.Error(errStr)
				errors = append(errors, &FixedJobError{
					Job:    addingJ,
					Method: "Add",
					Error:  errStr,
				})
				continue
			}
			addingJ.ID = newJob.Id
			*fixedJobs = append(*fixedJobs, addingJ)
		}

		w.Header().Set(contentType, jsonContentType)
		if len(errors) > 0 {
			for _, errorJ := range errors {
				switch errorJ.Method {
				case MethodAdd:
					for i, v := range addingJobs {
						if v.Name == errorJ.Job.Name {
							addingJobs = append(addingJobs[:i], addingJobs[i+1:]...)
						}
					}
				case MethodDelete:
					for i, v := range deletingJobs {
						if v.Name == errorJ.Job.Name {
							deletingJobs = append(deletingJobs[:i], deletingJobs[i+1:]...)
						}
					}
				default:
					log.Fatal("unknown method", errorJ.Method)
				}
			}
			w.WriteHeader(http.StatusBadRequest)
		} else {
			w.WriteHeader(http.StatusOK)
			if len(deletingJobs) > 0 || len(addingJobs) > 0 {
				fixed.PersistFixedJobs(db.(*boltdb.BoltJobDB), fixedJobs)
			}
		}

		resp := &FixedJobsResponse{
			Deleted: deletingJobs,
			Added:   addingJobs,
			Errors:  errors,
		}

		if err := json.NewEncoder(w).Encode(resp); err != nil {
			log.Errorf("Error occured when marshalling response: %s", err)
			return
		}
	}
}

type ListJobsResponse struct {
	Jobs map[string]*job.Job `json:"jobs"`
}

// HandleListJobs responds with an array of all Jobs within the server,
// active or disabled.
func HandleListJobsRequest(cache job.JobCache) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		allJobs := cache.GetAll()
		allJobs.Lock.RLock()
		defer allJobs.Lock.RUnlock()

		resp := &ListJobsResponse{
			Jobs: allJobs.Jobs,
		}

		w.Header().Set(contentType, jsonContentType)
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			log.Errorf("Error occured when marshalling response: %s", err)
			return
		}
	}
}

type AddJobResponse struct {
	Id string `json:"id"`
}

func unmarshalNewJob(r *http.Request) (*job.Job, error) {
	newJob := &job.Job{}

	body, err := ioutil.ReadAll(io.LimitReader(r.Body, 1048576))
	if err != nil {
		log.Errorf("Error occured when reading r.Body: %s", err)
		return nil, err
	}
	defer r.Body.Close()

	if err := json.Unmarshal(body, newJob); err != nil {
		log.Errorf("Error occured when unmarshalling data: %s", err)
		return nil, err
	}

	return newJob, nil
}

// HandleAddJob takes a job object and unmarshals it to a Job type,
// and then throws the job in the schedulers.
func HandleAddJob(cache job.JobCache, defaultOwner string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		newJob, err := unmarshalNewJob(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if defaultOwner != "" && newJob.Owner == "" {
			newJob.Owner = defaultOwner
		}

		err = newJob.Init(cache)
		if err != nil {
			errStr := "Error occured when initializing the job"
			log.Errorf(errStr+": %s", err)
			http.Error(w, errStr, http.StatusBadRequest)
			return
		}

		resp := &AddJobResponse{
			Id: newJob.Id,
		}

		w.Header().Set(contentType, jsonContentType)
		w.WriteHeader(http.StatusCreated)
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			log.Errorf("Error occured when marshalling response: %s", err)
			return
		}
	}
}

// HandleJobRequest routes requests to /api/v1/job/{id} to either
// handleDeleteJob if its a DELETE or handleGetJob if its a GET request.
func HandleJobRequest(cache job.JobCache, db job.JobDB) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		id := mux.Vars(r)["id"]

		j, err := cache.Get(id)
		if err != nil {
			log.Errorf("Error occured when trying to get the job you requested.")
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if j == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		if r.Method == "DELETE" {
			err = j.Delete(cache, db)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
		} else if r.Method == "GET" {
			handleGetJob(w, r, j)
		}
	}
}

type JobResponse struct {
	Job *job.Job `json:"job"`
}

func handleGetJob(w http.ResponseWriter, r *http.Request, j *job.Job) {
	resp := &JobResponse{
		Job: j,
	}

	w.Header().Set(contentType, jsonContentType)
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Errorf("Error occured when marshalling response: %s", err)
		return
	}
}

// HandleStartJobRequest is the handler for manually starting jobs
// /api/v1/job/start/{id}
func HandleStartJobRequest(cache job.JobCache) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		id := mux.Vars(r)["id"]
		j, err := cache.Get(id)
		if err != nil {
			log.Errorf("Error occured when trying to get the job you requested.")
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if j == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		j.StopTimer()
		j.Run(cache)

		w.WriteHeader(http.StatusNoContent)
	}
}

// HandleDisableJobRequest is the handler for mdisabling jobs
// /api/v1/job/disable/{id}
func HandleDisableJobRequest(cache job.JobCache) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		id := mux.Vars(r)["id"]
		j, err := cache.Get(id)
		if err != nil {
			log.Errorf("Error occured when trying to get the job you requested.")
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if j == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		j.Disable()

		w.WriteHeader(http.StatusNoContent)
	}
}

// HandleEnableJobRequest is the handler for enable jobs
// /api/v1/job/enable/{id}
func HandleEnableJobRequest(cache job.JobCache) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		id := mux.Vars(r)["id"]
		j, err := cache.Get(id)
		if err != nil {
			log.Errorf("Error occured when trying to get the job you requested.")
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if j == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		j.Enable(cache)

		w.WriteHeader(http.StatusNoContent)
	}
}

// SetupApiRoutes is used within main to initialize all of the routes
func SetupApiRoutes(r *mux.Router, cache job.JobCache, db job.JobDB, defaultOwner string, fixedJobs *[]*fixed.Job) {
	// Route for creating a job
	r.HandleFunc(ApiJobPath, HandleAddJob(cache, defaultOwner)).Methods("POST")
	// Route for deleting and getting a job
	r.HandleFunc(ApiJobPath+"{id}/", HandleJobRequest(cache, db)).Methods("DELETE", "GET")
	// Route for getting job stats
	r.HandleFunc(ApiJobPath+"stats/{id}/", HandleListJobStatsRequest(cache)).Methods("GET")
	// Route for listing all jops
	r.HandleFunc(ApiJobPath, HandleListJobsRequest(cache)).Methods("GET")
	// Route for manually start a job
	r.HandleFunc(ApiJobPath+"start/{id}/", HandleStartJobRequest(cache)).Methods("POST")
	// Route for manually start a job
	r.HandleFunc(ApiJobPath+"enable/{id}/", HandleEnableJobRequest(cache)).Methods("POST")
	// Route for manually disable a job
	r.HandleFunc(ApiJobPath+"disable/{id}/", HandleDisableJobRequest(cache)).Methods("POST")
	// Route for getting app-level metrics
	r.HandleFunc(ApiUrlPrefix+"stats/", HandleKalaStatsRequest(cache)).Methods("GET")
	// Route for syncing fixed jobs
	r.HandleFunc(ApiUrlPrefix+"fixed-jobs", HandleFixedJobsRequest(cache, db, fixedJobs)).Methods("POST")
}

func StartServer(listenAddr string, cache job.JobCache, db job.JobDB, defaultOwner string, fixedJobs *[]*fixed.Job) error {
	r := mux.NewRouter()
	// Allows for the use for /job as well as /job/
	r.StrictSlash(true)
	SetupApiRoutes(r, cache, db, defaultOwner, fixedJobs)
	n := negroni.New(negroni.NewRecovery(), &middleware.Logger{log.Logger{}})
	n.UseHandler(r)
	handler := cors.New(cors.Options{
		AllowedMethods: []string{"GET", "POST", "DELETE", "PATCH"},
	}).Handler(n)
	return http.ListenAndServe(listenAddr, handler)
}
