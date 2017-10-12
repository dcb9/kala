package fixed

import (
	"bytes"
	"encoding/gob"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/ajvb/kala/job/storage/boltdb"
	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"time"
)

type Job struct {
	Name     string
	Schedule string
	ID       string
}

var (
	fixedJobsBucket = []byte("fixed-jobs")
	fixedJobsKey    = []byte("fixed-jobs")
)

func LoadFixedJobs(db *boltdb.BoltJobDB) ([]*Job, error) {
	fixedJobs, err := loadFixedJobs(db)
	if err != nil {
		return nil, errors.Wrap(err, "load fixed jobs")
	}

	log.Debug("loaded fixed jobs", fmt.Sprintf("%#v", fixedJobs))

	return fixedJobs, nil
}

func loadFixedJobs(db *boltdb.BoltJobDB) ([]*Job, error) {
	allJobs := []*Job{}
	err := db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(fixedJobsBucket)
		if err != nil {
			return err
		}

		val := bucket.Get(fixedJobsKey)
		if val != nil {
			buffer := bytes.NewBuffer(val)
			dec := gob.NewDecoder(buffer)
			err := dec.Decode(&allJobs)
			if err != nil {
				return errors.Wrap(err, "dec.Decode all fixed jobs")
			}
		}

		return nil
	})

	return allJobs, err
}

func PersistEvery(duration time.Duration, db *boltdb.BoltJobDB, jobs *[]*Job) {
	wait := time.Tick(duration)
	var err error
	for {
		<-wait
		log.Debug(fmt.Sprintf("fixed.PersistEvery(%d seconds)", duration/time.Second))
		err = PersistFixedJobs(db, jobs)
		if err != nil {
			log.Errorf("Error occured persisting the database. Err: %s", err)
		}
	}
}

func PersistFixedJobs(db *boltdb.BoltJobDB, jobs *[]*Job) error {
	log.Debug("start persisting fixed jobs", fmt.Sprintf("%#v", *jobs))
	err := db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(fixedJobsBucket)
		if err != nil {
			return err
		}

		buffer := new(bytes.Buffer)
		enc := gob.NewEncoder(buffer)
		err = enc.Encode(*jobs)
		if err != nil {
			return err
		}

		return bucket.Put(fixedJobsKey, buffer.Bytes())
	})

	return err
}
