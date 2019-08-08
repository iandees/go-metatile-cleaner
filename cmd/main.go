package main

import (
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/iandees/go-metatile-cleaner/tile"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const maxDeleteKeys = 500

func resetDeleteObjectsInput(bucketName string) *s3.DeleteObjectsInput {
	return &s3.DeleteObjectsInput{
		Bucket: aws.String(bucketName),
		Delete: &s3.Delete{},
	}
}

func sendDeleteObjects(svc *s3.S3, input *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
	result, err := svc.DeleteObjects(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				log.Fatalf("Couldn't delete objects: %+v", aerr.Error())
			}
		} else {
			log.Fatalf("Couldn't delete objects: %+v", err.Error())
		}
		return nil, err
	}

	return result, nil
}

func computeKey(buildID string, t *tile.Tile) string {
	key := fmt.Sprintf("%d/%d/%d.zip", t.Z, t.X, t.Y)
	hash := md5.Sum([]byte(key))
	hashPrefix := hex.EncodeToString(hash[:])[:5]
	return hashPrefix + "/" + buildID + "/" + key
}

func main() {
	buildID := flag.String("build-id", "", "The build ID to delete")
	bucketName := flag.String("bucket", "", "The name of the S3 bucket to delete metatiles from")
	concurrency := flag.Int("concurrency", 32, "The number of goroutines to use when deleting metatiles")
	maxZoom := flag.Uint("max-zoom", 13, "The maximum zoom to use when deleting metatiles")
	flag.Parse()

	if *buildID == "" {
		log.Fatalf("Specify build-id")
	}

	if *bucketName == "" {
		log.Fatalf("Specify bucket")
	}

	sess, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		log.Fatalf("Couldn't create AWS session: %+v", err)
	}

	svc := s3.New(sess)

	tileChan := make(chan *tile.Tile, 10000)

	sourceWaitGroup := &sync.WaitGroup{}
	sourceWaitGroup.Add(1)
	go func() {
		defer sourceWaitGroup.Done()
		for z := uint(0); z <= *maxZoom; z++ {
			tile.GenerateTiles(&tile.GenerateTilesOptions{
				Bounds: &tile.LngLatBbox{-180.0, -90.0, 180.0, 90.0},
				Zooms:  []uint{z},
				ConsumerFunc: func(tile *tile.Tile) {
					tileChan <- tile
				},
				InvertedY: false,
			})
		}
	}()
	log.Printf("Started tiles generator source")

	var deletes, errors uint64
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		for range ticker.C {
			log.Printf("Deleted %d objects (%d errors)", atomic.LoadUint64(&deletes), atomic.LoadUint64(&errors))
		}
	}()

	sinkWaitGroup := &sync.WaitGroup{}
	sinkWaitGroup.Add(*concurrency)
	log.Printf("Starting %d delete workers", *concurrency)
	for j := 0; j < *concurrency; j++ {
		go func() {
			defer sinkWaitGroup.Done()

			var input *s3.DeleteObjectsInput

			for t := range tileChan {
				if input == nil {
					input = resetDeleteObjectsInput(*bucketName)
				}

				key := computeKey(*buildID, t)

				input.Delete.Objects = append(input.Delete.Objects, &s3.ObjectIdentifier{
					Key: aws.String(key),
				})

				if len(input.Delete.Objects) == maxDeleteKeys {
					result, err := sendDeleteObjects(svc, input)
					if err != nil {
						log.Fatalf("Couldn't delete objects: %+v", err)
					}

					input = nil

					if len(result.Errors) > 10 {
						log.Printf("Sample error: %+v", result.Errors[0])
					}

					atomic.AddUint64(&deletes, uint64(len(result.Deleted)))
					atomic.AddUint64(&errors, uint64(len(result.Errors)))
				}
			}

			if input != nil && len(input.Delete.Objects) > 0 {
				result, err := sendDeleteObjects(svc, input)
				if err != nil {
					log.Fatalf("Couldn't delete objects: %+v", err)
				}

				atomic.AddUint64(&deletes, uint64(len(result.Deleted)))
				atomic.AddUint64(&errors, uint64(len(result.Errors)))
			}
		}()
	}

	sourceWaitGroup.Wait()
	close(tileChan)
	sinkWaitGroup.Wait()
	ticker.Stop()

	log.Printf("Done. Deleted %d metatiles with %d errors.", atomic.LoadUint64(&deletes), atomic.LoadUint64(&errors))
}
