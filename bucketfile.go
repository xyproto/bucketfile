// Package bucketfile provides an easy way to:
// * upload a file to a bucket
// * get a file from a bucket
// * list files in a bucket
package bucketfile

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// Upload takes an io.Reader, bucket name and object name
// and uploads the file to the bucket. It has a 50 second timeout.
func Upload(file io.Reader, bucket, object string) error {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("storage.NewClient: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()

	// Upload an object with storage.Writer.
	wc := client.Bucket(bucket).Object(object).NewWriter(ctx)
	if _, err = io.Copy(wc, file); err != nil {
		return fmt.Errorf("io.Copy: %v", err)
	}
	if err := wc.Close(); err != nil {
		return fmt.Errorf("Writer.Close: %v", err)
	}

	return nil
}

// Get takes a bucket name and an object name and
// returns the file data as a slice of bytes.
// It has a 50 second timeout.
func Get(bucket, object string) ([]byte, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("storage.NewClient: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()

	rc, err := client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("Object(%q).NewReader: %v", object, err)
	}
	defer rc.Close()

	data, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("ioutil.ReadAll: %v", err)
	}

	return data, nil
}

// List lists objects within the specified bucket
// It has a 10 second timeout.
func List(bucket string) ([]string, error) {
	var fileNames []string

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fileNames, fmt.Errorf("storage.NewClient: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	it := client.Bucket(bucket).Objects(ctx, nil)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fileNames, fmt.Errorf("Bucket(%q).Objects: %v", bucket, err)
		}
		fileNames = append(fileNames, attrs.Name)
	}

	return fileNames, nil
}
