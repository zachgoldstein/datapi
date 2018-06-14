package storage

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	log "github.com/sirupsen/logrus"

	"github.com/zachgoldstein/datatoapi/models"
)

const ReqTimeout = time.Duration(10000000000) // 10 seconds

type AWSFS struct {
	FSLocation string
	FilePaths  []string
	awsClient  *s3.S3
}

func NewAWSFS() *AWSFS {
	return &AWSFS{}
}

func (awsfs *AWSFS) getSession() *session.Session {
	// Initial credentials loaded from SDK's default credential chain. Such as
	// the environment, shared credentials (~/.aws/credentials), or EC2 Instance
	// Role. These credentials will be used to to make the STS Assume Role API.
	return session.Must(session.NewSession(&aws.Config{
		Region: aws.String("us-east-1")}))
}

func (awsfs *AWSFS) Start(path string, credentials map[string]interface{}) error {
	sess := awsfs.getSession()
	awsfs.awsClient = s3.New(sess)
	awsfs.FSLocation = path
	return awsfs.TestData()
}

// example path: https://s3.amazonaws.com/datatoapi/data.jsonfiles
func getPathDetails(awsURL string) (bucket, key string) {
	awsURL = strings.Replace(awsURL, "https://s3.amazonaws.com/", "", -1)
	urlParts := strings.SplitN(awsURL, "/", 2)
	return urlParts[0], urlParts[1]
}

func (awsfs *AWSFS) TestData() error {
	bucket, _ := getPathDetails(awsfs.FSLocation)
	log.Info("Testing data access")
	objs, err := ListObjects(bucket, awsfs.awsClient)
	if err != nil {
		log.WithError(err).Error("Could not access data")
		return err
	}

	log.WithFields(log.Fields{
		"numObjects": len(objs),
	}).Info("Data is accessible")
	return nil
}

func ListObjects(bucket string, client *s3.S3) ([]*s3.Object, error) {
	ctx := context.Background()
	var cancelFn func()
	ctx, cancelFn = context.WithTimeout(ctx, ReqTimeout)
	defer cancelFn()
	result, err := client.ListObjectsWithContext(ctx, &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		log.WithError(err).Error("Could not list objects")
		return nil, err
	}
	return result.Contents, nil
}

// DownloadObjectsIntoChanIterator implements the BatchDownloadIterator interface and allows for batched
// download of objects, sending downloaded data to an interface chan
type DownloadObjectsIntoChanIterator struct {
	interfaceChan chan<- interface{}
	Objects       []s3manager.BatchDownloadObject
	index         int
	inc           bool
}

// Next will increment the default iterator's index and and ensure that there
// is another object to iterator to. It will also attempt to scan the last loaded object into the chan
func (batcher *DownloadObjectsIntoChanIterator) Next() bool {
	lastObject := batcher.Objects[batcher.index]
	writer := lastObject.Writer.(*aws.WriteAtBuffer)
	writtenBytes := writer.Bytes()
	if len(writtenBytes) > 0 {
		scanner := bufio.NewScanner(bytes.NewReader(writtenBytes))
		err := WriteJSONToInterfaceChan(scanner, batcher.interfaceChan)
		if err != nil {
			log.WithError(err).Error("Could not write to channel")
			batcher.inc = false
			return false
		}
	}
	if batcher.inc {
		batcher.index++
	} else {
		batcher.inc = true
	}
	return batcher.index < len(batcher.Objects)
}

// DownloadObject will return the BatchDownloadObject at the current batched index.
func (batcher *DownloadObjectsIntoChanIterator) DownloadObject() s3manager.BatchDownloadObject {
	object := batcher.Objects[batcher.index]
	return object
}

// Err will return an error. Since this is just used to satisfy the BatchDeleteIterator interface
// this will only return nil.
func (batcher *DownloadObjectsIntoChanIterator) Err() error {
	return nil
}

func (awsfs *AWSFS) ScanData(interfaceChan chan<- interface{}) error {
	bucket, _ := getPathDetails(awsfs.FSLocation)
	objs, err := ListObjects(bucket, awsfs.awsClient)
	if err != nil {
		return err
	}
	downloadObjs := []s3manager.BatchDownloadObject{}
	for _, obj := range objs {
		newIOWriter := aws.NewWriteAtBuffer([]byte{})
		downloadObjs = append(downloadObjs, s3manager.BatchDownloadObject{
			Object: &s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    obj.Key,
			},
			Writer: newIOWriter,
		})
	}

	sess := awsfs.getSession()
	svc := s3manager.NewDownloader(sess)
	iter := &DownloadObjectsIntoChanIterator{
		Objects:       downloadObjs,
		interfaceChan: interfaceChan,
	}
	if err := svc.DownloadWithIterator(aws.BackgroundContext(), iter); err != nil {
		log.WithError(err).Error("Could not download data")
		return err
	}
	close(iter.interfaceChan)

	return nil
}

// DownloadObjectsIntoDataChansIterator implements the BatchDownloadIterator interface and allows for batched
// download of objects, sending downloaded data to an interface chan
type DownloadObjectsIntoDataChansIterator struct {
	dataChan  chan<- models.IndexData
	blockChan chan<- models.DataBlock
	Objects   []s3manager.BatchDownloadObject
	index     int
	inc       bool
}

// Next will increment the default iterator's index and and ensure that there
// is another object to iterator to. It will also attempt to scan the last loaded object into the chans
func (batcher *DownloadObjectsIntoDataChansIterator) Next() bool {
	lastObject := batcher.Objects[batcher.index]
	writer := lastObject.Writer.(*aws.WriteAtBuffer)
	writtenBytes := writer.Bytes()
	if len(writtenBytes) > 0 {
		scanner := bufio.NewScanner(bytes.NewReader(writtenBytes))
		err := WriteJSONToDataChans(*lastObject.Object.Key, scanner, batcher.dataChan, batcher.blockChan)
		if err != nil {
			log.WithError(err).Error("Could not write to data chans")
			batcher.inc = false
			return false
		}
	}

	if batcher.inc {
		batcher.index++
	} else {
		batcher.inc = true
	}
	return batcher.index < len(batcher.Objects)
}

// DownloadObject will return the BatchDownloadObject at the current batched index.
func (batcher *DownloadObjectsIntoDataChansIterator) DownloadObject() s3manager.BatchDownloadObject {
	object := batcher.Objects[batcher.index]
	return object
}

// Err will return an error. Since this is just used to satisfy the BatchDeleteIterator interface
// this will only return nil.
func (batcher *DownloadObjectsIntoDataChansIterator) Err() error {
	return nil
}

func (awsfs *AWSFS) ScanDataBlocks(dataChan chan<- models.IndexData, blockChan chan<- models.DataBlock) error {
	log.Info("Scanning aws data into channels")
	bucket, _ := getPathDetails(awsfs.FSLocation)
	objs, err := ListObjects(bucket, awsfs.awsClient)
	if err != nil {
		log.WithError(err).Error("Could not list objects for scanning")
		return err
	}
	downloadObjs := []s3manager.BatchDownloadObject{}
	for _, obj := range objs {
		newIOWriter := aws.NewWriteAtBuffer([]byte{})
		downloadObjs = append(downloadObjs, s3manager.BatchDownloadObject{
			Object: &s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    obj.Key,
			},
			Writer: newIOWriter,
		})
	}

	sess := awsfs.getSession()
	svc := s3manager.NewDownloader(sess)
	iter := &DownloadObjectsIntoDataChansIterator{
		Objects:   downloadObjs,
		dataChan:  dataChan,
		blockChan: blockChan,
	}
	if err := svc.DownloadWithIterator(aws.BackgroundContext(), iter); err != nil {
		log.WithError(err).Error("Could not download data")
		return err
	}
	close(iter.blockChan)
	close(iter.dataChan)
	log.Info("Finished scanning aws data into channels")
	return nil
}

func (awsfs *AWSFS) RetrieveDataBlockBytes(block *models.DataBlock) ([]byte, error) {
	bucket, _ := getPathDetails(awsfs.FSLocation)
	return GetObjectBytes(bucket, block.File.Address, awsfs.awsClient, block.Start, block.End)
}

func GetObjectBytes(bucket string, key string, client *s3.S3, start, end int64) ([]byte, error) {
	ctx := context.Background()
	var cancelFn func()
	ctx, cancelFn = context.WithTimeout(ctx, ReqTimeout)
	// Ensure the context is canceled to prevent leaking.
	defer cancelFn()
	result, err := client.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  aws.String(fmt.Sprintf("bytes=%v-%v", start, end)),
	})
	if err != nil {
		log.WithError(err).Error("Could not access data")
		return nil, err
	}

	// Make sure to close the body when done with it for S3 GetObject APIs or
	// will leak connections.
	defer result.Body.Close()

	bytes, err := ioutil.ReadAll(result.Body)
	if err != nil {
		log.WithError(err).Error("Could read body data")
		return nil, err
	}
	return bytes, nil
}
