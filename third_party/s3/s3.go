package s3

import (
	"bytes"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/spf13/viper"
	"io"
	"io/ioutil"
	"log"
	"os"
	"time"
)

/**
  @author: chenxi@cpgroup.cn
  @date:2022/4/28
  @description:
**/

var (
	accessKey = viper.GetString("s3.accessKey")
	secretKey = viper.GetString("s3.secretKey")
	bucket    = viper.GetString("s3.bucket")
	region    = viper.GetString("s3.region")
	endpoint  = viper.GetString("s3.endpoint")

	PartSize = 5 * 1024 * 1024
	Retries  = 2
)

//GetObjectUrl 获取对象URL

func GetObjectUrl(key string) (objectUrl string) {
	//objectUrl = fmt.Sprintf("https://%s.s3.%s.amazonaws.com/%s/%s%s", bucket, region, fileType, key, ext)
	objectUrl = fmt.Sprintf("https://blog-static.cpgroup.top/%s", key)
	return
}

//获取session
func getSession() *session.Session {
	sess, err := session.NewSession(&aws.Config{
		Region:           aws.String(region),
		Endpoint:         aws.String(endpoint),
		Credentials:      credentials.NewStaticCredentials(accessKey, secretKey, ""),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(false),
	})
	if err != nil {
		panic(err)
	}
	return sess
}

//获取svc
func getSvc() *s3.S3 {
	sess := getSession()
	return s3.New(sess)
}

//创建bucket
func createBucket(bucketName string) {
	svc := getSvc()
	input := &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	}
	_, err := svc.CreateBucket(input)
	if err != nil {
		panic(err)
	}
}

//获取bucket_list
func getBucketList() []*s3.Bucket {
	svc := getSvc()
	result, err := svc.ListBuckets(nil)
	if err != nil {
		panic(err)
	}
	return result.Buckets
}

//获取bucket文件列表
func getBucketFileList(bucket string) []*s3.Object {
	svc := getSvc()
	result, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		panic(err)
	}
	return result.Contents
}

//获取bucket文件内容
func getBucketFileContent(bucket string, key string) []byte {
	svc := getSvc()
	result, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		panic(err)
	}

	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {

		}
	}(result.Body)

	content, err := ioutil.ReadAll(result.Body)
	if err != nil {
		panic(err)
	}
	return content
}

//UploadFile 上传文件
func UploadFile(key string, file []byte) error {
	_, err := s3manager.NewUploader(getSession()).Upload(&s3manager.UploadInput{
		ACL:    aws.String(s3.ObjectCannedACLPublicRead),
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(file),
	})

	if err != nil {
		return err
	}
	return nil
}

// Upload the fileBytes bytearray a MultiPart upload
func Upload(resp *s3.CreateMultipartUploadOutput, fileBytes []byte, partNum int) (completedPart *s3.CompletedPart, err error) {
	var try int
	for try <= Retries {
		uploadResp, err := getSvc().UploadPart(&s3.UploadPartInput{
			Body:          bytes.NewReader(fileBytes),
			Bucket:        resp.Bucket,
			Key:           resp.Key,
			PartNumber:    aws.Int64(int64(partNum)),
			UploadId:      resp.UploadId,
			ContentLength: aws.Int64(int64(len(fileBytes))),
		})

		// Upload failed
		if err != nil {
			log.Println("UploadPart failed:", err)
			// Max retries reached! Quitting
			if try == Retries {
				return nil, err
			} else {
				// Retrying
				try++
			}
		} else {
			// Upload is done!
			return &s3.CompletedPart{
				ETag:       uploadResp.ETag,
				PartNumber: aws.Int64(int64(partNum)),
			}, nil
		}
	}

	return nil, nil
}

//MultipartUpload 断点续传文件
func MultipartUpload(key string, buf []byte) error {
	svc := getSvc()

	// Create MultipartUpload object
	expiryDate := time.Now().AddDate(0, 0, 1)
	createdResp, err := svc.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		ACL:     aws.String(s3.ObjectCannedACLPublicRead),
		Bucket:  aws.String(bucket),
		Key:     aws.String(key),
		Expires: &expiryDate,
	})

	if err != nil {
		log.Println("创建MultipartUpload失败：", err)
		return err
	}

	var start, currentSize int
	var remaining = len(buf)
	var partNum = 1
	var completedParts []*s3.CompletedPart

	for start = 0; remaining != 0; start += PartSize {
		if remaining < PartSize {
			currentSize = remaining
		} else {
			currentSize = PartSize
		}

		completed, err := Upload(createdResp, buf[start:start+currentSize], partNum)

		// If upload function failed (meaning it retried acoording to RETRIES)
		if err != nil {
			_, err = svc.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
				Bucket:   createdResp.Bucket,
				Key:      createdResp.Key,
				UploadId: createdResp.UploadId,
			})
			if err != nil {
				log.Println("AbortMultipartUpload失败：", err)
				return err
			}
		}

		// Detract the current part size from remaining
		remaining -= currentSize
		log.Printf("Part %v complete, %v btyes remaining\n", partNum, remaining)

		// Add the completed part to our list
		completedParts = append(completedParts, completed)
		partNum++

	}

	// All the parts are uploaded, completing the upload
	resp, err := svc.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket:   createdResp.Bucket,
		Key:      createdResp.Key,
		UploadId: createdResp.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		log.Println("CompleteMultipartUpload失败：", err)
		return err
	}

	log.Println("CompleteMultipartUpload成功：", resp.String())
	return nil
}

//DownloadFile 文件下载
func DownloadFile(key string, file *os.File) (err error) {

	defer func(file *os.File) {
		err = file.Close()
	}(file)

	_, err = s3manager.NewDownloader(getSession()).Download(file, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	return err
}

// DeleteFile 删除文件
func DeleteFile(key string) error {
	svc := getSvc()
	_, err := svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return err
	}
	return nil
}
