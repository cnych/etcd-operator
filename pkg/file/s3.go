package file

import (
	"context"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type s3Uploader struct {
	Endpoint        string
	AccessKeyId     string
	SecretAccessKey string
}

func NewS3Uploader(Endpoint, AK, SK string) *s3Uploader {
	return &s3Uploader{
		Endpoint:        Endpoint,
		AccessKeyId:     AK,
		SecretAccessKey: SK,
	}
}

func (su *s3Uploader) InitClient() (*minio.Client, error) {
	return minio.New(su.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(su.AccessKeyId, su.SecretAccessKey, ""),
		Secure: true,
	})
}

func (su *s3Uploader) Upload(ctx context.Context, bucketName, objectName, filePath string) (int64, error) {
	minioClient, err := su.InitClient()
	if err != nil {
		return 0, err
	}
	uploadInfo, err := minioClient.FPutObject(ctx, bucketName, objectName, filePath, minio.PutObjectOptions{})
	if err != nil {
		return 0, err
	}
	return uploadInfo.Size, nil
}
