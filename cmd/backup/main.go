package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/cnych/etcd-operator/api/v1alpha1"
	"github.com/cnych/etcd-operator/pkg/file"
	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/snapshot"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func logErr(log logr.Logger, err error, message string) error {
	log.Error(err, message)
	return fmt.Errorf("%s: %s", message, err)
}

func main() {

	var (
		backupTempDir      string
		etcdURL            string
		backupURL          string
		dialTimeoutSeconds int64
		timeoutSeconds     int64
	)

	flag.StringVar(&backupTempDir, "backup-tmp-dir", os.TempDir(), "The directory to temp place backup etcd cluster.")
	flag.StringVar(&etcdURL, "etcd-url", "", "URL for backup etcd.")
	flag.StringVar(&backupURL, "backup-url", "", "URL for backup etcd object storage.")
	flag.Int64Var(&dialTimeoutSeconds, "dial-timeout-seconds", 5, "Timeout for dialing the Etcd.")
	flag.Int64Var(&timeoutSeconds, "timeout-seconds", 60, "Timeout for Backup the Etcd.")
	flag.Parse() // 一定要加上

	zapLogger := zap.NewRaw(zap.UseDevMode(true))
	ctrl.SetLogger(zapr.NewLogger(zapLogger))

	ctx, ctxCancel := context.WithTimeout(context.Background(), time.Second*time.Duration(timeoutSeconds))
	defer ctxCancel()

	log := ctrl.Log.WithName("backup")

	storageType, bucketName, objectName, err := file.ParseBackupURL(backupURL)
	if err != nil {
		panic(logErr(log, err, "failed to parse backup url"))
	}

	log.Info("Connecting to Etcd and getting Snapshot data")

	// 定义一个本地的数据目录
	localPath := filepath.Join(backupTempDir, "snapshot.db")
	// 创建etcd snapshot manager
	etcdManager := snapshot.NewV3(zapLogger)
	// 保存etcd snapshot数据到localPath
	err = etcdManager.Save(ctx, clientv3.Config{
		Endpoints:   []string{etcdURL},
		DialTimeout: time.Second * time.Duration(dialTimeoutSeconds),
	}, localPath)
	if err != nil {
		panic(logErr(log, err, "failed to get etcd snapshot data"))
	}

	// 根据storageType来决定上传数据到什么地方去
	switch storageType {
	case string(v1alpha1.BackupStorageTypeS3): // s3
		log.Info("Uploading snapshot...")
		size, err := handleS3(ctx, bucketName, objectName, localPath)
		if err != nil {
			panic(logErr(log, err, "failed to upload backup etcd"))
		}
		log.WithValues("upload-size", size).Info("Backup completed")
	case string(v1alpha1.BackupStorageTypeOSS): // oss（todo）
	default:
		panic(logErr(log, fmt.Errorf("storage type error"), fmt.Sprintf("unkown storage type:%v", storageType)))
	}

}

func handleS3(ctx context.Context, bucketName, objectName, localPath string) (int64, error) {
	// 数据保存到本地成功
	// 接下来就上传
	endpoint := os.Getenv("ENDPOINT")
	accessKeyID := os.Getenv("MINIO_ACCESS_KEY")
	secretAccessKey := os.Getenv("MINIO_SECRET_KEY")
	//endpoint := "play.min.io"
	//accessKeyID := "Q3AM3UQ867SPQQA43P2F"
	//secretAccessKey := "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
	s3Uploader := file.NewS3Uploader(endpoint, accessKeyID, secretAccessKey)
	return s3Uploader.Upload(ctx, bucketName, objectName, localPath)
}
