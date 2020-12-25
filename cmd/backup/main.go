package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

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
		dialTimeoutSeconds int64
		timeoutSeconds     int64
	)

	flag.StringVar(&backupTempDir, "backup-tmp-dir", os.TempDir(), "The directory to temp place backup etcd cluster.")
	flag.StringVar(&etcdURL, "etcd-url", "", "URL for backup etcd.")
	flag.Int64Var(&dialTimeoutSeconds, "dial-timeout-seconds", 5, "Timeout for dialing the Etcd.")
	flag.Int64Var(&timeoutSeconds, "timeout-seconds", 60, "Timeout for Backup the Etcd.")
	flag.Parse() // 一定要加上

	zapLogger := zap.NewRaw(zap.UseDevMode(true))
	ctrl.SetLogger(zapr.NewLogger(zapLogger))

	ctx, ctxCancel := context.WithTimeout(context.Background(), time.Second*time.Duration(timeoutSeconds))
	defer ctxCancel()

	log := ctrl.Log.WithName("backup").WithValues("etcd-url", etcdURL)
	log.Info("Connecting to Etcd and getting Snapshot data")

	// 定义一个本地的数据目录
	localPath := filepath.Join(backupTempDir, "snapshot.db")
	// 创建etcd snapshot manager
	etcdManager := snapshot.NewV3(zapLogger)
	// 保存etcd snapshot数据到localPath
	err := etcdManager.Save(ctx, clientv3.Config{
		Endpoints:   []string{etcdURL},
		DialTimeout: time.Second * time.Duration(dialTimeoutSeconds),
	}, localPath)
	if err != nil {
		panic(logErr(log, err, "failed to get etcd snapshot data"))
	}

	// 数据保存到本地成功
	// 接下来就上传
	// TODO，根据传递进来的参数判断初始化s3还是oss
	endpoint := "play.min.io"
	accessKeyID := "Q3AM3UQ867SPQQA43P2F"
	secretAccessKey := "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
	s3Uploader := file.NewS3Uploader(endpoint, accessKeyID, secretAccessKey)

	log.Info("Uploading snapshot...")
	size, err := s3Uploader.Upload(ctx, localPath)
	if err != nil {
		panic(logErr(log, err, "failed to upload backup etcd"))
	}
	log.WithValues("upload-size", size).Info("Backup completed")

}
