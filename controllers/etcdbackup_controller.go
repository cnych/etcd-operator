/*
Copyright 2020 cnych.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"fmt"
	"html/template"
	"strings"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	etcdv1alpha1 "github.com/cnych/etcd-operator/api/v1alpha1"
)

// EtcdBackupReconciler reconciles a EtcdBackup object
type EtcdBackupReconciler struct {
	client.Client
	Log         logr.Logger
	Scheme      *runtime.Scheme
	Recorder    record.EventRecorder
	BackupImage string
}

type backupState struct {
	backup  *etcdv1alpha1.EtcdBackup
	actual  *backupStateContainer // 状态!=status，job pod 在执行
	desired *backupStateContainer // 期望的状态
}

type backupStateContainer struct {
	pod *corev1.Pod // backup.name namespace
}

// 获取真实的状态
func (r *EtcdBackupReconciler) setStateActual(ctx context.Context, state *backupState) error {
	var actual backupStateContainer
	key := client.ObjectKey{
		Name:      state.backup.Name,
		Namespace: state.backup.Namespace,
	}
	// 获取对应的 Pod
	actual.pod = &corev1.Pod{}
	if err := r.Get(ctx, key, actual.pod); err != nil {
		if client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("gettting pod error: %s", err)
		}
		actual.pod = nil
	}

	// 填充当前真实的状态
	state.actual = &actual
	return nil
}

// 获取期望的状态
func (r *EtcdBackupReconciler) setStateDesired(state *backupState) error {
	var desired backupStateContainer
	// 根据 EtcdBackup 创建一个用于备份 etcd 的Pod
	pod, err := podForBackup(state.backup, r.BackupImage)
	if err != nil {
		return err
	}

	// 配置 controller references
	if err := controllerutil.SetControllerReference(state.backup, pod, r.Scheme); err != nil {
		return fmt.Errorf("setting controller reference error: %s", err)
	}
	desired.pod = pod

	// 获取到期望的对象
	state.desired = &desired
	return nil
}

// 获取当前应用的整个状态
func (r *EtcdBackupReconciler) getState(ctx context.Context, req ctrl.Request) (*backupState, error) {
	var state backupState

	// 获取 EtcdBackup 对象
	state.backup = &etcdv1alpha1.EtcdBackup{}
	if err := r.Get(ctx, req.NamespacedName, state.backup); err != nil {
		if client.IgnoreNotFound(err) != nil {
			return nil, fmt.Errorf("getting backup object error: %s", err)
		}
		// 被删除了直接忽略
		state.backup = nil
		return &state, nil
	}

	// 获得了 EtcdBackup 对象

	// 获取当前真实的状态
	if err := r.setStateActual(ctx, &state); err != nil {
		return nil, fmt.Errorf("setting actual state error: %s", err)
	}

	// 获取期望的状态
	if err := r.setStateDesired(&state); err != nil {
		return nil, fmt.Errorf("setting desired state error: %s", err)
	}

	return &state, nil
}

func podForBackup(backup *etcdv1alpha1.EtcdBackup, image string) (*corev1.Pod, error) {
	var secretRef *corev1.SecretEnvSource
	var backupEndpoint, backupURL string
	// TODO，validate yaml
	if backup.Spec.StorageType == etcdv1alpha1.BackupStorageTypeS3 {
		backupEndpoint = backup.Spec.S3.Endpoint
		// format：s3://my-bucket/my-dir/my-object.db
		// s3://my-bucket/{{ .Namespace }}/{{ .Name }}/{{ .CreationTimestamp }}/snapshot.db
		// 备份的目标地址支持go-template
		tmpl, err := template.New("template").Parse(backup.Spec.S3.Path)
		if err != nil {
			return nil, err
		}
		// 解析成备份的地址
		var objectURL strings.Builder
		if err := tmpl.Execute(&objectURL, backup); err != nil {
			return nil, err
		}
		backupURL = fmt.Sprintf("%s://%s", backup.Spec.StorageType, objectURL.String())
		secretRef = &corev1.SecretEnvSource{
			LocalObjectReference: corev1.LocalObjectReference{
				Name: backup.Spec.S3.Secret,
			},
		}
	} else { // oss
		//backupURL = ?
		//backupEndpoint = ?
		secretRef = &corev1.SecretEnvSource{
			LocalObjectReference: corev1.LocalObjectReference{
				Name: backup.Spec.OSS.Secret,
			},
		}
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      backup.Name,
			Namespace: backup.Namespace,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "etcd-backup",
					Image: image,
					Args: []string{
						"--etcd-url", backup.Spec.EtcdUrl,
						"--backup-url", backupURL,
					},
					Env: []corev1.EnvVar{
						{
							Name:  "ENDPOINT",
							Value: backupEndpoint,
						},
					},
					EnvFrom: []corev1.EnvFromSource{
						{
							SecretRef: secretRef,
						},
					},
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("100m"),
							corev1.ResourceMemory: resource.MustParse("100Mi"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("100m"),
							corev1.ResourceMemory: resource.MustParse("100Mi"),
						},
					},
				},
			},
			RestartPolicy: corev1.RestartPolicyNever,
		},
	}, nil
}

// +kubebuilder:rbac:groups=etcd.ydzs.io,resources=etcdbackups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=etcd.ydzs.io,resources=etcdbackups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods/status,verbs=get;update;patch

func (r *EtcdBackupReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	ctx := context.Background()
	log := r.Log.WithValues("etcdbackup", req.NamespacedName)

	// 获取backupState
	state, err := r.getState(ctx, req)
	if err != nil {
		return ctrl.Result{}, err
	}

	// 根据状态来判断下一步要执行的动作
	var action Action

	// 开始判断状态
	switch {
	case state.backup == nil: // 被删除了
		log.Info("Backup Object not found")
	case !state.backup.DeletionTimestamp.IsZero(): // 被标记为删除了
		log.Info("Backup Object has been deleted")
	case state.backup.Status.Phase == "": // 要开始备份了，先标记状态为备份中
		log.Info("Backup starting...")
		newBackup := state.backup.DeepCopy()
		newBackup.Status.Phase = etcdv1alpha1.EtcdBackupPhaseBackingUp // 更改成备份中...
		action = &PatchStatus{client: r.Client, original: state.backup, new: newBackup}
	case state.backup.Status.Phase == etcdv1alpha1.EtcdBackupPhaseFailed: // 失败了
		log.Info("Backup has failed. Ignoring...")
	case state.backup.Status.Phase == etcdv1alpha1.EtcdBackupPhaseCompleted: // 完成了
		log.Info("Backup has completed. Ignoring...")
	case state.actual.pod == nil: // 当前还没有执行任务的Pod
		log.Info("Backup Pod does not exists. Creating...")
		action = &CreateObject{client: r.Client, obj: state.desired.pod}
		r.Recorder.Event(state.backup, corev1.EventTypeNormal, EventReasonSuccessfulCreate,
			fmt.Sprintf("Create Pod: %s", state.desired.pod.Name))
	case state.actual.pod.Status.Phase == corev1.PodFailed: // Pod执行失败
		log.Info("Backup Pod failed.")
		newBackup := state.backup.DeepCopy()
		newBackup.Status.Phase = etcdv1alpha1.EtcdBackupPhaseFailed // 更改成备份失败
		action = &PatchStatus{client: r.Client, original: state.backup, new: newBackup}
		r.Recorder.Event(state.backup, corev1.EventTypeWarning, EventReasonBackupFailed,
			"Backup failed. See backup pod for detail information.")
	case state.actual.pod.Status.Phase == corev1.PodSucceeded: // Pod 执行成功
		log.Info("Backup Pod success.")
		newBackup := state.backup.DeepCopy()
		newBackup.Status.Phase = etcdv1alpha1.EtcdBackupPhaseCompleted // 更改成备份成功
		action = &PatchStatus{client: r.Client, original: state.backup, new: newBackup}
		r.Recorder.Event(state.backup, corev1.EventTypeNormal, EventReasonBackupSucceeded,
			"Backup completed successfully")
	}

	if action != nil {
		if err := action.Execute(ctx); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *EtcdBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&etcdv1alpha1.EtcdBackup{}).
		Owns(&corev1.Pod{}).
		Complete(r)
}
