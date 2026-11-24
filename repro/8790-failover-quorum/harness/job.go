/*
Copyright © contributors to CloudNativePG, established as
CloudNativePG a Series of LF Projects, LLC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ackStore collects the writer's acknowledged commits as they stream in.
type ackStore struct {
	mu   sync.Mutex
	acks []Ack
}

func (s *ackStore) add(a Ack) {
	s.mu.Lock()
	s.acks = append(s.acks, a)
	s.mu.Unlock()
}

func (s *ackStore) snapshot() []Ack {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Ack, len(s.acks))
	copy(out, s.acks)
	return out
}

func (s *ackStore) countNamedBehind() (total, violations int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, a := range s.acks {
		total++
		if a.Named && a.Behind {
			violations++
		}
	}
	return
}

func writerJobName(cluster string) string { return cluster + "-writer" }

// startWriter creates the writer Job and streams its pod logs into the store
// until the context is cancelled. The Job runs this same binary in writer mode.
func (k *kubeClient) startWriter(ctx context.Context, cfg attemptConfig, replica string, store *ackStore) error {
	name := writerJobName(cfg.cluster)
	_ = k.typed.BatchV1().Jobs(cfg.namespace).Delete(ctx, name, metav1.DeleteOptions{
		PropagationPolicy: ptr(metav1.DeletePropagationBackground),
	})
	// Wait for any previous pod to go away so the label selector is unambiguous.
	time.Sleep(2 * time.Second)

	created, err := k.typed.BatchV1().Jobs(cfg.namespace).Create(ctx, writerJob(name, cfg, replica), metav1.CreateOptions{})
	if err != nil {
		return err
	}

	// Select by the new Job's UID: the previous attempt's pod can still be
	// terminating under the same job-name label, and streaming its log would
	// attribute the previous run's acks to this one.
	pod, err := k.waitJobPod(ctx, cfg.namespace, "batch.kubernetes.io/controller-uid="+string(created.UID), 120*time.Second)
	if err != nil {
		return err
	}
	go k.streamAcks(ctx, cfg.namespace, pod, store)
	return nil
}

func (k *kubeClient) stopWriter(ctx context.Context, ns, cluster string) {
	_ = k.typed.BatchV1().Jobs(ns).Delete(ctx, writerJobName(cluster), metav1.DeleteOptions{
		PropagationPolicy: ptr(metav1.DeletePropagationBackground),
	})
}

func (k *kubeClient) waitJobPod(ctx context.Context, ns, selector string, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		pods, err := k.typed.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{
			LabelSelector: selector,
		})
		if err == nil {
			for _, p := range pods.Items {
				if p.Status.Phase == corev1.PodRunning && p.DeletionTimestamp == nil {
					return p.Name, nil
				}
			}
		}
		time.Sleep(time.Second)
	}
	return "", fmt.Errorf("writer pod (%s) not running within %s", selector, timeout)
}

func (k *kubeClient) streamAcks(ctx context.Context, ns, pod string, store *ackStore) {
	req := k.typed.CoreV1().Pods(ns).GetLogs(pod, &corev1.PodLogOptions{Follow: true})
	stream, err := req.Stream(ctx)
	if err != nil {
		return
	}
	defer func() { _ = stream.Close() }()
	readAcks(stream, store)
}

// readAcks adds every acknowledgement line of the writer's output to the
// store and skips everything else.
func readAcks(r io.Reader, store *ackStore) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		if a, ok := parseAckLine(sc.Bytes()); ok {
			store.add(a)
		}
	}
}

// writerJob runs the harness image in writer mode against the cluster's app
// user, whose connection URI CloudNativePG keeps in the <cluster>-app secret.
func writerJob(name string, cfg attemptConfig, replica string) *batchv1.Job {
	backoff := int32(0)
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cfg.namespace},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoff,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:            "writer",
						Image:           cfg.image,
						ImagePullPolicy: corev1.PullIfNotPresent,
						Command:         []string{"/harness", "writer"},
						Env: []corev1.EnvVar{
							{Name: "REPLICA", Value: replica},
							{Name: "PAYLOAD_BYTES", Value: strconv.Itoa(cfg.payload)},
							{Name: "WRITERS", Value: strconv.Itoa(cfg.writers)},
							{Name: "PAYLOAD_MODE", Value: cfg.payloadMode},
							{Name: "CHECKPOINT_EVERY", Value: cfg.checkpointEvery},
							{Name: "COMMIT_RATE", Value: strconv.FormatFloat(cfg.commitRate, 'f', -1, 64)},
							{Name: "URI", ValueFrom: secretRef(cfg.cluster+"-app", "uri")},
						},
					}},
				},
			},
		},
	}
}

func secretRef(name, key string) *corev1.EnvVarSource {
	return &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: name},
		Key:                  key,
	}}
}

func ptr[T any](v T) *T { return &v }
