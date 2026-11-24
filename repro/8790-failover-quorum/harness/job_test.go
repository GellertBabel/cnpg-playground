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
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestWriterJob(t *testing.T) {
	cfg := attemptConfig{
		namespace: "repro-8790", cluster: "repro-8790", image: "registry.dev:5000/cnpg-repro-writer:latest",
		payload: 20000, writers: 4, payloadMode: "fixed", commitRate: 2.5,
	}
	job := writerJob(writerJobName(cfg.cluster), cfg, "repro-8790-2")
	if job.Name != "repro-8790-writer" || job.Namespace != "repro-8790" {
		t.Errorf("job %s/%s", job.Namespace, job.Name)
	}
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 {
		t.Errorf("backoffLimit %v, want 0", job.Spec.BackoffLimit)
	}
	pod := job.Spec.Template.Spec
	if pod.RestartPolicy != corev1.RestartPolicyNever || len(pod.Containers) != 1 {
		t.Fatalf("restartPolicy %s, %d containers", pod.RestartPolicy, len(pod.Containers))
	}
	c := pod.Containers[0]
	if c.Image != cfg.image || !reflect.DeepEqual(c.Command, []string{"/harness", "writer"}) {
		t.Errorf("image %s command %v", c.Image, c.Command)
	}
	env := map[string]corev1.EnvVar{}
	for _, e := range c.Env {
		env[e.Name] = e
	}
	for name, want := range map[string]string{
		"REPLICA": "repro-8790-2", "PAYLOAD_BYTES": "20000", "WRITERS": "4",
		"PAYLOAD_MODE": "fixed", "CHECKPOINT_EVERY": "", "COMMIT_RATE": "2.5",
	} {
		if e, ok := env[name]; !ok || e.Value != want {
			t.Errorf("env %s = %q (set %t), want %q", name, e.Value, ok, want)
		}
	}
	uri, ok := env["URI"]
	if !ok || uri.ValueFrom == nil || uri.ValueFrom.SecretKeyRef == nil {
		t.Fatalf("URI not from a secret: %+v", uri)
	}
	if ref := uri.ValueFrom.SecretKeyRef; ref.Name != "repro-8790-app" || ref.Key != "uri" {
		t.Errorf("URI from %s/%s, want repro-8790-app/uri", ref.Name, ref.Key)
	}
	if len(c.Env) != 7 {
		t.Errorf("%d env vars, want 7", len(c.Env))
	}
}

func TestAckStore(t *testing.T) {
	s := &ackStore{}
	s.add(Ack{ID: 1, Named: true, Behind: true})
	s.add(Ack{ID: 2, Named: true})
	s.add(Ack{ID: 3, Behind: true})
	snap := s.snapshot()
	snap[0].ID = 99
	if s.snapshot()[0].ID != 1 {
		t.Error("snapshot must be a copy")
	}
	if total, v := s.countNamedBehind(); total != 3 || v != 1 {
		t.Errorf("countNamedBehind = %d, %d; want 3, 1", total, v)
	}
}

func TestReadAcksSkipsNoise(t *testing.T) {
	s := &ackStore{}
	readAcks(strings.NewReader(
		"NOTICE: table \"marker\" does not exist, skipping\n"+
			`{"id":1,"named":true,"behind":false}`+"\n"+
			`{"id":2,"named":true,"behind":true}`+"\n"), s)
	acks := s.snapshot()
	if len(acks) != 2 || acks[0].ID != 1 || acks[1].ID != 2 {
		t.Errorf("read %+v, want ids 1 and 2", acks)
	}
}

func TestWaitJobPodSkipsTerminatingPods(t *testing.T) {
	sel := map[string]string{"batch.kubernetes.io/controller-uid": "u1"}
	now := metav1.Now()
	k := &kubeClient{typed: fake.NewClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "old", Namespace: "ns", Labels: sel, DeletionTimestamp: &now,
				Finalizers: []string{"test"}},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "pending", Namespace: "ns", Labels: sel},
			Status:     corev1.PodStatus{Phase: corev1.PodPending},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "new", Namespace: "ns", Labels: sel},
			Status:     corev1.PodStatus{Phase: corev1.PodRunning},
		},
	)}
	got, err := k.waitJobPod(context.Background(), "ns", "batch.kubernetes.io/controller-uid=u1", 5*time.Second)
	if err != nil || got != "new" {
		t.Errorf("got %q, %v; want new", got, err)
	}
}

func TestStopWriterDeletesJob(t *testing.T) {
	k := &kubeClient{typed: fake.NewClientset(&batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "repro-8790-writer", Namespace: "repro-8790"},
	})}
	ctx := context.Background()
	k.stopWriter(ctx, "repro-8790", "repro-8790")
	_, err := k.typed.BatchV1().Jobs("repro-8790").Get(ctx, "repro-8790-writer", metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		t.Errorf("job still there: %v", err)
	}
	k.stopWriter(ctx, "repro-8790", "repro-8790") // a missing Job is fine
}
