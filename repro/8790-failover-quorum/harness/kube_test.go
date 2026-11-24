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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
)

func node(name string, controlPlane bool) *corev1.Node {
	n := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{}}}
	if controlPlane {
		n.Labels["node-role.kubernetes.io/control-plane"] = ""
	}
	return n
}

// The default cluster of cloudnative-pg's hack/setup-cluster.sh (kind, NODES=3).
func TestWorkerNodeNames(t *testing.T) {
	k := &kubeClient{typed: fake.NewClientset(
		node("pg-operator-e2e-v1-37-0-control-plane", true),
		node("pg-operator-e2e-v1-37-0-worker", false),
		node("pg-operator-e2e-v1-37-0-worker2", false),
		node("pg-operator-e2e-v1-37-0-worker3", false),
	)}
	got, err := k.workerNodeNames(context.Background())
	want := []string{"pg-operator-e2e-v1-37-0-worker", "pg-operator-e2e-v1-37-0-worker2", "pg-operator-e2e-v1-37-0-worker3"}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, %v; want %v", got, err, want)
	}
}

func TestWorkerNodeNamesNone(t *testing.T) {
	k := &kubeClient{typed: fake.NewClientset(node("solo-control-plane", true))}
	if _, err := k.workerNodeNames(context.Background()); err == nil {
		t.Error("want an error without worker nodes")
	}
}

func cnpgObject(gvr schema.GroupVersionResource, kind, name string, fields map[string]any) *unstructured.Unstructured {
	obj := map[string]any{
		"apiVersion": gvr.Group + "/" + gvr.Version,
		"kind":       kind,
		"metadata":   map[string]any{"name": name, "namespace": "repro-8790"},
	}
	for k, v := range fields {
		obj[k] = v
	}
	return &unstructured.Unstructured{Object: obj}
}

func dynamicClient(objs ...runtime.Object) *kubeClient {
	lists := map[schema.GroupVersionResource]string{clusterGVR: "ClusterList", quorumGVR: "FailoverQuorumList"}
	return &kubeClient{dynamic: dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), lists, objs...)}
}

func TestCurrentPrimary(t *testing.T) {
	k := dynamicClient(cnpgObject(clusterGVR, "Cluster", "repro-8790",
		map[string]any{"status": map[string]any{"currentPrimary": "repro-8790-1"}}))
	ctx := context.Background()
	if p, err := k.currentPrimary(ctx, "repro-8790", "repro-8790"); err != nil || p != "repro-8790-1" {
		t.Errorf("got %q, %v", p, err)
	}
	if _, err := k.currentPrimary(ctx, "repro-8790", "missing"); err == nil {
		t.Error("want an error for a missing Cluster")
	}
}

func TestSetFenced(t *testing.T) {
	k := dynamicClient(cnpgObject(clusterGVR, "Cluster", "repro-8790", nil))
	ctx := context.Background()
	annotation := func() (string, bool) {
		c, err := k.dynamic.Resource(clusterGVR).Namespace("repro-8790").Get(ctx, "repro-8790", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		v, ok := c.GetAnnotations()["cnpg.io/fencedInstances"]
		return v, ok
	}
	if err := k.setFenced(ctx, "repro-8790", "repro-8790", "repro-8790-2"); err != nil {
		t.Fatal(err)
	}
	if v, ok := annotation(); !ok || v != `["repro-8790-2"]` {
		t.Errorf("fenced annotation %q (set %t)", v, ok)
	}
	if err := k.setFenced(ctx, "repro-8790", "repro-8790", ""); err != nil {
		t.Fatal(err)
	}
	if v, ok := annotation(); ok {
		t.Errorf("annotation still set: %q", v)
	}
}

func TestQuorumState(t *testing.T) {
	k := dynamicClient(cnpgObject(quorumGVR, "FailoverQuorum", "repro-8790", map[string]any{
		"status": map[string]any{"primary": "repro-8790-1", "standbyNames": []any{"repro-8790-2"}},
	}))
	cfg := attemptConfig{namespace: "repro-8790", cluster: "repro-8790"}
	got := k.quorumState(context.Background(), cfg)
	if !strings.HasPrefix(got, "FailoverQuorum: map[") || !strings.Contains(got, "standbyNames:[repro-8790-2]") {
		t.Errorf("got %q", got)
	}
	cfg.cluster = "missing"
	if got := k.quorumState(context.Background(), cfg); got != "FailoverQuorum: <none>" {
		t.Errorf("missing: got %q", got)
	}
}

func instancePod(name, role string, ready bool) *corev1.Pod {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "repro-8790", Labels: map[string]string{
			"cnpg.io/cluster": "repro-8790", "cnpg.io/instanceRole": role,
		}},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: status}}},
	}
}

func TestReplicaPod(t *testing.T) {
	k := &kubeClient{typed: fake.NewClientset(
		instancePod("repro-8790-1", "primary", true), instancePod("repro-8790-2", "replica", true))}
	p, err := k.replicaPod(context.Background(), "repro-8790", "repro-8790")
	if err != nil || p.Name != "repro-8790-2" {
		t.Errorf("got %v, %v", p, err)
	}
	k = &kubeClient{typed: fake.NewClientset(instancePod("repro-8790-1", "primary", true))}
	if _, err := k.replicaPod(context.Background(), "repro-8790", "repro-8790"); err == nil {
		t.Error("want an error without a replica")
	}
}

func TestPodReady(t *testing.T) {
	if !podReady(instancePod("a", "replica", true)) || podReady(instancePod("b", "replica", false)) {
		t.Error("Ready condition not read")
	}
	if podReady(&corev1.Pod{}) {
		t.Error("a pod without conditions is not Ready")
	}
}

func TestWaitPodReadyAlreadyReady(t *testing.T) {
	k := &kubeClient{typed: fake.NewClientset(instancePod("repro-8790-2", "replica", true))}
	if _, err := k.waitPodReady(context.Background(), "repro-8790", "repro-8790-2", time.Second); err != nil {
		t.Error(err)
	}
}

func TestWaitPodReadyWatchesTransition(t *testing.T) {
	cs := fake.NewClientset(instancePod("repro-8790-2", "replica", false))
	k := &kubeClient{typed: cs}
	go func() {
		time.Sleep(100 * time.Millisecond)
		_, _ = cs.CoreV1().Pods("repro-8790").Update(context.Background(),
			instancePod("repro-8790-2", "replica", true), metav1.UpdateOptions{})
	}()
	start := time.Now()
	readyAt, err := k.waitPodReady(context.Background(), "repro-8790", "repro-8790-2", 5*time.Second)
	if err != nil || readyAt.Before(start.Add(100*time.Millisecond)) {
		t.Errorf("readyAt %s after start, err %v", readyAt.Sub(start), err)
	}
}

func TestWaitPodReadyTimeout(t *testing.T) {
	k := &kubeClient{typed: fake.NewClientset(instancePod("repro-8790-2", "replica", false))}
	if _, err := k.waitPodReady(context.Background(), "repro-8790", "repro-8790-2", 100*time.Millisecond); err == nil {
		t.Error("want a timeout error")
	}
}

func TestDeletePodNow(t *testing.T) {
	k := &kubeClient{typed: fake.NewClientset(instancePod("repro-8790-1", "primary", true))}
	ctx := context.Background()
	if err := k.deletePodNow(ctx, "repro-8790", "repro-8790-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.pod(ctx, "repro-8790", "repro-8790-1"); !apierrors.IsNotFound(err) {
		t.Errorf("pod still there: %v", err)
	}
}

func TestFencePatch(t *testing.T) {
	if got := fencePatch("repro-8790-2"); got != `{"metadata":{"annotations":{"cnpg.io/fencedInstances":"[\"repro-8790-2\"]"}}}` {
		t.Errorf("fence: %s", got)
	}
	if got := fencePatch(""); got != `{"metadata":{"annotations":{"cnpg.io/fencedInstances":null}}}` {
		t.Errorf("unfence: %s", got)
	}
}

func TestPsqlCommand(t *testing.T) {
	got := psqlCommand("app", "SELECT 1")
	want := []string{"psql", "-XAtq", "-v", "ON_ERROR_STOP=1", "-d", "app", "-c", "SELECT 1"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v", got)
	}
}
