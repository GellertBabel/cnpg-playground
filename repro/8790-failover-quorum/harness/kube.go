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
	"bytes"
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
)

var (
	clusterGVR = schema.GroupVersionResource{Group: "postgresql.cnpg.io", Version: "v1", Resource: "clusters"}
	quorumGVR  = schema.GroupVersionResource{Group: "postgresql.cnpg.io", Version: "v1", Resource: "failoverquorums"}
)

type kubeClient struct {
	cfg     *rest.Config
	typed   kubernetes.Interface
	dynamic dynamic.Interface
	// exec runs a command in a pod container; execInPod outside of tests.
	exec func(ctx context.Context, ns, pod, container string, cmd ...string) (string, error)
}

// newKubeClient uses the same kubeconfig as kubectl: $KUBECONFIG or
// ~/.kube/config, current context.
func newKubeClient() (*kubeClient, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		rules, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return nil, err
	}
	typed, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	k := &kubeClient{cfg: cfg, typed: typed, dynamic: dyn}
	k.exec = k.execInPod
	return k, nil
}

func (k *kubeClient) currentPrimary(ctx context.Context, ns, name string) (string, error) {
	c, err := k.dynamic.Resource(clusterGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	p, _, _ := unstructured.NestedString(c.Object, "status", "currentPrimary")
	return p, nil
}

func (k *kubeClient) quorumStatus(ctx context.Context, ns, name string) string {
	q, err := k.dynamic.Resource(quorumGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return "<none>"
	}
	status, _, _ := unstructured.NestedMap(q.Object, "status")
	return fmt.Sprintf("%v", status)
}

// fencePatch is the merge patch that fences one instance, or clears the
// fencing annotation when instance is empty.
func fencePatch(instance string) string {
	if instance == "" {
		return `{"metadata":{"annotations":{"cnpg.io/fencedInstances":null}}}`
	}
	return fmt.Sprintf(`{"metadata":{"annotations":{"cnpg.io/fencedInstances":"[\"%s\"]"}}}`, instance)
}

// setFenced writes or clears the fencing annotation on the Cluster. An empty
// instance clears it.
func (k *kubeClient) setFenced(ctx context.Context, ns, name, instance string) error {
	_, err := k.dynamic.Resource(clusterGVR).Namespace(ns).Patch(
		ctx, name, types.MergePatchType, []byte(fencePatch(instance)), metav1.PatchOptions{})
	return err
}

func (k *kubeClient) replicaPod(ctx context.Context, ns, cluster string) (*corev1.Pod, error) {
	pods, err := k.typed.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("cnpg.io/cluster=%s,cnpg.io/instanceRole=replica", cluster),
	})
	if err != nil {
		return nil, err
	}
	if len(pods.Items) == 0 {
		return nil, fmt.Errorf("no replica pod for cluster %s", cluster)
	}
	return &pods.Items[0], nil
}

func (k *kubeClient) pod(ctx context.Context, ns, name string) (*corev1.Pod, error) {
	return k.typed.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
}

// workerNodeNames lists the nodes without the control-plane role. On kind the
// node name is also the name of the node's docker container.
func (k *kubeClient) workerNodeNames(ctx context.Context) ([]string, error) {
	nodes, err := k.typed.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	var names []string
	for _, n := range nodes.Items {
		if _, cp := n.Labels["node-role.kubernetes.io/control-plane"]; !cp {
			names = append(names, n.Name)
		}
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("no worker nodes in the current context; pass --nodes")
	}
	return names, nil
}

func podReady(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// waitPodReady watches one pod and returns when it becomes Ready, with the
// time the transition was observed.
func (k *kubeClient) waitPodReady(ctx context.Context, ns, name string, timeout time.Duration) (time.Time, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	w, err := k.typed.CoreV1().Pods(ns).Watch(ctx, metav1.ListOptions{
		FieldSelector: fields.OneTermEqualSelector("metadata.name", name).String(),
	})
	if err != nil {
		return time.Time{}, err
	}
	defer w.Stop()
	if p, err := k.pod(ctx, ns, name); err == nil && podReady(p) {
		return time.Now(), nil
	}
	for {
		select {
		case <-ctx.Done():
			return time.Time{}, fmt.Errorf("pod %s not Ready within %s", name, timeout)
		case ev, ok := <-w.ResultChan():
			if !ok {
				return time.Time{}, fmt.Errorf("watch closed before pod %s Ready", name)
			}
			if p, ok := ev.Object.(*corev1.Pod); ok && podReady(p) {
				return time.Now(), nil
			}
		}
	}
}

func (k *kubeClient) deletePodNow(ctx context.Context, ns, name string) error {
	zero := int64(0)
	return k.typed.CoreV1().Pods(ns).Delete(ctx, name, metav1.DeleteOptions{GracePeriodSeconds: &zero})
}

// execInPod runs a command in a pod container and returns stdout. Used for the
// read-only SQL the orchestrator cannot run from outside the pod network, and
// for reading the marker ids on the new primary.
func (k *kubeClient) execInPod(ctx context.Context, ns, pod, container string, cmd ...string) (string, error) {
	req := k.typed.CoreV1().RESTClient().Post().
		Resource("pods").Name(pod).Namespace(ns).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container,
			Command:   cmd,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)
	exec, err := remotecommand.NewSPDYExecutor(k.cfg, "POST", req.URL())
	if err != nil {
		return "", err
	}
	var out, errb bytes.Buffer
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &out, Stderr: &errb})
	if err != nil {
		return out.String(), fmt.Errorf("exec in %s: %v: %s", pod, err, errb.String())
	}
	return out.String(), nil
}

// psqlCommand is the psql invocation for one SQL statement against the named
// database, unaligned and tuples only, stopping on the first error.
func psqlCommand(db, query string) []string {
	return []string{"psql", "-XAtq", "-v", "ON_ERROR_STOP=1", "-d", db, "-c", query}
}

// psql runs one SQL statement in a pod's postgres container and returns its
// output.
func (k *kubeClient) psql(ctx context.Context, ns, pod, db, query string) (string, error) {
	return k.exec(ctx, ns, pod, "postgres", psqlCommand(db, query)...)
}
