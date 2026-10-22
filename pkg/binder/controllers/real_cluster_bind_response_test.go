// Copyright 2026 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package controllers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/kai-scheduler/KAI-scheduler/pkg/binder/binding"
	"github.com/kai-scheduler/KAI-scheduler/pkg/binder/binding/resourcereservation"
	"github.com/kai-scheduler/KAI-scheduler/pkg/binder/common"
	"github.com/kai-scheduler/KAI-scheduler/pkg/binder/plugins"
	"github.com/kai-scheduler/KAI-scheduler/pkg/binder/plugins/gpusharing"
	"github.com/kai-scheduler/KAI-scheduler/pkg/binder/plugins/state"
	"github.com/kai-scheduler/api/constants"
	schedulingv1alpha2 "github.com/kai-scheduler/api/scheduling/v1alpha2"
)

const privateKindReservationNamespace = "kai2452-reservations"

func TestPrivateKindBindingResponseLoss(t *testing.T) {
	kubeconfig := os.Getenv("KAI_PRIVATE_KUBECONFIG")
	if kubeconfig == "" {
		t.Skip("requires the dedicated private Kind cluster kubeconfig")
	}
	receipts := os.Getenv("KAI_PRIVATE_RECEIPTS")
	require.NotEmpty(t, receipts)
	require.NoError(t, os.MkdirAll(receipts, 0o700))
	nodeName := os.Getenv("KAI_PRIVATE_NODE")
	require.Equal(t, "kai2452-repro-20261022-f4c2a9-control-plane", nodeName)
	expectedNodeUID := os.Getenv("KAI_PRIVATE_NODE_UID")
	require.NotEmpty(t, expectedNodeUID)
	rawConfig, err := clientcmd.LoadFromFile(kubeconfig)
	require.NoError(t, err)
	require.Equal(t, "kind-kai2452-repro-20261022-f4c2a9", rawConfig.CurrentContext)
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	require.NoError(t, err)
	upstream, err := url.Parse(cfg.Host)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1", upstream.Hostname())
	require.Equal(t, "https", upstream.Scheme)
	scheme := runtime.NewScheme()
	require.NoError(t, v1.AddToScheme(scheme))
	require.NoError(t, schedulingv1alpha2.AddToScheme(scheme))
	bindingDecoder := serializer.NewCodecFactory(scheme).UniversalDeserializer()
	direct, err := client.NewWithWatch(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	ctx := context.Background()
	node := &v1.Node{}
	require.NoError(t, direct.Get(ctx, client.ObjectKey{Name: nodeName}, node))
	require.Equal(t, types.UID(expectedNodeUID), node.UID)
	require.Contains(t, node.Labels, "node-role.kubernetes.io/control-plane")
	originalNode := node.DeepCopy()
	if node.Labels == nil {
		node.Labels = map[string]string{}
	}
	node.Labels[constants.NvidiaGpuMemory] = "8192"
	require.NoError(t, direct.Patch(ctx, node, client.MergeFrom(originalNode)))
	requirePrivateKindNamespace(t, ctx, direct, privateKindReservationNamespace)
	writePrivateKindJSON(t, receipts, "node-fixture.json", node)

	for _, enabled := range []bool{true, false} {
		mode := "reader"
		if !enabled {
			mode = "nil-reader"
		}
		for _, scenario := range []string{"ordinary", "lost-response", "rejection"} {
			t.Run(mode+"/"+scenario, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
				defer cancel()
				name := "kai2452-" + mode + "-" + scenario
				group := name + "-group"
				requirePrivateKindNamespace(t, ctx, direct, name)
				pod := &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{Namespace: name, Name: "fractional-pod",
						Annotations: map[string]string{"runai/shared-gpu-configmap": "fractional-pod-gpu"}},
					Spec: v1.PodSpec{SchedulerName: "kai-private-no-scheduler",
						Containers: []v1.Container{{Name: "worker", Image: "registry.k8s.io/pause:3.10", ImagePullPolicy: v1.PullNever}}},
				}
				common.AddGPUSharingEnvVars(&pod.Spec.Containers[0], "fractional-pod-gpu-0")
				require.NoError(t, direct.Create(ctx, pod))
				reservation := &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{Namespace: privateKindReservationNamespace, Name: name,
						Labels:      map[string]string{constants.GPUGroup: group},
						Annotations: map[string]string{"run.ai/reserve_for_gpu_index": "GPU-0"}},
					Spec: v1.PodSpec{NodeName: node.Name,
						Containers: []v1.Container{{Name: "reservation", Image: "registry.k8s.io/pause:3.10", ImagePullPolicy: v1.PullNever}}},
				}
				require.NoError(t, direct.Create(ctx, reservation))
				require.Eventually(t, func() bool {
					return direct.Get(ctx, client.ObjectKeyFromObject(reservation), reservation) == nil && privateKindPodReady(reservation)
				}, 45*time.Second, 200*time.Millisecond, "real kubelet must run the reservation Pod")
				br := &schedulingv1alpha2.BindRequest{
					ObjectMeta: metav1.ObjectMeta{Namespace: name, Name: "bind-request"},
					Spec: schedulingv1alpha2.BindRequestSpec{PodName: pod.Name, SelectedNode: node.Name,
						ReceivedResourceType: common.ReceivedTypeFraction,
						SelectedGPUGroups:    []string{group},
						ReceivedGPU:          &schedulingv1alpha2.ReceivedGPU{Count: 1, Portion: "0.5"}},
				}
				require.NoError(t, direct.Create(ctx, br))
				br.Status.Phase = schedulingv1alpha2.BindRequestPhasePending
				require.NoError(t, direct.Status().Update(ctx, br))
				writePrivateKindJSON(t, receipts, name+"-inputs.json", map[string]interface{}{"pod": pod, "reservation": reservation, "bindRequest": br})

				result := &privateKindResult{Mode: mode, Scenario: scenario, PodUID: pod.UID, Node: node.Name}
				upstreamTransport, err := rest.TransportFor(cfg)
				require.NoError(t, err)
				var proxyMu sync.Mutex
				var proxyFailure error
				proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
					isBinding := request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/binding")
					var body []byte
					var bindingRequest v1.Binding
					if isBinding {
						rawBody, readErr := io.ReadAll(request.Body)
						if readErr != nil {
							http.Error(w, readErr.Error(), http.StatusBadRequest)
							return
						}
						body = rawBody
						if _, _, decodeErr := bindingDecoder.Decode(body, nil, &bindingRequest); decodeErr != nil {
							proxyMu.Lock()
							proxyFailure = fmt.Errorf("Binding decode failed for Content-Type %q: %w", request.Header.Get("Content-Type"), decodeErr)
							proxyMu.Unlock()
							http.Error(w, decodeErr.Error(), http.StatusBadRequest)
							return
						}
					}
					forward := request.Clone(request.Context())
					forward.URL.Scheme, forward.URL.Host = upstream.Scheme, upstream.Host
					forward.Host, forward.RequestURI = upstream.Host, ""
					forwardedBody := bytes.Clone(body)
					if isBinding {
						forward.Body = io.NopCloser(bytes.NewReader(forwardedBody))
					}
					response, err := upstreamTransport.RoundTrip(forward)
					if err != nil {
						http.Error(w, err.Error(), http.StatusBadGateway)
						return
					}
					defer response.Body.Close()
					wire := privateKindWire{UID: bindingRequest.UID, Node: bindingRequest.Target.Name, HTTP: response.StatusCode,
						RequestContentType: request.Header.Get("Content-Type"), IncomingBodySHA256: fmt.Sprintf("%x", sha256.Sum256(body)),
						ForwardedBodySHA256: fmt.Sprintf("%x", sha256.Sum256(forwardedBody))}
					if isBinding && scenario == "lost-response" && response.StatusCode == http.StatusCreated {
						_, _ = io.Copy(io.Discard, response.Body)
						verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 45*time.Second)
						defer verifyCancel()
						var verified *v1.Pod
						var verificationFailure error
						for {
							observed := &v1.Pod{}
							wire.ProxyVerificationGETs++
							readErr := direct.Get(verifyCtx, client.ObjectKeyFromObject(pod), observed)
							if readErr == nil && observed.UID == pod.UID && observed.Spec.NodeName == node.Name && privateKindPodReady(observed) {
								verified = observed
								break
							}
							select {
							case <-verifyCtx.Done():
								verificationFailure = fmt.Errorf("committed Pod did not become Running/Ready: last=%+v error=%v", observed.Status, readErr)
							case <-time.After(200 * time.Millisecond):
								continue
							}
							break
						}
						if verified != nil {
							wire.CommittedPod = verified
						}
						connection, _, hijackErr := w.(http.Hijacker).Hijack()
						if hijackErr != nil {
							verificationFailure = hijackErr
						} else {
							wire.SocketClosedBeforeHeaders = true
						}
						proxyMu.Lock()
						proxyFailure = verificationFailure
						result.Wire = append(result.Wire, wire)
						proxyMu.Unlock()
						if connection != nil {
							_ = connection.Close()
						}
						return
					}
					if isBinding {
						proxyMu.Lock()
						result.Wire = append(result.Wire, wire)
						proxyMu.Unlock()
					}
					for key, values := range response.Header {
						for _, value := range values {
							w.Header().Add(key, value)
						}
					}
					w.WriteHeader(response.StatusCode)
					_, _ = io.Copy(w, response.Body)
				}))
				defer proxy.Close()
				pipelineConfig := rest.CopyConfig(cfg)
				pipelineConfig.Host = proxy.URL
				pipelineConfig.TLSClientConfig = rest.TLSClientConfig{}
				pipelineConfig.Wrap(func(base http.RoundTripper) http.RoundTripper {
					return privateKindTransportFunc(func(request *http.Request) (*http.Response, error) {
						response, transportErr := base.RoundTrip(request)
						if request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/binding") && transportErr != nil {
							proxyMu.Lock()
							result.TransportErrors = append(result.TransportErrors, fmt.Sprintf("%T: %v", transportErr, transportErr))
							proxyMu.Unlock()
						}
						return response, transportErr
					})
				})
				pipelineBase, err := client.NewWithWatch(pipelineConfig, client.Options{Scheme: scheme})
				require.NoError(t, err)
				pipeline := interceptor.NewClient(pipelineBase, interceptor.Funcs{
					SubResourceCreate: func(ctx context.Context, c client.Client, subresource string, obj, body client.Object, opts ...client.SubResourceCreateOption) error {
						if subresource != "binding" {
							return c.SubResource(subresource).Create(ctx, obj, body, opts...)
						}
						result.LogicalBindingCalls++
						result.Prepared = readPrivateKindResources(t, ctx, direct, pod, reservation)
						require.Equal(t, group, result.Prepared.Pod.Labels[constants.GPUGroup])
						require.Len(t, result.Prepared.ConfigMaps, 2)
						if scenario == "rejection" {
							invalid := body.(*v1.Binding).DeepCopy()
							invalid.UID = "00000000-0000-0000-0000-000000000000"
							body = invalid
						}
						bindingErr := c.SubResource(subresource).Create(ctx, obj, body, opts...)
						if bindingErr != nil {
							result.BindingError = fmt.Sprintf("%T: %v", bindingErr, bindingErr)
						}
						return bindingErr
					},
				})
				confirmation := interceptor.NewClient(direct, interceptor.Funcs{
					Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
						result.ConfirmationReads++
						err := c.Get(ctx, key, obj, opts...)
						if observed, ok := obj.(*v1.Pod); ok && err == nil {
							result.ConfirmationPod = observed.DeepCopy()
						}
						return err
					},
				})
				var reader client.Reader = confirmation
				if !enabled {
					reader = nil
				}
				rrs := resourcereservation.NewService(false, pipeline, "", time.Second,
					privateKindReservationNamespace, "unused", "private-repro", name, "", nil, nil, nil, false)
				binderPlugins := plugins.New()
				binderPlugins.RegisterPlugin(gpusharing.New(pipeline, false, false))
				binderPlugins.RegisterPlugin(&privateKindPostBindRecorder{result: result})
				binder := binding.NewBinder(pipeline, reader, rrs, binderPlugins)
				reconciler := NewBindRequestReconciler(pipeline, scheme, record.NewFakeRecorder(20), &ReconcilerParams{}, binder, rrs)
				_, reconcileErr := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(br)})
				if reconcileErr != nil {
					result.ReconcileError = reconcileErr.Error()
				}
				result.Final = readPrivateKindResources(t, ctx, direct, pod, reservation)
				result.BindRequest = &schedulingv1alpha2.BindRequest{}
				require.NoError(t, direct.Get(ctx, client.ObjectKeyFromObject(br), result.BindRequest))
				proxyMu.Lock()
				capturedProxyFailure := proxyFailure
				proxyMu.Unlock()
				require.NoError(t, capturedProxyFailure)
				writePrivateKindJSON(t, receipts, name+"-result.json", result)
				wireSummary := []string{}
				for _, attempt := range result.Wire {
					wireSummary = append(wireSummary, fmt.Sprintf("HTTP=%d UID=%s node=%s proxyGETs=%d socketClosed=%t", attempt.HTTP, attempt.UID, attempt.Node, attempt.ProxyVerificationGETs, attempt.SocketClosedBeforeHeaders))
				}
				t.Logf("mode=%s scenario=%s logicalBindings=%d confirmationReads=%d PostBind=%d rollback=%d wire=%+v error=%q phase=%s group=%q ConfigMaps=%d reservationUID=%s",
					mode, scenario, result.LogicalBindingCalls, result.ConfirmationReads, result.PostBindCalls, result.RollbackCalls, wireSummary,
					result.BindingError, result.BindRequest.Status.Phase, result.Final.Pod.Labels[constants.GPUGroup], len(result.Final.ConfigMaps), result.Final.Reservation.UID)
				require.Equal(t, 1, result.LogicalBindingCalls)
				require.Len(t, result.Wire, 1, "record every wire attempt; default retries must not replay this Binding")
				assert.NotEmpty(t, result.Wire[0].RequestContentType)
				assert.Equal(t, result.Wire[0].IncomingBodySHA256, result.Wire[0].ForwardedBodySHA256, "proxy must forward the exact serialized Binding bytes")
				assert.Equal(t, pod.UID, result.Final.Pod.UID)
				assert.Equal(t, reservation.UID, result.Final.Reservation.UID)
				assert.Nil(t, result.Final.Reservation.DeletionTimestamp)
				wantSuccess := scenario == "ordinary" || scenario == "lost-response" && enabled
				if wantSuccess {
					assert.NoError(t, reconcileErr)
					assert.Equal(t, schedulingv1alpha2.BindRequestPhaseSucceeded, result.BindRequest.Status.Phase)
					assert.Equal(t, group, result.Final.Pod.Labels[constants.GPUGroup])
					assert.Equal(t, privateKindConfigMapUIDs(result.Prepared.ConfigMaps), privateKindConfigMapUIDs(result.Final.ConfigMaps))
					assert.Equal(t, 1, result.PostBindCalls)
					assert.Zero(t, result.RollbackCalls)
				} else {
					assert.Error(t, reconcileErr)
					assert.Equal(t, schedulingv1alpha2.BindRequestPhaseFailed, result.BindRequest.Status.Phase)
					assert.Empty(t, result.Final.Pod.Labels[constants.GPUGroup])
					assert.Empty(t, result.Final.ConfigMaps)
					assert.Zero(t, result.PostBindCalls)
					assert.Equal(t, 1, result.RollbackCalls)
				}
				if scenario == "ordinary" {
					assert.Equal(t, http.StatusCreated, result.Wire[0].HTTP)
					assert.Empty(t, result.TransportErrors)
					assert.Empty(t, result.BindingError)
					assert.Zero(t, result.ConfirmationReads)
				} else if scenario == "lost-response" {
					assert.Equal(t, http.StatusCreated, result.Wire[0].HTTP)
					assert.True(t, result.Wire[0].SocketClosedBeforeHeaders)
					require.NotNil(t, result.Wire[0].CommittedPod)
					assert.Equal(t, pod.UID, result.Wire[0].CommittedPod.UID)
					assert.Equal(t, node.Name, result.Wire[0].CommittedPod.Spec.NodeName)
					assert.True(t, privateKindPodReady(result.Wire[0].CommittedPod))
					assert.NotEmpty(t, result.TransportErrors, "must observe the genuine socket error")
					assert.NotEmpty(t, result.BindingError)
					assert.Equal(t, node.Name, result.Final.Pod.Spec.NodeName)
					if enabled {
						assert.Equal(t, 1, result.ConfirmationReads)
						require.NotNil(t, result.ConfirmationPod)
						assert.Equal(t, pod.UID, result.ConfirmationPod.UID)
						assert.Equal(t, node.Name, result.ConfirmationPod.Spec.NodeName)
					} else {
						assert.Zero(t, result.ConfirmationReads)
					}
				} else {
					assert.Equal(t, http.StatusConflict, result.Wire[0].HTTP)
					assert.Empty(t, result.TransportErrors)
					assert.Empty(t, result.Final.Pod.Spec.NodeName)
					if enabled {
						assert.Equal(t, 1, result.ConfirmationReads)
					} else {
						assert.Zero(t, result.ConfirmationReads)
					}
				}
			})
		}
	}
}

type privateKindResult struct {
	Mode, Scenario                                                       string
	PodUID                                                               types.UID
	Node                                                                 string
	LogicalBindingCalls, ConfirmationReads, PostBindCalls, RollbackCalls int
	TransportErrors                                                      []string
	BindingError, ReconcileError                                         string
	Wire                                                                 []privateKindWire
	ConfirmationPod                                                      *v1.Pod
	Prepared, Final                                                      privateKindResources
	BindRequest                                                          *schedulingv1alpha2.BindRequest
}

type privateKindWire struct {
	UID                         types.UID
	Node                        string
	HTTP, ProxyVerificationGETs int
	SocketClosedBeforeHeaders   bool
	CommittedPod                *v1.Pod
	RequestContentType          string
	IncomingBodySHA256          string
	ForwardedBodySHA256         string
}

type privateKindResources struct {
	Pod, Reservation *v1.Pod
	ConfigMaps       map[string]*v1.ConfigMap
}

func requirePrivateKindNamespace(t *testing.T, ctx context.Context, direct client.Client, name string) {
	t.Helper()
	require.NoError(t, direct.Create(ctx, &v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}))
	require.Eventually(t, func() bool {
		return direct.Get(ctx, client.ObjectKey{Namespace: name, Name: "default"}, &v1.ServiceAccount{}) == nil
	}, 15*time.Second, 100*time.Millisecond, "real service-account controller must initialize the namespace")
}

func readPrivateKindResources(t *testing.T, ctx context.Context, direct client.Client, pod, reservation *v1.Pod) privateKindResources {
	t.Helper()
	resources := privateKindResources{Pod: &v1.Pod{}, Reservation: &v1.Pod{}, ConfigMaps: map[string]*v1.ConfigMap{}}
	require.NoError(t, direct.Get(ctx, client.ObjectKeyFromObject(pod), resources.Pod))
	require.NoError(t, direct.Get(ctx, client.ObjectKeyFromObject(reservation), resources.Reservation))
	for _, name := range []string{"fractional-pod-gpu-0", "fractional-pod-gpu-0-evar"} {
		cm := &v1.ConfigMap{}
		err := direct.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: name}, cm)
		if apierrors.IsNotFound(err) {
			continue
		}
		require.NoError(t, err)
		require.NotEmpty(t, cm.UID)
		resources.ConfigMaps[name] = cm
	}
	return resources
}

func privateKindConfigMapUIDs(configmaps map[string]*v1.ConfigMap) map[string]types.UID {
	uids := map[string]types.UID{}
	for name, cm := range configmaps {
		uids[name] = cm.UID
	}
	return uids
}

func privateKindPodReady(pod *v1.Pod) bool {
	if pod.Status.Phase != v1.PodRunning {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == v1.PodReady && condition.Status == v1.ConditionTrue {
			return true
		}
	}
	return false
}

func writePrivateKindJSON(t *testing.T, directory, name string, value interface{}) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(directory, name), append(data, '\n'), 0o600))
}

type privateKindTransportFunc func(*http.Request) (*http.Response, error)

func (f privateKindTransportFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type privateKindPostBindRecorder struct{ result *privateKindResult }

func (*privateKindPostBindRecorder) Name() string { return "private-kind-observer" }
func (*privateKindPostBindRecorder) PreBind(context.Context, *v1.Pod, *v1.Node, *schedulingv1alpha2.BindRequest, *state.BindingState) error {
	return nil
}
func (p *privateKindPostBindRecorder) PostBind(context.Context, *v1.Pod, *v1.Node, *schedulingv1alpha2.BindRequest, *state.BindingState) {
	p.result.PostBindCalls++
}
func (p *privateKindPostBindRecorder) Rollback(context.Context, *v1.Pod, *v1.Node, *schedulingv1alpha2.BindRequest, *state.BindingState) error {
	p.result.RollbackCalls++
	return nil
}
