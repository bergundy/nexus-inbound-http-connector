package nexushttp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/temporalproto"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestResolveClientOptionsFromEnvironment pins the documented default: no
// ClientOptions means the connector reads Temporal environment configuration.
func TestResolveClientOptionsFromEnvironment(t *testing.T) {
	// The config file is pointed at a path that does not exist so a developer's
	// own temporal.toml cannot leak into the test.
	t.Setenv("TEMPORAL_CONFIG_FILE", filepath.Join(t.TempDir(), "temporal.toml"))
	t.Setenv("TEMPORAL_ADDRESS", "temporal.example.com:7233")
	t.Setenv("TEMPORAL_NAMESPACE", "env-namespace")

	clientOptions, err := resolveClientOptions(Options{})
	require.NoError(t, err)
	require.Equal(t, "temporal.example.com:7233", clientOptions.HostPort)
	require.Equal(t, "env-namespace", clientOptions.Namespace)

	// Supplied options are used as-is, without consulting the environment.
	supplied := client.Options{HostPort: "supplied:7233", Namespace: "supplied-namespace"}
	clientOptions, err = resolveClientOptions(Options{ClientOptions: &supplied})
	require.NoError(t, err)
	require.Equal(t, supplied.HostPort, clientOptions.HostPort)
	require.Equal(t, supplied.Namespace, clientOptions.Namespace)
}

// TestNamespaceDefaultsLikeTheSDK keeps the connector's namespace default in
// step with the client it dials.
func TestNamespaceDefaultsLikeTheSDK(t *testing.T) {
	t.Parallel()
	var gotRequest *workflowservice.CountNexusOperationExecutionsRequest
	h := newTestHandler(t, &fakeWorkflowService{
		count: func(_ context.Context, request *workflowservice.CountNexusOperationExecutionsRequest) (*workflowservice.CountNexusOperationExecutionsResponse, error) {
			gotRequest = request
			return &workflowservice.CountNexusOperationExecutionsResponse{}, nil
		},
	}, Options{ClientOptions: &client.Options{}})

	response := serve(h, http.MethodGet, "/operation-count", "")
	requireStatus(t, http.StatusOK, response)
	require.Equal(t, client.DefaultNamespace, gotRequest.Namespace)
}

// TestIdentityComesFromClientOptions covers the one client option the
// connector copies into request bodies.
func TestIdentityComesFromClientOptions(t *testing.T) {
	t.Parallel()
	var gotStart *workflowservice.StartNexusOperationExecutionRequest
	var gotCancel *workflowservice.RequestCancelNexusOperationExecutionRequest
	var gotTerminate *workflowservice.TerminateNexusOperationExecutionRequest
	h := newTestHandler(t, &fakeWorkflowService{
		start: func(_ context.Context, request *workflowservice.StartNexusOperationExecutionRequest) (*workflowservice.StartNexusOperationExecutionResponse, error) {
			gotStart = request
			return &workflowservice.StartNexusOperationExecutionResponse{}, nil
		},
		cancel: func(_ context.Context, request *workflowservice.RequestCancelNexusOperationExecutionRequest) (*workflowservice.RequestCancelNexusOperationExecutionResponse, error) {
			gotCancel = request
			return &workflowservice.RequestCancelNexusOperationExecutionResponse{}, nil
		},
		terminate: func(_ context.Context, request *workflowservice.TerminateNexusOperationExecutionRequest) (*workflowservice.TerminateNexusOperationExecutionResponse, error) {
			gotTerminate = request
			return &workflowservice.TerminateNexusOperationExecutionResponse{}, nil
		},
	}, Options{ClientOptions: &client.Options{Namespace: "test-namespace", Identity: "connector-identity"}})

	serve(h, http.MethodPost, "/operations/op", `{"endpoint":"e","service":"s","operation":"o"}`)
	serve(h, http.MethodPost, "/operations/op/cancel", `{}`)
	serve(h, http.MethodPost, "/operations/op/terminate", `{"identity":"caller-identity"}`)

	require.Equal(t, "connector-identity", gotStart.Identity)
	require.Equal(t, "connector-identity", gotCancel.Identity)
	require.Equal(t, "caller-identity", gotTerminate.Identity, "the caller's identity must be preserved")
}

// TestIdentityIsNotSynthesized pins that an unset client identity stays unset,
// rather than picking up the SDK's generated default.
func TestIdentityIsNotSynthesized(t *testing.T) {
	t.Parallel()
	var gotStart *workflowservice.StartNexusOperationExecutionRequest
	h := newTestHandler(t, &fakeWorkflowService{
		start: func(_ context.Context, request *workflowservice.StartNexusOperationExecutionRequest) (*workflowservice.StartNexusOperationExecutionResponse, error) {
			gotStart = request
			return &workflowservice.StartNexusOperationExecutionResponse{}, nil
		},
	}, Options{})

	serve(h, http.MethodPost, "/operations/op", `{"endpoint":"e","service":"s","operation":"o"}`)
	require.Empty(t, gotStart.Identity)
}

func TestMaxRequestBodySize(t *testing.T) {
	t.Parallel()

	body := `{"endpoint":"e","service":"s","operation":"o"}`
	oversizedBody := `{"endpoint":"` + strings.Repeat("e", int(defaultMaxRequestBodySize)) + `","service":"s","operation":"o"}`
	tests := []struct {
		name  string
		limit RequestBodySizeLimit
		body  string
		want  int
	}{
		{"default", nil, body, http.StatusCreated},
		{"default rejects oversized body", nil, oversizedBody, http.StatusBadRequest},
		{"exact limit", MaxRequestBodySize(len(body)), body, http.StatusCreated},
		{"zero", MaxRequestBodySize(0), body, http.StatusBadRequest},
		{"not enforced", MaxRequestBodySizeNotEnforced{}, oversizedBody, http.StatusCreated},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t, &fakeWorkflowService{
				start: func(context.Context, *workflowservice.StartNexusOperationExecutionRequest) (*workflowservice.StartNexusOperationExecutionResponse, error) {
					return &workflowservice.StartNexusOperationExecutionResponse{}, nil
				},
			}, Options{MaxRequestBodySize: test.limit})
			requireStatus(t, test.want, serve(h, http.MethodPost, "/operations/op", test.body))
		})
	}
}

func TestHandlerLogsInternalErrors(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	h := newTestHandler(t, &fakeWorkflowService{
		start: func(context.Context, *workflowservice.StartNexusOperationExecutionRequest) (*workflowservice.StartNexusOperationExecutionResponse, error) {
			return nil, errors.New("service broke")
		},
	}, Options{Logger: slog.New(slog.NewTextHandler(&logs, nil))})

	response := serve(h, http.MethodPost, "/operations/op", `{"endpoint":"e","service":"s","operation":"o"}`)
	requireStatus(t, http.StatusInternalServerError, response)
	require.Contains(t, logs.String(), `msg="internal error"`)
	require.Contains(t, logs.String(), `error="service broke"`)
}

func TestStartOperationProtoJSON(t *testing.T) {
	t.Parallel()

	var gotRequest *workflowservice.StartNexusOperationExecutionRequest
	service := &fakeWorkflowService{start: func(_ context.Context, request *workflowservice.StartNexusOperationExecutionRequest) (*workflowservice.StartNexusOperationExecutionResponse, error) {
		gotRequest = request
		return &workflowservice.StartNexusOperationExecutionResponse{RunId: "run-1", Started: true}, nil
	}}
	h := newTestHandler(t, service, Options{ClientOptions: &client.Options{Namespace: "configured-namespace"}})

	body := `{
		"namespace":"caller-namespace",
		"requestId":"fixed-start-request",
		"endpoint":"payments",
		"service":"charge",
		"operation":"authorize",
		"input":{"amount":42},
		"scheduleToCloseTimeout":"3600s",
		"scheduleToStartTimeout":"30s",
		"startToCloseTimeout":"300s",
		"idConflictPolicy":"UseExisting",
		"idReusePolicy":"RejectDuplicate",
		"searchAttributes":{"indexedFields":{"CustomKeywordField":"card"}},
		"nexusHeader":{"trace-id":"trace-1"},
		"userMetadata":{"summary":"card authorization","details":{"card":"ending 42"}}
	}`
	response := serve(h, http.MethodPost, "/operations/op%20%2F%20ID", body)
	requireStatus(t, http.StatusCreated, response)
	require.NotNil(t, gotRequest, "StartNexusOperationExecution was not called")

	// The configured namespace wins over the caller's, and the operation ID
	// comes from the percent-decoded path.
	require.Equal(t, "configured-namespace", gotRequest.Namespace)
	require.Equal(t, "fixed-start-request", gotRequest.RequestId)
	require.Equal(t, "op / ID", gotRequest.OperationId)

	require.Equal(t, "payments", gotRequest.Endpoint)
	require.Equal(t, "charge", gotRequest.Service)
	require.Equal(t, "authorize", gotRequest.Operation)
	require.Equal(t, `{"amount":42}`, string(gotRequest.Input.Data))
	require.Equal(t, converter.MetadataEncodingJSON, string(gotRequest.Input.Metadata[converter.MetadataEncoding]))

	require.Equal(t, time.Hour, gotRequest.ScheduleToCloseTimeout.AsDuration())
	require.Equal(t, 30*time.Second, gotRequest.ScheduleToStartTimeout.AsDuration())
	require.Equal(t, 5*time.Minute, gotRequest.StartToCloseTimeout.AsDuration())

	require.Equal(t, enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_USE_EXISTING, gotRequest.IdConflictPolicy)
	require.Equal(t, enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_REJECT_DUPLICATE, gotRequest.IdReusePolicy)

	require.NotNil(t, gotRequest.SearchAttributes.GetIndexedFields()["CustomKeywordField"])
	require.Equal(t, "trace-1", gotRequest.NexusHeader["trace-id"])
	require.Equal(t, `"card authorization"`, string(gotRequest.UserMetadata.GetSummary().GetData()))
	require.Equal(t, `{"card":"ending 42"}`, string(gotRequest.UserMetadata.GetDetails().GetData()))

	decoded := &workflowservice.StartNexusOperationExecutionResponse{}
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	err := temporalproto.CustomJSONUnmarshalOptions{Metadata: newRequestQuery(request).payloadMetadata()}.Unmarshal(response.Body.Bytes(), decoded)
	require.NoError(t, err, "unmarshal response")
	require.Equal(t, "run-1", decoded.RunId)
	require.False(t, decoded.Started)
	require.NotContains(t, response.Body.String(), `"started"`)
}

func TestStartOperationCodec(t *testing.T) {
	t.Parallel()

	codec := &prefixCodec{}
	service := &fakeWorkflowService{start: func(_ context.Context, request *workflowservice.StartNexusOperationExecutionRequest) (*workflowservice.StartNexusOperationExecutionResponse, error) {
		require.NotEmpty(t, request.RequestId, "request ID must be generated")
		require.Equal(t, "encoded/test", string(request.Input.Metadata[converter.MetadataEncoding]))
		require.Equal(t, `prefix:{"value":1}`, string(request.Input.Data))
		return &workflowservice.StartNexusOperationExecutionResponse{RunId: "run-1"}, nil
	}}
	h := newTestHandler(t, service, Options{PayloadCodecs: []converter.PayloadCodec{codec}})
	response := serve(h, http.MethodPost, "/operations/op", `{"endpoint":"e","service":"s","operation":"o","input":{"value":1}}`)
	requireStatus(t, http.StatusCreated, response)
	require.Equal(t, 1, codec.encoded)
	require.Equal(t, 0, codec.decoded)
}

// TestStartOperationCodecSkipsSearchAttributes pins the one payload the codecs
// must not touch: the server indexes search attributes, so it has to be able to
// read them. Every other embedded payload is encoded.
func TestStartOperationCodecSkipsSearchAttributes(t *testing.T) {
	t.Parallel()

	codec := &prefixCodec{}
	var gotRequest *workflowservice.StartNexusOperationExecutionRequest
	h := newTestHandler(t, &fakeWorkflowService{
		start: func(_ context.Context, request *workflowservice.StartNexusOperationExecutionRequest) (*workflowservice.StartNexusOperationExecutionResponse, error) {
			gotRequest = request
			return &workflowservice.StartNexusOperationExecutionResponse{RunId: "run-1"}, nil
		},
	}, Options{PayloadCodecs: []converter.PayloadCodec{codec}})

	response := serve(h, http.MethodPost, "/operations/op", `{
		"endpoint":"e","service":"s","operation":"o",
		"input":{"value":1},
		"searchAttributes":{"indexedFields":{"CustomKeywordField":"card"}},
		"userMetadata":{"summary":"card authorization"}
	}`)
	requireStatus(t, http.StatusCreated, response)

	searchAttribute := gotRequest.SearchAttributes.GetIndexedFields()["CustomKeywordField"]
	require.NotNil(t, searchAttribute)
	require.Equal(t, `"card"`, string(searchAttribute.Data), "search attribute must not be encoded")
	require.Equal(t, converter.MetadataEncodingJSON, string(searchAttribute.Metadata[converter.MetadataEncoding]))
	require.Equal(t, `prefix:{"value":1}`, string(gotRequest.Input.Data), "input must be encoded")
	require.Equal(t, `prefix:"card authorization"`, string(gotRequest.UserMetadata.GetSummary().GetData()), "user metadata summary must be encoded")
	// Input and the user metadata summary, but not the search attribute.
	require.Equal(t, 2, codec.encoded)
}

func TestPayloadShorthandNullRoundTrip(t *testing.T) {
	t.Parallel()

	request := httptest.NewRequest(http.MethodPost, "/operations/id", bytes.NewBufferString(
		`{"endpoint":"e","service":"s","operation":"o","input":null}`))
	query := newRequestQuery(request)
	message := &workflowservice.StartNexusOperationExecutionRequest{}
	require.NoError(t, decodeProtoBody(request.Body, query, message, false), "decode null input")
	require.NoError(t, newBareHandler().encodeStartRequest(t.Context(), message), "encode null input")
	payload := message.Input
	require.Equal(t, converter.MetadataEncodingNil, string(payload.Metadata[converter.MetadataEncoding]))

	responseRecorder := httptest.NewRecorder()
	newBareHandler().writeProto(responseRecorder, query, http.StatusOK, &nexuspb.StartOperationResponse{
		Variant: &nexuspb.StartOperationResponse_SyncSuccess{
			SyncSuccess: &nexuspb.StartOperationResponse_Sync{Payload: payload},
		},
	})
	require.Contains(t, responseRecorder.Body.String(), `"encoding":"YmluYXJ5L251bGw="`)
}

func TestNoPayloadShorthandUsesCanonicalProtoJSON(t *testing.T) {
	t.Parallel()

	request := httptest.NewRequest(http.MethodPost, "/operations/id?noPayloadShorthand", bytes.NewBufferString(
		`{"endpoint":"e","service":"s","operation":"o","input":{"metadata":{"encoding":"anNvbi9wbGFpbg=="},"data":"eyJ4IjoxfQ=="}}`))
	query := newRequestQuery(request)
	message := &workflowservice.StartNexusOperationExecutionRequest{}
	require.NoError(t, decodeProtoBody(request.Body, query, message, false), "decode canonical input")
	require.Equal(t, `{"x":1}`, string(message.Input.Data))

	responseRecorder := httptest.NewRecorder()
	newBareHandler().writeProto(responseRecorder, query, http.StatusOK, &workflowservice.PollNexusOperationExecutionResponse{
		Outcome: &workflowservice.PollNexusOperationExecutionResponse_Result{Result: message.Input},
	})
	require.Contains(t, responseRecorder.Body.String(), `"data":"eyJ4IjoxfQ=="`)
}

func TestReadAndActionRoutesUseProtoResponses(t *testing.T) {
	t.Parallel()

	rawInfo := &nexuspb.NexusOperationExecutionInfo{
		OperationId:  "op-1",
		RunId:        "run-1",
		Endpoint:     "endpoint",
		Service:      "service",
		Operation:    "operation",
		Status:       enumspb.NEXUS_OPERATION_EXECUTION_STATUS_RUNNING,
		ScheduleTime: timestamppb.New(time.Unix(10, 0)),
	}
	rawListInfo := &nexuspb.NexusOperationExecutionListInfo{
		OperationId: "op-1",
		RunId:       "run-1",
		Status:      enumspb.NEXUS_OPERATION_EXECUTION_STATUS_RUNNING,
	}
	encodedInput := &commonpb.Payload{Metadata: map[string][]byte{converter.MetadataEncoding: []byte("encoded/test")}, Data: []byte(`prefix:{"input":1}`)}
	encodedResult := &commonpb.Payload{Metadata: map[string][]byte{converter.MetadataEncoding: []byte("encoded/test")}, Data: []byte(`prefix:"done"`)}
	codec := &prefixCodec{}
	var gotDescribe *workflowservice.DescribeNexusOperationExecutionRequest
	var gotPoll *workflowservice.PollNexusOperationExecutionRequest
	var gotList *workflowservice.ListNexusOperationExecutionsRequest
	var gotCount *workflowservice.CountNexusOperationExecutionsRequest
	var gotCancel *workflowservice.RequestCancelNexusOperationExecutionRequest
	var gotTerminate *workflowservice.TerminateNexusOperationExecutionRequest
	service := &fakeWorkflowService{
		describe: func(_ context.Context, request *workflowservice.DescribeNexusOperationExecutionRequest) (*workflowservice.DescribeNexusOperationExecutionResponse, error) {
			gotDescribe = request
			return &workflowservice.DescribeNexusOperationExecutionResponse{
				RunId:         "run-1",
				Info:          rawInfo,
				Input:         encodedInput,
				Outcome:       &workflowservice.DescribeNexusOperationExecutionResponse_Result{Result: encodedResult},
				LongPollToken: []byte{4, 5, 6},
			}, nil
		},
		poll: func(_ context.Context, request *workflowservice.PollNexusOperationExecutionRequest) (*workflowservice.PollNexusOperationExecutionResponse, error) {
			gotPoll = request
			return &workflowservice.PollNexusOperationExecutionResponse{
				RunId:          "run-1",
				WaitStage:      enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
				OperationToken: "operation-token",
				Outcome:        &workflowservice.PollNexusOperationExecutionResponse_Result{Result: encodedResult},
			}, nil
		},
		list: func(_ context.Context, request *workflowservice.ListNexusOperationExecutionsRequest) (*workflowservice.ListNexusOperationExecutionsResponse, error) {
			gotList = request
			return &workflowservice.ListNexusOperationExecutionsResponse{Operations: []*nexuspb.NexusOperationExecutionListInfo{rawListInfo}, NextPageToken: []byte{7, 8, 9}}, nil
		},
		count: func(_ context.Context, request *workflowservice.CountNexusOperationExecutionsRequest) (*workflowservice.CountNexusOperationExecutionsResponse, error) {
			gotCount = request
			return &workflowservice.CountNexusOperationExecutionsResponse{
				Count: 2,
				Groups: []*workflowservice.CountNexusOperationExecutionsResponse_AggregationGroup{{
					GroupValues: []*commonpb.Payload{{Metadata: map[string][]byte{converter.MetadataEncoding: []byte(converter.MetadataEncodingJSON)}, Data: []byte(`"Running"`)}},
					Count:       2,
				}},
			}, nil
		},
		cancel: func(_ context.Context, request *workflowservice.RequestCancelNexusOperationExecutionRequest) (*workflowservice.RequestCancelNexusOperationExecutionResponse, error) {
			gotCancel = request
			return &workflowservice.RequestCancelNexusOperationExecutionResponse{}, nil
		},
		terminate: func(_ context.Context, request *workflowservice.TerminateNexusOperationExecutionRequest) (*workflowservice.TerminateNexusOperationExecutionResponse, error) {
			gotTerminate = request
			return &workflowservice.TerminateNexusOperationExecutionResponse{}, nil
		},
	}
	h := newTestHandler(t, service, Options{PayloadCodecs: []converter.PayloadCodec{codec}})

	tests := []struct {
		method string
		path   string
		body   string
		status int
		fields []string
	}{
		{http.MethodGet, "/operations/op-1?runId=run-1&includeInput=true&includeOutcome=true&longPollToken=AQID", "", http.StatusOK, []string{`"operationId":"op-1"`, `"input":{"input":1}`, `"result":"done"`, `"longPollToken":"BAUG"`}},
		{http.MethodGet, "/operations/op-1/poll?runId=run-1&waitStage=Started", "", http.StatusOK, []string{`"waitStage":"NEXUS_OPERATION_WAIT_STAGE_CLOSED"`, `"operationToken":"operation-token"`, `"result":"done"`}},
		{http.MethodGet, "/operations?query=ExecutionStatus%3D%27Running%27&pageSize=25&nextPageToken=AQID", "", http.StatusOK, []string{`"operations":[`, `"nextPageToken":"BwgJ"`}},
		{http.MethodGet, "/operation-count?query=GROUP+BY+ExecutionStatus", "", http.StatusOK, []string{`"count":"2"`, `"groupValues":["Running"]`}},
		{http.MethodPost, "/operations/op-1/cancel?runId=run-1", `{"reason":"stop","requestId":"fixed-cancel-request"}`, http.StatusAccepted, []string{`{}`}},
		{http.MethodPost, "/operations/op-1/terminate?runId=run-1", `{"reason":"kill","requestId":"fixed-terminate-request"}`, http.StatusOK, []string{`{}`}},
	}
	for _, test := range tests {
		t.Run(test.method+" "+test.path, func(t *testing.T) {
			response := serve(h, test.method, test.path, test.body)
			requireStatus(t, test.status, response)
			for _, field := range test.fields {
				require.Contains(t, response.Body.String(), field)
			}
		})
	}

	require.Equal(t, "test-namespace", gotDescribe.Namespace)
	require.Equal(t, "op-1", gotDescribe.OperationId)
	require.Equal(t, "run-1", gotDescribe.RunId)
	require.True(t, gotDescribe.IncludeInput)
	require.True(t, gotDescribe.IncludeOutcome)
	require.Equal(t, []byte{1, 2, 3}, gotDescribe.LongPollToken)

	require.Equal(t, "test-namespace", gotPoll.Namespace)
	require.Equal(t, "op-1", gotPoll.OperationId)
	require.Equal(t, "run-1", gotPoll.RunId)
	require.Equal(t, enumspb.NEXUS_OPERATION_WAIT_STAGE_STARTED, gotPoll.WaitStage)

	require.Equal(t, "test-namespace", gotList.Namespace)
	require.Equal(t, "ExecutionStatus='Running'", gotList.Query)
	require.EqualValues(t, 25, gotList.PageSize)
	require.Equal(t, []byte{1, 2, 3}, gotList.NextPageToken)

	require.Equal(t, "test-namespace", gotCount.Namespace)
	require.Equal(t, "GROUP BY ExecutionStatus", gotCount.Query)

	require.Equal(t, "test-namespace", gotCancel.Namespace)
	require.Equal(t, "op-1", gotCancel.OperationId)
	require.Equal(t, "run-1", gotCancel.RunId)
	require.Equal(t, "fixed-cancel-request", gotCancel.RequestId)
	require.Equal(t, "stop", gotCancel.Reason)

	require.Equal(t, "test-namespace", gotTerminate.Namespace)
	require.Equal(t, "op-1", gotTerminate.OperationId)
	require.Equal(t, "run-1", gotTerminate.RunId)
	require.Equal(t, "fixed-terminate-request", gotTerminate.RequestId)
	require.Equal(t, "kill", gotTerminate.Reason)

	// Describe input, describe result, poll result, and the count aggregation
	// group values. The list fixture carries no payloads.
	require.Equal(t, 4, codec.decoded)
}

func TestInvalidRequestsAreBadRequests(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t, &fakeWorkflowService{}, Options{})
	const startBody = `{"endpoint":"e","service":"s","operation":"o"}`
	tests := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{"malformed body", http.MethodPost, "/operations/op", `{"endpoint":`},
		{"missing endpoint", http.MethodPost, "/operations/op", `{"service":"s","operation":"o"}`},
		{"negative timeout", http.MethodPost, "/operations/op", `{"endpoint":"e","service":"s","operation":"o","scheduleToCloseTimeout":"-5s"}`},
		{"invalid includeInput", http.MethodGet, "/operations/op?includeInput=maybe", ""},
		{"invalid longPollToken", http.MethodGet, "/operations/op?longPollToken=not-base64!", ""},
		{"invalid waitStage", http.MethodGet, "/operations/op/poll?waitStage=Nonsense", ""},
		{"invalid pageSize", http.MethodGet, "/operations?pageSize=huge", ""},
		{"invalid nextPageToken", http.MethodGet, "/operations?nextPageToken=not-base64!", ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			body := test.body
			if test.method == http.MethodPost && body == "" {
				body = startBody
			}
			response := serve(h, test.method, test.path, body)
			requireStatus(t, http.StatusBadRequest, response)
			require.NotEmpty(t, requireLoneMessage(t, response.Body.Bytes()))
		})
	}
}

func TestErrorMappings(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		err        error
		wantStatus int
		// wantMessage is empty for internal errors, which must be hidden.
		wantMessage string
	}{
		{"status", &statusError{statusCode: http.StatusTooManyRequests, Message: "busy"}, http.StatusTooManyRequests, "busy"},
		{"wrapped status", fmt.Errorf("encode request payloads: %w", &statusError{statusCode: http.StatusBadRequest, Message: "bad payload"}), http.StatusBadRequest, "bad payload"},
		{"grpc", status.Error(codes.DeadlineExceeded, "late"), http.StatusGatewayTimeout, "late"},
		{"service", serviceerror.NewNotFound("missing"), http.StatusNotFound, "missing"},
		{"grpc internal", status.Error(codes.Internal, "shard closed"), http.StatusInternalServerError, ""},
		{"unknown", errors.New("broken"), http.StatusInternalServerError, ""},
	}
	errorIDPattern := regexp.MustCompile(`^internal error \(ID: ([0-9a-f-]+)\)$`)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var logs bytes.Buffer
			h := &handler{logger: slog.New(slog.NewTextHandler(&logs, nil))}
			recorder := httptest.NewRecorder()
			h.writeError(recorder, test.err)
			requireStatus(t, test.wantStatus, recorder)
			// Every error, classified or not, is a lone message field.
			message := requireLoneMessage(t, recorder.Body.Bytes())

			if test.wantMessage != "" {
				require.Equal(t, test.wantMessage, message)
				require.Zero(t, logs.Len(), "a classified error must not be logged")
				return
			}
			// The error itself only reaches the log. The caller gets the ID it
			// was logged under and nothing else.
			require.NotContains(t, recorder.Body.String(), test.err.Error(), "body leaked the error")
			require.Contains(t, logs.String(), test.err.Error())
			match := errorIDPattern.FindStringSubmatch(message)
			require.NotNil(t, match, "message = %q, want an error ID", message)
			require.Contains(t, logs.String(), "errorID="+match[1])
		})
	}
}

func TestPollOperationFailurePreservesTemporalFailure(t *testing.T) {
	t.Parallel()
	encodingCodec := &prefixCodec{}
	failureConverter := temporal.NewDefaultFailureConverter(temporal.DefaultFailureConverterOptions{
		DataConverter: converter.NewCodecDataConverter(converter.GetDefaultDataConverter(), encodingCodec),
	})
	protoFailure := failureConverter.ErrorToFailure(&temporal.NexusOperationError{
		Message: "operation failed",
		Cause: temporal.NewApplicationErrorWithOptions("payment failed", "PaymentError", temporal.ApplicationErrorOptions{
			Details: []any{"payment detail"},
			Cause: &nexus.HandlerError{
				Type:    nexus.HandlerErrorTypeUnavailable,
				Message: "handler unavailable",
			},
		}),
	})
	decodingCodec := &prefixCodec{}
	h := newTestHandler(t, &fakeWorkflowService{
		poll: func(context.Context, *workflowservice.PollNexusOperationExecutionRequest) (*workflowservice.PollNexusOperationExecutionResponse, error) {
			return &workflowservice.PollNexusOperationExecutionResponse{Outcome: &workflowservice.PollNexusOperationExecutionResponse_Failure{Failure: protoFailure}}, nil
		},
	}, Options{PayloadCodecs: []converter.PayloadCodec{decodingCodec}})

	response := serve(h, http.MethodGet, "/operations/op/poll", "")
	requireStatus(t, http.StatusFailedDependency, response)

	var failure nexus.Failure
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &failure))
	require.Equal(t, temporalFailureType, failure.Metadata["type"])
	require.Contains(t, failure.Message, "operation failed")

	var temporalFailure failurepb.Failure
	require.NoError(t, protojson.Unmarshal(failure.Details, &temporalFailure))
	require.NotNil(t, temporalFailure.GetNexusOperationExecutionFailureInfo())

	require.NotNil(t, failure.Cause)
	require.Equal(t, "payment failed", failure.Cause.Message)
	require.Equal(t, temporalFailureType, failure.Cause.Metadata["type"])

	var applicationFailure failurepb.Failure
	require.NoError(t, protojson.Unmarshal(failure.Cause.Details, &applicationFailure))
	require.Equal(t, "PaymentError", applicationFailure.GetApplicationFailureInfo().GetType())
	require.Equal(t, `"payment detail"`, string(applicationFailure.GetApplicationFailureInfo().GetDetails().GetPayloads()[0].Data))

	require.NotNil(t, failure.Cause.Cause)
	require.Equal(t, "handler unavailable", failure.Cause.Cause.Message)
	require.Equal(t, "nexus.HandlerError", failure.Cause.Cause.Metadata["type"])

	require.Equal(t, 1, decodingCodec.decoded)
}

func TestPollOperationStartedStageHasNoOutcome(t *testing.T) {
	t.Parallel()
	codec := &prefixCodec{}
	h := newTestHandler(t, &fakeWorkflowService{
		poll: func(context.Context, *workflowservice.PollNexusOperationExecutionRequest) (*workflowservice.PollNexusOperationExecutionResponse, error) {
			return &workflowservice.PollNexusOperationExecutionResponse{
				RunId:          "run-1",
				WaitStage:      enumspb.NEXUS_OPERATION_WAIT_STAGE_STARTED,
				OperationToken: "operation-token",
			}, nil
		},
	}, Options{PayloadCodecs: []converter.PayloadCodec{codec}})

	response := serve(h, http.MethodGet, "/operations/op/poll", "")
	requireStatus(t, http.StatusOK, response)

	var body map[string]any
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	require.Equal(t, "operation-token", body["operationToken"])
	require.Equal(t, "NEXUS_OPERATION_WAIT_STAGE_STARTED", body["waitStage"])
	require.NotContains(t, body, "result")
	require.Equal(t, 0, codec.decoded)
}

// TestConvertGRPCErrorMappings pins the HTTP status the connector returns for
// every gRPC code Temporal can produce. The mapping comes from grpc-gateway, so
// this is the connector's contract restated rather than a rule of its own.
func TestConvertGRPCErrorMappings(t *testing.T) {
	t.Parallel()
	tests := map[codes.Code]int{
		codes.Canceled:           499, // client closed request
		codes.Unknown:            http.StatusInternalServerError,
		codes.InvalidArgument:    http.StatusBadRequest,
		codes.DeadlineExceeded:   http.StatusGatewayTimeout,
		codes.NotFound:           http.StatusNotFound,
		codes.AlreadyExists:      http.StatusConflict,
		codes.PermissionDenied:   http.StatusForbidden,
		codes.ResourceExhausted:  http.StatusTooManyRequests,
		codes.FailedPrecondition: http.StatusBadRequest,
		codes.Aborted:            http.StatusConflict,
		codes.OutOfRange:         http.StatusBadRequest,
		codes.Unimplemented:      http.StatusNotImplemented,
		codes.Internal:           http.StatusInternalServerError,
		codes.Unavailable:        http.StatusServiceUnavailable,
		codes.DataLoss:           http.StatusInternalServerError,
		codes.Unauthenticated:    http.StatusUnauthorized,
	}
	for code, want := range tests {
		t.Run(code.String(), func(t *testing.T) {
			t.Parallel()
			converted := convertGRPCError(status.Error(code, "message"))
			statusErr, ok := converted.(*statusError)
			require.True(t, ok, "convertGRPCError(%q) = %#v, want a status error", code, converted)
			require.Equal(t, want, statusErr.statusCode)
			// The gRPC "rpc error: code = ... desc = " prefix is dropped, so a raw
			// status error reads the same as the equivalent serviceerror.
			require.Equal(t, "message", statusErr.Message)
		})
	}
}

// TestUnmappedGRPCCodeIsInternal pins that an unrecognized code resolves to 500
// and is therefore hidden rather than reported with its message.
func TestUnmappedGRPCCodeIsInternal(t *testing.T) {
	t.Parallel()
	err := status.Error(codes.Code(42), "unmapped")
	converted, ok := convertGRPCError(err).(*statusError)
	require.True(t, ok, "convertGRPCError = %#v, want a status error", converted)
	require.Equal(t, http.StatusInternalServerError, converted.statusCode)

	recorder := httptest.NewRecorder()
	newBareHandler().writeError(recorder, err)
	requireStatus(t, http.StatusInternalServerError, recorder)
	require.NotContains(t, recorder.Body.String(), "unmapped", "body leaked the error")
}

// newTestHandler builds the routed handler around a fake WorkflowService,
// skipping the Temporal connection NewHandler would dial. ClientOptions
// defaults to the namespace the request assertions expect.
func newTestHandler(t *testing.T, service workflowservice.WorkflowServiceClient, options Options) http.Handler {
	t.Helper()
	clientOptions := client.Options{Namespace: "test-namespace"}
	if options.ClientOptions != nil {
		clientOptions = *options.ClientOptions
	}
	return newHandler(service, clientOptions, options)
}

// newBareHandler builds a handler without routing, for tests that exercise a
// single helper rather than the HTTP API.
func newBareHandler(codecs ...converter.PayloadCodec) *handler {
	return &handler{logger: slog.New(slog.DiscardHandler), codecs: codecs}
}

func serve(h http.Handler, method, target, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, target, bytes.NewBufferString(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	h.ServeHTTP(response, request)
	return response
}

// requireStatus asserts the response status, reporting the body on failure.
func requireStatus(t *testing.T, want int, response *httptest.ResponseRecorder) {
	t.Helper()
	require.Equal(t, want, response.Code, "body = %s", response.Body.String())
}

// requireLoneMessage asserts the body is a JSON object holding nothing but a
// message field, and returns that message.
func requireLoneMessage(t *testing.T, body []byte) string {
	t.Helper()
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(body, &decoded))
	require.Len(t, decoded, 1, "body = %s, want a lone message field", body)
	message, ok := decoded["message"].(string)
	require.True(t, ok, "body = %s, want a string message field", body)
	return message
}

type fakeWorkflowService struct {
	workflowservice.WorkflowServiceClient
	start     func(context.Context, *workflowservice.StartNexusOperationExecutionRequest) (*workflowservice.StartNexusOperationExecutionResponse, error)
	describe  func(context.Context, *workflowservice.DescribeNexusOperationExecutionRequest) (*workflowservice.DescribeNexusOperationExecutionResponse, error)
	poll      func(context.Context, *workflowservice.PollNexusOperationExecutionRequest) (*workflowservice.PollNexusOperationExecutionResponse, error)
	list      func(context.Context, *workflowservice.ListNexusOperationExecutionsRequest) (*workflowservice.ListNexusOperationExecutionsResponse, error)
	count     func(context.Context, *workflowservice.CountNexusOperationExecutionsRequest) (*workflowservice.CountNexusOperationExecutionsResponse, error)
	cancel    func(context.Context, *workflowservice.RequestCancelNexusOperationExecutionRequest) (*workflowservice.RequestCancelNexusOperationExecutionResponse, error)
	terminate func(context.Context, *workflowservice.TerminateNexusOperationExecutionRequest) (*workflowservice.TerminateNexusOperationExecutionResponse, error)
}

func (f *fakeWorkflowService) StartNexusOperationExecution(ctx context.Context, request *workflowservice.StartNexusOperationExecutionRequest, _ ...grpc.CallOption) (*workflowservice.StartNexusOperationExecutionResponse, error) {
	return f.start(ctx, request)
}

func (f *fakeWorkflowService) DescribeNexusOperationExecution(ctx context.Context, request *workflowservice.DescribeNexusOperationExecutionRequest, _ ...grpc.CallOption) (*workflowservice.DescribeNexusOperationExecutionResponse, error) {
	return f.describe(ctx, request)
}

func (f *fakeWorkflowService) PollNexusOperationExecution(ctx context.Context, request *workflowservice.PollNexusOperationExecutionRequest, _ ...grpc.CallOption) (*workflowservice.PollNexusOperationExecutionResponse, error) {
	return f.poll(ctx, request)
}

func (f *fakeWorkflowService) ListNexusOperationExecutions(ctx context.Context, request *workflowservice.ListNexusOperationExecutionsRequest, _ ...grpc.CallOption) (*workflowservice.ListNexusOperationExecutionsResponse, error) {
	return f.list(ctx, request)
}

func (f *fakeWorkflowService) CountNexusOperationExecutions(ctx context.Context, request *workflowservice.CountNexusOperationExecutionsRequest, _ ...grpc.CallOption) (*workflowservice.CountNexusOperationExecutionsResponse, error) {
	return f.count(ctx, request)
}

func (f *fakeWorkflowService) RequestCancelNexusOperationExecution(ctx context.Context, request *workflowservice.RequestCancelNexusOperationExecutionRequest, _ ...grpc.CallOption) (*workflowservice.RequestCancelNexusOperationExecutionResponse, error) {
	return f.cancel(ctx, request)
}

func (f *fakeWorkflowService) TerminateNexusOperationExecution(ctx context.Context, request *workflowservice.TerminateNexusOperationExecutionRequest, _ ...grpc.CallOption) (*workflowservice.TerminateNexusOperationExecutionResponse, error) {
	return f.terminate(ctx, request)
}

type prefixCodec struct {
	encoded int
	decoded int
}

func (c *prefixCodec) Encode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	c.encoded++
	payload := payloads[0]
	return []*commonpb.Payload{{
		Metadata: map[string][]byte{converter.MetadataEncoding: []byte("encoded/test")},
		Data:     append([]byte("prefix:"), payload.Data...),
	}}, nil
}

func (c *prefixCodec) Decode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	c.decoded++
	payload := payloads[0]
	return []*commonpb.Payload{{
		Metadata: map[string][]byte{converter.MetadataEncoding: []byte(converter.MetadataEncodingJSON)},
		Data:     bytes.TrimPrefix(payload.Data, []byte("prefix:")),
	}}, nil
}

var _ converter.PayloadCodec = (*prefixCodec)(nil)
