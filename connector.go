// Package nexushttp provides an HTTP handler for invoking Temporal Standalone
// Nexus Operations through a Temporal Go SDK client it dials from
// caller-supplied client options.
package nexushttp

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/google/uuid"
	"go.temporal.io/api/temporalproto"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

const defaultMaxRequestBodySize int64 = 4 << 20

// RequestBodySizeLimit is a closed set of request body size configurations.
type RequestBodySizeLimit interface {
	requestBodySizeLimit()
}

// MaxRequestBodySize is an enforced request body size limit in bytes.
type MaxRequestBodySize int64

func (MaxRequestBodySize) requestBodySizeLimit() {}

// MaxRequestBodySizeNotEnforced disables request body size enforcement.
type MaxRequestBodySizeNotEnforced struct{}

func (MaxRequestBodySizeNotEnforced) requestBodySizeLimit() {}

// Options configures a Nexus HTTP handler.
type Options struct {
	// ClientOptions configures the Temporal client [NewHandler] dials. When it
	// is nil, NewHandler loads the options from Temporal environment
	// configuration with [envconfig.LoadDefaultClientOptions]: the TEMPORAL_*
	// environment variables layered over the temporal.toml profile they select.
	//
	// The connector calls Temporal through the client's low-level
	// WorkflowService, so only part of this struct reaches its requests:
	//
	//   - HostPort, Credentials, ConnectionOptions, and HeadersProvider open the
	//     connection and authenticate every call the connector makes. HostPort
	//     defaults to [client.DefaultHostPort].
	//   - Namespace is written to every WorkflowService request. It defaults to
	//     [client.DefaultNamespace], matching the SDK client's own default.
	//   - Identity is written to start, cancel, and terminate requests whose
	//     body omits an identity. Unlike the SDK client, the connector does not
	//     synthesize an identity when this is empty; it leaves the field unset.
	//   - Logger and MetricsHandler, with DisableErrorCodeMetricTags, observe
	//     the connector's calls, because the SDK installs both as
	//     connection-level gRPC interceptors. Logger is unrelated to
	//     Options.Logger, which records the connector's own HTTP-layer errors.
	//   - Plugins are honored by client creation. A plugin that rewrites
	//     Namespace or Identity does not change what the connector sends: the
	//     connector reads both from these options as given.
	//
	// Every other field only affects the SDK's higher-level client APIs, which
	// the connector never calls, and is therefore ignored: DataConverter,
	// FailureConverter, ContextPropagators, Interceptors, ExternalStorage,
	// PayloadLimits, and the worker-only WorkerHeartbeatInterval, SdkName, and
	// SdkVersion. Use PayloadCodecs for payload encoding. TEMPORAL_CODEC_*
	// environment configuration is ignored for the same reason: it only
	// configures a remote data converter.
	ClientOptions *client.Options

	// PayloadCodecs are applied by the connector when reading and writing
	// operation input, result, and failure payloads. Direct WorkflowService
	// calls bypass the client's data converter.
	PayloadCodecs []converter.PayloadCodec

	// MaxRequestBodySize accepts a MaxRequestBodySize byte limit or
	// MaxRequestBodySizeNotEnforced. It defaults to 4 MiB when unset. A
	// MaxRequestBodySize of zero rejects non-empty request bodies.
	MaxRequestBodySize RequestBodySizeLimit

	// Logger records internal server errors, each under a generated ID that is
	// also the only detail the caller receives. It defaults to slog.Default().
	Logger *slog.Logger
}

type handler struct {
	service   workflowservice.WorkflowServiceClient
	namespace string
	identity  string
	codecs    []converter.PayloadCodec
	logger    *slog.Logger
}

// Handler serves the complete Standalone Nexus Operation API over HTTP and owns
// the Temporal connection it dialed. Close it to release that connection.
type Handler struct {
	mux http.Handler
	// close shuts down the dialed connection. The SDK client it came from is
	// not retained: the connector only needs the WorkflowService stub bound to
	// that connection, which it captured in mux.
	close func()
}

// NewHandler dials Temporal and returns a handler for the complete Standalone
// Nexus Operation API. The context bounds the connection attempt only; served
// requests use their own contexts. Callers own the returned handler's
// connection and must call [Handler.Close] when they are done with it.
func NewHandler(ctx context.Context, options Options) (*Handler, error) {
	clientOptions, err := resolveClientOptions(options)
	if err != nil {
		return nil, err
	}
	temporalClient, err := client.DialContext(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("dial Temporal: %w", err)
	}
	return &Handler{
		mux:   newHandler(temporalClient.WorkflowService(), clientOptions, options),
		close: temporalClient.Close,
	}, nil
}

// ServeHTTP implements [http.Handler].
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

// Close closes the Temporal connection [NewHandler] dialed. It does not wait
// for in-flight HTTP requests; shut down the HTTP server first.
func (h *Handler) Close() {
	h.close()
}

// resolveClientOptions returns the client options to dial with, loading them
// from Temporal environment configuration when the caller supplied none.
func resolveClientOptions(options Options) (client.Options, error) {
	if options.ClientOptions != nil {
		return *options.ClientOptions, nil
	}
	clientOptions, err := envconfig.LoadDefaultClientOptions()
	if err != nil {
		return client.Options{}, fmt.Errorf("load Temporal environment configuration: %w", err)
	}
	return clientOptions, nil
}

// newHandler builds the routed HTTP handler around an already-connected
// WorkflowService client.
func newHandler(service workflowservice.WorkflowServiceClient, clientOptions client.Options, options Options) http.Handler {
	h := &handler{
		service:   service,
		namespace: cmp.Or(clientOptions.Namespace, client.DefaultNamespace),
		identity:  clientOptions.Identity,
		codecs:    append([]converter.PayloadCodec(nil), options.PayloadCodecs...),
		logger:    cmp.Or(options.Logger, slog.Default()),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /operations/{id}", h.startOperation)
	mux.HandleFunc("GET /operations/{id}", h.describeOperation)
	mux.HandleFunc("GET /operations/{id}/poll", h.pollOperation)
	mux.HandleFunc("GET /operations", h.listOperations)
	mux.HandleFunc("GET /operation-count", h.countOperations)
	mux.HandleFunc("POST /operations/{id}/cancel", h.cancelOperation)
	mux.HandleFunc("POST /operations/{id}/terminate", h.terminateOperation)

	switch value := options.MaxRequestBodySize.(type) {
	case nil:
		return http.MaxBytesHandler(mux, defaultMaxRequestBodySize)
	case MaxRequestBodySize:
		return http.MaxBytesHandler(mux, int64(value))
	default:
		return mux
	}
}

func (h *handler) startOperation(w http.ResponseWriter, r *http.Request) {
	query := newRequestQuery(r)
	request := &workflowservice.StartNexusOperationExecutionRequest{}
	if err := decodeProtoBody(r.Body, query, request, false); err != nil {
		h.writeError(w, err)
		return
	}
	request.OperationId = r.PathValue("id")
	if err := validateStartRequest(request); err != nil {
		h.writeError(w, err)
		return
	}

	if err := h.encodeStartRequest(r.Context(), request); err != nil {
		h.writeError(w, fmt.Errorf("encode request payloads: %w", err))
		return
	}
	request.Namespace = h.namespace
	request.Identity = cmp.Or(request.Identity, h.identity)
	if request.RequestId == "" {
		request.RequestId = uuid.NewString()
	}
	response, err := h.service.StartNexusOperationExecution(r.Context(), request)
	if err != nil {
		h.writeError(w, err)
		return
	}
	h.writeProto(w, query, http.StatusCreated, &workflowservice.StartNexusOperationExecutionResponse{
		// Started is intentionally omitted. The connector's public start response
		// only promises the run ID.
		RunId: response.GetRunId(),
	})
}

func validateStartRequest(request *workflowservice.StartNexusOperationExecutionRequest) error {
	if request.Endpoint == "" || request.Service == "" || request.Operation == "" {
		return newStatusErrorf(http.StatusBadRequest, "endpoint, service, and operation are required")
	}
	if request.OperationId == "" {
		return newStatusErrorf(http.StatusBadRequest, "operation ID is required")
	}
	for _, timeout := range []struct {
		name  string
		value *durationpb.Duration
	}{
		{"scheduleToCloseTimeout", request.ScheduleToCloseTimeout},
		{"scheduleToStartTimeout", request.ScheduleToStartTimeout},
		{"startToCloseTimeout", request.StartToCloseTimeout},
	} {
		if err := validateDuration(timeout.name, timeout.value); err != nil {
			return err
		}
	}
	return nil
}

func validateDuration(name string, value *durationpb.Duration) error {
	if value == nil {
		return nil
	}
	if err := value.CheckValid(); err != nil {
		return newStatusErrorf(http.StatusBadRequest, "invalid %s: %v", name, err)
	}
	if value.AsDuration() < 0 {
		return newStatusErrorf(http.StatusBadRequest, "invalid %s: duration must not be negative", name)
	}
	return nil
}

func (h *handler) describeOperation(w http.ResponseWriter, r *http.Request) {
	query := newRequestQuery(r)
	includeInput, err := query.optBool("includeInput")
	if err != nil {
		h.writeError(w, err)
		return
	}
	includeOutcome, err := query.optBool("includeOutcome")
	if err != nil {
		h.writeError(w, err)
		return
	}
	longPollToken, err := query.optBytes("longPollToken")
	if err != nil {
		h.writeError(w, err)
		return
	}
	response, err := h.service.DescribeNexusOperationExecution(r.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
		Namespace:      h.namespace,
		OperationId:    r.PathValue("id"),
		RunId:          query.optString("runId"),
		IncludeInput:   includeInput,
		IncludeOutcome: includeOutcome,
		LongPollToken:  longPollToken,
	})
	if err != nil {
		h.writeError(w, err)
		return
	}
	if response == nil {
		h.writeError(w, errors.New("empty describe response from Temporal"))
		return
	}
	if err := h.decodeInbound(r.Context(), response); err != nil {
		h.writeError(w, fmt.Errorf("decode describe response: %w", err))
		return
	}
	h.writeProto(w, query, http.StatusOK, response)
}

func (h *handler) pollOperation(w http.ResponseWriter, r *http.Request) {
	query := newRequestQuery(r)
	waitStage, err := query.optWaitStage("waitStage")
	if err != nil {
		h.writeError(w, err)
		return
	}
	response, err := h.service.PollNexusOperationExecution(r.Context(), &workflowservice.PollNexusOperationExecutionRequest{
		Namespace:   h.namespace,
		OperationId: r.PathValue("id"),
		RunId:       query.optString("runId"),
		WaitStage:   waitStage,
	})
	if err != nil {
		h.writeError(w, err)
		return
	}
	if err := h.decodeInbound(r.Context(), response); err != nil {
		h.writeError(w, fmt.Errorf("decode poll response: %w", err))
		return
	}
	if failure := response.GetFailure(); failure != nil {
		// decodeInbound above decoded the failure in place, so it is converted
		// as-is.
		converted, convertErr := temporalFailureToNexusFailureInPlace(failure)
		if convertErr != nil {
			h.writeError(w, fmt.Errorf("convert operation failure: %w", convertErr))
			return
		}
		h.writeJSON(w, http.StatusFailedDependency, &converted)
		return
	}
	h.writeProto(w, query, http.StatusOK, response)
}

func (h *handler) listOperations(w http.ResponseWriter, r *http.Request) {
	query := newRequestQuery(r)
	pageSize, err := query.optInt32("pageSize")
	if err != nil {
		h.writeError(w, err)
		return
	}
	nextPageToken, err := query.optBytes("nextPageToken")
	if err != nil {
		h.writeError(w, err)
		return
	}
	response, err := h.service.ListNexusOperationExecutions(r.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
		Namespace:     h.namespace,
		PageSize:      pageSize,
		NextPageToken: nextPageToken,
		Query:         query.optString("query"),
	})
	if err != nil {
		h.writeError(w, err)
		return
	}
	if err := h.decodeInbound(r.Context(), response); err != nil {
		h.writeError(w, fmt.Errorf("decode list response: %w", err))
		return
	}
	h.writeProto(w, query, http.StatusOK, response)
}

func (h *handler) countOperations(w http.ResponseWriter, r *http.Request) {
	query := newRequestQuery(r)
	response, err := h.service.CountNexusOperationExecutions(r.Context(), &workflowservice.CountNexusOperationExecutionsRequest{
		Namespace: h.namespace,
		Query:     query.optString("query"),
	})
	if err != nil {
		h.writeError(w, err)
		return
	}
	if err := h.decodeInbound(r.Context(), response); err != nil {
		h.writeError(w, fmt.Errorf("decode count response: %w", err))
		return
	}
	h.writeProto(w, query, http.StatusOK, response)
}

func (h *handler) cancelOperation(w http.ResponseWriter, r *http.Request) {
	query := newRequestQuery(r)
	request := &workflowservice.RequestCancelNexusOperationExecutionRequest{}
	if err := decodeProtoBody(r.Body, query, request, true); err != nil {
		h.writeError(w, err)
		return
	}
	request.Namespace = h.namespace
	request.OperationId = r.PathValue("id")
	request.RunId = cmp.Or(request.RunId, query.optString("runId"))
	request.Identity = cmp.Or(request.Identity, h.identity)
	if request.RequestId == "" {
		request.RequestId = uuid.NewString()
	}
	if err := h.encodeOutbound(r.Context(), request); err != nil {
		h.writeError(w, fmt.Errorf("encode request payloads: %w", err))
		return
	}
	response, err := h.service.RequestCancelNexusOperationExecution(r.Context(), request)
	if err != nil {
		h.writeError(w, err)
		return
	}
	if err := h.decodeInbound(r.Context(), response); err != nil {
		h.writeError(w, fmt.Errorf("decode cancel response: %w", err))
		return
	}
	h.writeProto(w, query, http.StatusAccepted, response)
}

func (h *handler) terminateOperation(w http.ResponseWriter, r *http.Request) {
	query := newRequestQuery(r)
	request := &workflowservice.TerminateNexusOperationExecutionRequest{}
	if err := decodeProtoBody(r.Body, query, request, true); err != nil {
		h.writeError(w, err)
		return
	}
	request.Namespace = h.namespace
	request.OperationId = r.PathValue("id")
	request.RunId = cmp.Or(request.RunId, query.optString("runId"))
	request.Identity = cmp.Or(request.Identity, h.identity)
	if request.RequestId == "" {
		request.RequestId = uuid.NewString()
	}
	if err := h.encodeOutbound(r.Context(), request); err != nil {
		h.writeError(w, fmt.Errorf("encode request payloads: %w", err))
		return
	}
	response, err := h.service.TerminateNexusOperationExecution(r.Context(), request)
	if err != nil {
		h.writeError(w, err)
		return
	}
	if err := h.decodeInbound(r.Context(), response); err != nil {
		h.writeError(w, fmt.Errorf("decode terminate response: %w", err))
		return
	}
	h.writeProto(w, query, http.StatusOK, response)
}

func decodeProtoBody(body io.Reader, query requestQuery, message proto.Message, allowEmpty bool) error {
	data, err := io.ReadAll(body)
	if err != nil {
		// Read failures are treated as client errors: unless the size limit is disabled, the handler is
		// wrapped in http.MaxBytesHandler (see newHandler), so the most likely cause is an oversized body,
		// which is non-retryable. The other realistic cause is an aborted request or an expired read
		// deadline, in which case the connection is already gone and this classification only affects how
		// the error is logged.
		return newStatusErrorf(http.StatusBadRequest, "%v", err)
	}
	if len(data) == 0 && allowEmpty {
		return nil
	}
	unmarshalOptions := temporalproto.CustomJSONUnmarshalOptions{Metadata: query.payloadMetadata()}
	if err := unmarshalOptions.Unmarshal(data, message); err != nil {
		return newStatusErrorf(http.StatusBadRequest, "%v", err)
	}
	return nil
}

func (h *handler) writeProto(w http.ResponseWriter, query requestQuery, statusCode int, message proto.Message) {
	data, err := query.marshalOptions().Marshal(message)
	if err != nil {
		h.writeError(w, fmt.Errorf("marshal response: %w", err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if _, err := w.Write(append(data, '\n')); err != nil {
		h.logInternalError(fmt.Errorf("write response: %w", err))
	}
}
