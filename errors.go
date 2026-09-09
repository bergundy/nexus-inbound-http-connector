package nexushttp

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/nexus-rpc/sdk-go/nexus"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

var temporalFailureType = string((&failurepb.Failure{}).ProtoReflect().Descriptor().FullName())

type serializedHandlerError struct {
	Type              string `json:"type,omitempty"`
	RetryableOverride *bool  `json:"retryableOverride,omitempty"`
}

// statusError is an error the connector reports to the caller, as an HTTP
// status and a JSON body holding its message. Errors reach a caller only in
// this form; anything else is internal. The status is unexported so that the
// set of errors a caller can see stays confined to this package.
type statusError struct {
	statusCode int
	Message    string `json:"message"`
}

func (e *statusError) Error() string { return e.Message }

func newStatusErrorf(statusCode int, format string, args ...any) *statusError {
	return &statusError{statusCode: statusCode, Message: fmt.Sprintf(format, args...)}
}

// writeError renders err to the response. Errors the connector classified as
// something other than internal — its own status errors, and the gRPC statuses
// convertGRPCError maps onto them — are reported with their message, which is
// part of the API the connector exposes. Everything else is an internal fault,
// whether it arrived unclassified or as a status error that resolved to 500: it
// is logged under a generated ID and reported as a bare 500 carrying nothing
// but that ID, so that no implementation detail reaches the caller.
func (h *handler) writeError(w http.ResponseWriter, err error) {
	if statusErr, ok := errors.AsType[*statusError](convertGRPCError(err)); ok &&
		statusErr.statusCode != http.StatusInternalServerError {
		h.writeJSON(w, statusErr.statusCode, statusErr)
		return
	}
	errorID := h.logInternalError(err)
	h.writeJSON(w, http.StatusInternalServerError, &statusError{
		Message: fmt.Sprintf("internal error (ID: %s)", errorID),
	})
}

// logInternalError logs an error that is never rendered to the caller and
// returns the ID it was logged under, so that a response can point at it.
func (h *handler) logInternalError(err error) string {
	errorID := uuid.NewString()
	h.logger.Error("internal error", "errorID", errorID, "error", err)
	return errorID
}

func retryBehaviorOverride(behavior enumspb.NexusHandlerErrorRetryBehavior) *bool {
	switch behavior {
	case enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE:
		return new(true)
	case enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE:
		return new(false)
	default:
		return nil
	}
}

// temporalFailureToNexusFailureInPlace converts a Temporal API failure into a
// Nexus failure. The caller exclusively owns the failure produced by the SDK
// converter, so mutating it temporarily while marshaling is safe.
func temporalFailureToNexusFailureInPlace(failure *failurepb.Failure) (nexus.Failure, error) {
	var cause *nexus.Failure
	if failure.GetCause() != nil {
		converted, err := temporalFailureToNexusFailureInPlace(failure.GetCause())
		if err != nil {
			return nexus.Failure{}, err
		}
		cause = &converted
	}

	if info := failure.GetNexusHandlerFailureInfo(); info != nil {
		encoded, err := json.Marshal(serializedHandlerError{
			Type:              info.GetType(),
			RetryableOverride: retryBehaviorOverride(info.GetRetryBehavior()),
		})
		if err != nil {
			return nexus.Failure{}, err
		}
		return nexus.Failure{
			Message:    failure.GetMessage(),
			StackTrace: failure.GetStackTrace(),
			Metadata:   map[string]string{"type": "nexus.HandlerError"},
			Details:    encoded,
			Cause:      cause,
		}, nil
	}

	message := failure.Message
	failure.Message = ""
	stackTrace := failure.StackTrace
	failure.StackTrace = ""
	details, err := protojson.Marshal(failure)
	failure.Message = message
	failure.StackTrace = stackTrace
	if err != nil {
		return nexus.Failure{}, err
	}
	return nexus.Failure{
		Message:    message,
		StackTrace: stackTrace,
		Metadata:   map[string]string{"type": temporalFailureType},
		Details:    details,
		Cause:      cause,
	}, nil
}

// convertGRPCError converts service and gRPC status errors into status errors.
//
// The code is mapped by grpc-gateway's [runtime.HTTPStatusFromCode], so the
// connector's statuses match the convention other gRPC-to-HTTP gateways follow,
// including for codes it does not recognize, which that function maps to 500.
// Codes that map to 500 are reported as internal errors.
//
// The status message is used rather than the error string, so that a raw status
// error reads the same as the equivalent serviceerror instead of carrying gRPC's
// "rpc error: code = ..." prefix.
func convertGRPCError(err error) error {
	grpcStatus, ok := rpcStatus(err)
	if !ok {
		return err
	}
	if grpcStatus.Code() == codes.OK {
		return nil
	}
	return &statusError{
		statusCode: runtime.HTTPStatusFromCode(grpcStatus.Code()),
		Message:    grpcStatus.Message(),
	}
}

func rpcStatus(err error) (*status.Status, bool) {
	if serviceErr, ok := err.(serviceerror.ServiceError); ok {
		return serviceErr.Status(), true
	}
	return status.FromError(err)
}

func (h *handler) writeJSON(w http.ResponseWriter, statusCode int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		h.logInternalError(err)
	}
}
