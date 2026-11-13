package nexushttp

import (
	"context"
	"fmt"
	"path"
	"slices"

	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/proxy"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type (
	// serializationContextKey marks a visit's context with the serialization
	// context of the operation whose payloads are beneath the message being
	// visited.
	serializationContextKey struct{}

	// rpcKey marks a visit's context with the name of the RPC whose payloads are
	// visited, for codec errors to name.
	rpcKey struct{}
)

// NewPayloadCodecInterceptor returns a gRPC client interceptor that applies
// codecs to the payloads of every request and response on the connection it is
// installed on, as the connector does on a connection it dials. Install it on
// the connection beneath a WorkflowService client passed with
// [ConnectionOptionsFromWorkflowServiceClient] to encode that client's payloads
// the same way.
//
// Install it last, as the innermost interceptor, on a connection only the
// connector uses, or in place of a client's codec rather than beside it: on a
// connection whose client also encodes payloads, such as an SDK client with a
// codec data converter, every payload would be encoded twice. Installed outside
// other interceptors, it hands them status errors where they may have expected
// the typed errors of go.temporal.io/api/serviceerror.
//
// Requests are encoded last codec first and responses decoded first codec
// first, after which common failure attributes are restored. Search attributes
// are left unencoded, since the server indexes them.
//
// Codecs implementing converter.PayloadCodecWithSerializationContext receive a
// converter.NexusSerializationContext naming the operation's endpoint, service,
// and operation for the payloads of a start request, a describe response, and a
// poll response that propagates one. Every other payload, including user
// metadata, as in the SDK, is encoded and decoded without context.
//
// The request is encoded in a copy, so an interceptor outside this one that
// sends the same request again, such as the SDK's gzip fallback, does not
// encode it twice; a large start input is briefly held twice. Codec work runs
// inside the call, so the SDK's per-call metrics count it as call latency, and
// a codec error as a failed call. With no codecs, it returns an interceptor
// that only invokes the call.
func NewPayloadCodecInterceptor(codecs []converter.PayloadCodec) (grpc.UnaryClientInterceptor, error) {
	if len(codecs) == 0 {
		return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			return invoker(ctx, method, req, reply, cc, opts...)
		}, nil
	}
	codecs = slices.Clone(codecs)
	// ContextHook is experimental in go.temporal.io/api. The serialization
	// context tests pin what it provides here, so a change to it fails them.
	payloads, err := proxy.NewPayloadVisitorInterceptor(proxy.PayloadVisitorInterceptorOptions{
		Outbound: &proxy.VisitPayloadsOptions{
			Visitor:              encodeVisitor(codecs),
			SkipSearchAttributes: true,
			ContextHook:          markSerializationContext,
		},
		Inbound: &proxy.VisitPayloadsOptions{
			Visitor:              decodeVisitor(codecs),
			SkipSearchAttributes: true,
			ContextHook:          markSerializationContext,
		},
	})
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if message, ok := req.(proto.Message); ok {
			req = proto.Clone(message)
		}
		ctx = context.WithValue(ctx, rpcKey{}, path.Base(method))
		if err := payloads(ctx, method, req, reply, cc, invoker, opts...); err != nil {
			return err
		}
		if message, ok := reply.(proto.Message); ok {
			return restoreFailureAttributes(ctx, message)
		}
		return nil
	}, nil
}

// encodeVisitor applies codecs, last codec first, to the payloads it visits.
func encodeVisitor(codecs []converter.PayloadCodec) func(*proxy.VisitPayloadsContext, []*commonpb.Payload) ([]*commonpb.Payload, error) {
	return func(visit *proxy.VisitPayloadsContext, payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
		chain := codecsFor(visit, codecs)
		var err error
		for i := len(chain) - 1; i >= 0; i-- {
			if payloads, err = chain[i].Encode(payloads); err != nil {
				return nil, fmt.Errorf("encode %s payloads: %w", visit.Value(rpcKey{}), err)
			}
			if err := checkSinglePayload(visit, payloads); err != nil {
				return nil, err
			}
		}
		return payloads, nil
	}
}

// decodeVisitor applies codecs, first codec first, to the payloads it visits.
func decodeVisitor(codecs []converter.PayloadCodec) func(*proxy.VisitPayloadsContext, []*commonpb.Payload) ([]*commonpb.Payload, error) {
	return func(visit *proxy.VisitPayloadsContext, payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
		var err error
		for _, codec := range codecsFor(visit, codecs) {
			if payloads, err = codec.Decode(payloads); err != nil {
				return nil, fmt.Errorf("decode %s payloads: %w", visit.Value(rpcKey{}), err)
			}
		}
		if err := checkSinglePayload(visit, payloads); err != nil {
			return nil, err
		}
		return payloads, nil
	}
}

// restoreFailureAttributes restores the common attributes of every failure in
// message, after its payloads are decoded. That is the order in which the SDK
// chains its two interceptors, so an encoded attribute payload is decoded before
// it is read.
func restoreFailureAttributes(ctx context.Context, message proto.Message) error {
	return proxy.VisitFailures(ctx, message, proxy.VisitFailuresOptions{
		Visitor: func(_ *proxy.VisitFailuresContext, failure *failurepb.Failure) error {
			converter.DecodeCommonFailureAttributes(converter.GetDefaultDataConverter(), failure)
			failure.EncodedAttributes = nil
			return nil
		},
	})
}

// markSerializationContext records, for the payloads beneath message, the
// serialization context of the operation message describes, if it describes
// one.
func markSerializationContext(ctx context.Context, message proto.Message) (context.Context, error) {
	if serializationContext := serializationContextOf(message); serializationContext != nil {
		ctx = context.WithValue(ctx, serializationContextKey{}, serializationContext)
	}
	return ctx, nil
}

// serializationContextOf returns the serialization context for the payloads
// beneath message: a start request names its operation, a describe response
// its info, and a poll response the context the server propagated. Servers
// that propagate none leave the poll response's payloads without one.
func serializationContextOf(message proto.Message) converter.SerializationContext {
	switch m := message.(type) {
	case *workflowservice.StartNexusOperationExecutionRequest:
		return nexusSerializationContext(m.GetEndpoint(), m.GetService(), m.GetOperation())
	case *workflowservice.DescribeNexusOperationExecutionResponse:
		info := m.GetInfo()
		return nexusSerializationContext(info.GetEndpoint(), info.GetService(), info.GetOperation())
	case *workflowservice.PollNexusOperationExecutionResponse:
		propagated := m.GetPropagatedNexusSerializationContext()
		return nexusSerializationContext(propagated.GetEndpoint(), propagated.GetService(), propagated.GetOperation())
	default:
		return nil
	}
}

// nexusSerializationContext returns the serialization context for the payloads
// of the operation identified by endpoint, service, and operation, or nil when
// none of them is known.
func nexusSerializationContext(endpoint, service, operation string) converter.SerializationContext {
	if endpoint == "" && service == "" && operation == "" {
		return nil
	}
	return converter.NexusSerializationContext{Endpoint: endpoint, Service: service, Operation: operation}
}

// codecsFor returns codecs as they apply to the payloads being visited: with
// the serialization context recorded for them passed to every codec that
// accepts one, or as they are when there is none. User metadata belongs to the
// execution rather than to the operation's payloads, so, as in the SDK, it is
// encoded without serialization context.
func codecsFor(visit *proxy.VisitPayloadsContext, codecs []converter.PayloadCodec) []converter.PayloadCodec {
	if _, ok := visit.Parent.(*sdkpb.UserMetadata); ok {
		return codecs
	}
	serializationContext, _ := visit.Value(serializationContextKey{}).(converter.SerializationContext)
	if serializationContext == nil {
		return codecs
	}
	contextual := make([]converter.PayloadCodec, len(codecs))
	for i, codec := range codecs {
		if c, ok := codec.(converter.PayloadCodecWithSerializationContext); ok {
			codec = c.WithSerializationContext(serializationContext)
		}
		contextual[i] = codec
	}
	return contextual
}

// materializeNullInput sets an absent operation input to the null payload it is
// sent as, so that codecs encode it like any other input: the payload visitor
// only reaches payloads that are present.
func materializeNullInput(request *workflowservice.StartNexusOperationExecutionRequest) {
	if request.Input == nil {
		request.Input = &commonpb.Payload{
			Metadata: map[string][]byte{converter.MetadataEncoding: []byte(converter.MetadataEncodingNil)},
		}
	}
}

// checkSinglePayload enforces the contract for fields holding exactly one
// payload. The visitor rejects a wrong payload count on its own, but not a nil
// payload.
func checkSinglePayload(visit *proxy.VisitPayloadsContext, payloads []*commonpb.Payload) error {
	if !visit.SinglePayloadRequired {
		return nil
	}
	if len(payloads) != 1 || payloads[0] == nil {
		return fmt.Errorf("codec returned %d payloads, expected one non-nil payload", len(payloads))
	}
	return nil
}
