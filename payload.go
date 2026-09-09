package nexushttp

import (
	"context"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/proxy"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
)

// encodeOutbound applies the payload codecs, last codec first, to every payload
// embedded in an outbound message. It is a no-op for messages that carry none,
// so every request can be sent through it.
//
// Search attributes are deliberately left unencoded: the server indexes them,
// so it has to be able to read them.
func (h *handler) encodeOutbound(ctx context.Context, message proto.Message) error {
	if len(h.codecs) == 0 {
		return nil
	}
	return proxy.VisitPayloads(ctx, message, proxy.VisitPayloadsOptions{
		Visitor: func(visit *proxy.VisitPayloadsContext, payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
			var err error
			for i := len(h.codecs) - 1; i >= 0; i-- {
				if payloads, err = h.codecs[i].Encode(payloads); err != nil {
					return nil, err
				}
				if err := checkSinglePayload(visit, payloads); err != nil {
					return nil, err
				}
			}
			return payloads, nil
		},
		SkipSearchAttributes: true,
	})
}

// decodeInbound reverses encodeOutbound for an inbound message and then
// restores common failure attributes. That is the order in which the SDK chains
// its two interceptors, so an encoded attribute payload is decoded before it is
// read. Attributes are only worth restoring when a codec encoded them, so both
// passes are skipped when none is configured.
//
// Both traversals come from go.temporal.io/api/proxy, which is generated from
// the protos, so payload and failure fields added upstream are handled without
// a change here.
func (h *handler) decodeInbound(ctx context.Context, message proto.Message) error {
	if len(h.codecs) == 0 {
		return nil
	}
	err := proxy.VisitPayloads(ctx, message, proxy.VisitPayloadsOptions{
		Visitor: func(visit *proxy.VisitPayloadsContext, payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
			decoded, err := h.decodePayloads(payloads)
			if err != nil {
				return nil, err
			}
			if err := checkSinglePayload(visit, decoded); err != nil {
				return nil, err
			}
			return decoded, nil
		},
		SkipSearchAttributes: true,
	})
	if err != nil {
		return err
	}
	return proxy.VisitFailures(ctx, message, proxy.VisitFailuresOptions{
		Visitor: func(_ *proxy.VisitFailuresContext, failure *failurepb.Failure) error {
			converter.DecodeCommonFailureAttributes(converter.GetDefaultDataConverter(), failure)
			failure.EncodedAttributes = nil
			return nil
		},
	})
}

// encodeStartRequest applies the payload codecs to an outbound start request,
// first materializing the null payload that an absent operation input is sent
// as. The visitor only reaches payloads that are already present.
func (h *handler) encodeStartRequest(ctx context.Context, request *workflowservice.StartNexusOperationExecutionRequest) error {
	if request.Input == nil {
		request.Input = &commonpb.Payload{
			Metadata: map[string][]byte{converter.MetadataEncoding: []byte(converter.MetadataEncodingNil)},
		}
	}
	return h.encodeOutbound(ctx, request)
}

func (h *handler) decodePayloads(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	var err error
	for _, codec := range h.codecs {
		if payloads, err = codec.Decode(payloads); err != nil {
			return nil, err
		}
	}
	return payloads, nil
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
