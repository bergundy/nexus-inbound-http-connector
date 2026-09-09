package nexushttp

import (
	"encoding/base64"
	"net/http"
	"net/url"
	"strconv"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/temporalproto"
)

// requestQuery reads optional parameters from a request's query string, which
// it parses once per request. Values are read-only; parse failures are returned
// to the caller as bad request errors.
type requestQuery struct {
	values url.Values
}

func newRequestQuery(r *http.Request) requestQuery {
	return requestQuery{values: r.URL.Query()}
}

func (q requestQuery) optString(name string) string {
	return q.values.Get(name)
}

func (q requestQuery) optBool(name string) (bool, error) {
	return optQuery(q, name, strconv.ParseBool)
}

func (q requestQuery) optBytes(name string) ([]byte, error) {
	return optQuery(q, name, base64.StdEncoding.DecodeString)
}

func (q requestQuery) optInt32(name string) (int32, error) {
	return optQuery(q, name, parseInt32)
}

func (q requestQuery) optWaitStage(name string) (enumspb.NexusOperationWaitStage, error) {
	if q.values.Get(name) == "" {
		return enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED, nil
	}
	return optQuery(q, name, enumspb.NexusOperationWaitStageFromString)
}

// optQuery parses an optional query parameter, yielding the zero value when the
// parameter is absent and a bad request error when it does not parse.
func optQuery[T any](q requestQuery, name string, parse func(string) (T, error)) (T, error) {
	var zero T
	value := q.values.Get(name)
	if value == "" {
		return zero, nil
	}
	parsed, err := parse(value)
	if err != nil {
		return zero, newStatusErrorf(http.StatusBadRequest, "invalid %s: %v", name, err)
	}
	return parsed, nil
}

func parseInt32(value string) (int32, error) {
	parsed, err := strconv.ParseInt(value, 10, 32)
	return int32(parsed), err
}

func (q requestQuery) payloadMetadata() map[string]any {
	if _, disabled := q.values["noPayloadShorthand"]; disabled {
		return nil
	}
	return map[string]any{commonpb.EnablePayloadShorthandMetadataKey: true}
}

func (q requestQuery) marshalOptions() temporalproto.CustomJSONMarshalOptions {
	options := temporalproto.CustomJSONMarshalOptions{Metadata: q.payloadMetadata()}
	if _, pretty := q.values["pretty"]; pretty {
		options.Indent = "  "
	}
	return options
}
