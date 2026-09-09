# Nexus Inbound HTTP Connector

An experimental Go library that exposes Temporal Standalone Nexus Operations
through an embeddable `http.Handler`. Applications supply Temporal Go SDK
client options, wrap the handler in their own authentication and authorization
middleware, and deploy it with their existing HTTP server.

The connector is stateless and does not own a server or introduce a new
Temporal-side ingress path. It does dial its own Temporal connection from the
supplied `client.Options`, which control the address, TLS configuration,
credentials, and the namespace written to every Temporal request. When no
options are supplied, they are loaded from
[Temporal environment configuration](#client-configuration). The handler owns
that connection, so close it when the process shuts down.

## Requirements

- Go 1.26 or later, matching the minimum version on the Temporal Go SDK main
  branch
- A Temporal server with Standalone Nexus Operations enabled

## Installation

```bash
go get github.com/temporalio/nexus-inbound-http-connector
```

## Usage

```go
package main

import (
	"context"
	"log"
	"log/slog"
	"net/http"

	nexushttp "github.com/temporalio/nexus-inbound-http-connector"
)

func main() {
	// ClientOptions is unset, so the Temporal connection is configured from the
	// TEMPORAL_* environment variables and the temporal.toml profile.
	handler, err := nexushttp.NewHandler(context.Background(), nexushttp.Options{
		Logger: slog.Default(),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer handler.Close()

	// Add application-owned authentication, authorization, rate limiting, and
	// observability middleware around handler here.
	log.Fatal(http.ListenAndServe(":8080", handler))
}
```

The context passed to `NewHandler` bounds the connection attempt only; served
requests use their own contexts.

The handler exposes:

| Method | Route | Purpose |
| --- | --- | --- |
| `POST` | `/operations/{id}` | Start an operation |
| `GET` | `/operations/{id}` | Describe an operation |
| `GET` | `/operations/{id}/poll` | Long-poll for completion |
| `GET` | `/operations` | List operations |
| `GET` | `/operation-count` | Count operations |
| `POST` | `/operations/{id}/cancel` | Request cancellation |
| `POST` | `/operations/{id}/terminate` | Terminate an operation |

`runId` selects a specific run on describe, poll, cancel, and terminate routes.
Omitting it targets the latest run. Describe also accepts `includeInput`,
`includeOutcome`, and the base64-encoded `longPollToken` from a prior response.
Poll accepts `waitStage` (`Started` or `Closed`) and defaults to `Closed`. List
accepts `query`, `pageSize`, and a base64-encoded `nextPageToken`, and returns
exactly one page plus its continuation token. Count accepts `query`.

The connector uses the client's low-level `WorkflowService` for fields the Go
SDK's higher-level Nexus APIs do not expose. Those calls use the incoming HTTP
request context for cancellation and deadlines and do not add the SDK's
higher-level automatic retries. Clients that retry start, cancel, or terminate
requests should reuse the same `requestId`.

Request bodies are limited to 4 MiB by default. Set `MaxRequestBodySize` to an
`nexushttp.MaxRequestBodySize` byte count to override the limit; zero rejects
non-empty request bodies. Set it to
`nexushttp.MaxRequestBodySizeNotEnforced{}` to disable the limit.

## Client configuration

`Options.ClientOptions` is a `*client.Options`. Leave it nil—the default—and
`NewHandler` loads the options with
[`envconfig.LoadDefaultClientOptions`](https://pkg.go.dev/go.temporal.io/sdk/contrib/envconfig),
which reads the `temporal.toml` profile selected by `TEMPORAL_CONFIG_FILE` and
`TEMPORAL_PROFILE` and layers the `TEMPORAL_*` environment variables on top:
`TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_API_KEY`,
`TEMPORAL_CLIENT_AUTHORITY`, `TEMPORAL_TLS*`, and `TEMPORAL_GRPC_META_*`. This
is the same configuration the Temporal CLI and the other SDKs read, so the
connector picks up an existing local or Temporal Cloud profile without code.

Set the field to configure the connection in code instead:

```go
handler, err := nexushttp.NewHandler(ctx, nexushttp.Options{
	ClientOptions: &client.Options{
		HostPort:  "my-namespace.a1b2c.tmprl.cloud:7233",
		Namespace: "my-namespace",
		Credentials: client.NewAPIKeyStaticCredentials(apiKey),
	},
})
```

To start from environment configuration and adjust it, load the options
yourself and pass the result:

```go
clientOptions, err := envconfig.LoadDefaultClientOptions()
if err != nil {
	log.Fatal(err)
}
clientOptions.Identity = "nexus-http-connector"
handler, err := nexushttp.NewHandler(ctx, nexushttp.Options{ClientOptions: &clientOptions})
```

The connector calls Temporal through the client's low-level `WorkflowService`,
so only part of `client.Options` reaches its requests:

| Field | How the connector uses it |
| --- | --- |
| `HostPort` | gRPC target to dial. Defaults to `localhost:7233`. |
| `Namespace` | Written to every WorkflowService request; see [Connector namespace](#connector-namespace). Defaults to `default`, matching the SDK client. |
| `Identity` | Written to start, cancel, and terminate requests whose body omits an identity. Left empty when unset—unlike the SDK client, the connector does not synthesize one. |
| `Credentials`, `ConnectionOptions`, `HeadersProvider` | Open the connection and authenticate every call: TLS, API keys, mTLS, keep-alive, gRPC dial options, and per-request metadata. |
| `Logger`, `MetricsHandler`, `DisableErrorCodeMetricTags` | Observe the connector's calls; the SDK installs both as connection-level gRPC interceptors. `Logger` is unrelated to `Options.Logger`, which records the connector's own HTTP-layer errors. |
| `Plugins` | Honored by client creation. A plugin that rewrites `Namespace` or `Identity` does not change what the connector sends; the connector reads both from the options as given. |

Every other field only affects the SDK's higher-level client APIs, which the
connector never calls, and is ignored: `DataConverter`, `FailureConverter`,
`ContextPropagators`, `Interceptors`, `ExternalStorage`, `PayloadLimits`, and
the worker-only `WorkerHeartbeatInterval`, `SdkName`, and `SdkVersion`.
`TEMPORAL_CODEC_ENDPOINT` and `TEMPORAL_CODEC_AUTH` are ignored for the same
reason: they configure a remote data converter. Use
[`PayloadCodecs`](#payload-codecs) for payload encoding.

## Connector namespace

A Standalone Nexus Operation has no calling Workflow, so Temporal needs a
namespace to own it. That namespace is `ClientOptions.Namespace`, which the
connector writes to every request to the server. A single `nexushttp.Handler`
connects to a single connector namespace.

It is the caller side of an operation, not the handler side. The `endpoint` in
a start body names a Nexus Endpoint that routes to a handler in its own target
namespace; the two namespaces are independent.

Everything the connector's routes touch is scoped to the connector namespace:
operations are stored there and must have Standalone Nexus Operations enabled
on it, the `{id}` in a route must be unique among its operations, list and
count query its visibility store and custom search attributes must be
registered on it, and its server-side limits and rate limits apply. A dedicated
namespace for the connector is usually the right unit of isolation.

### Endpoint permissions

Temporal decides which endpoints the connector may call, using the connector
namespace as the caller identity. The connector does not filter endpoint names,
so any HTTP caller may name any endpoint in a start body and the namespace's
endpoint permissions are the boundary.

Starting an operation on an endpoint that does not allow the connector
namespace returns `403 Forbidden`.

**Temporal Cloud** keeps an allowed-caller-namespace list on each Nexus
Endpoint.

**Self-hosted clusters** have no such list: endpoints are registered
cluster-wide and the registry does not filter them by caller namespace, so the
cluster's `Authorizer` plugin is the only place to restrict them.

Either way, Temporal authorizes the connector namespace and not the HTTP caller
that reached it—see [Security](#security).

## Protobuf JSON API

Request and response bodies use the models in `go.temporal.io/api` and
Temporal's custom protobuf JSON implementation—the same implementation used by
Temporal's grpc-gateway HTTP API. Fields therefore use protobuf JSON names,
durations use strings, enums accept Temporal's camel-case or canonical names,
and unknown fields are rejected.

The start body is a
`temporal.api.workflowservice.v1.StartNexusOperationExecutionRequest`.
`operationId` comes from the route and the namespace comes from the configured
client options, so callers should omit both from the body. A caller-provided
`requestId` is forwarded unchanged; when it is omitted, the connector generates
one. Search attributes, Nexus headers, and both user-metadata fields are
forwarded. The request `identity` is also forwarded when present; when omitted,
it falls back to `ClientOptions.Identity` and stays empty if that is unset:

```http
POST /operations/my-op-1
Content-Type: application/json

{
  "endpoint": "myEndpoint",
  "service": "myService",
  "operation": "myOperation",
  "input": {"hello": "world"},
  "scheduleToCloseTimeout": "3600s"
}
```

Start always returns `201 Created` with a
`temporal.api.workflowservice.v1.StartNexusOperationExecutionResponse`
containing only the operation run ID. The `started` field is intentionally not
populated, even when Temporal reports that a new operation was started.
Describe, poll, list, count, cancel, and terminate return their generated
WorkflowService response messages. Caller-provided cancellation and termination
`requestId` values are also forwarded unchanged; omitted values are generated.
Cancellation returns `202 Accepted` because it is eventually consistent.
Termination happens immediately and returns `200 OK`.

### Payload shorthand

Payload shorthand is enabled by default:

- Any JSON value is read and written as a `json/plain` Temporal payload.
- `null` is accepted as a `binary/null` payload. Temporal's current custom
  protobuf JSON encoder writes that payload in canonical form.
- Canonical protobuf JSON payloads remain accepted.
- Protobuf payload shorthand with `_protoMessageType` is supported.

Add `?noPayloadShorthand` to use canonical protobuf JSON payloads instead. Add
`?pretty` to indent success responses.

### Payload codecs

Codecs are applied inside the customer process at the raw WorkflowService
payload boundary:

```go
handler, err := nexushttp.NewHandler(ctx, nexushttp.Options{
	PayloadCodecs: []converter.PayloadCodec{myCodec},
})
```

On start requests, codecs encode the parsed operation input before it is sent to
Temporal. On describe and poll responses, they decode operation input, result,
and failure payloads before those values are rendered as protobuf JSON or a
Nexus Failure. Low-level WorkflowService calls bypass the client's data
converter, so `PayloadCodecs` is the codec chain for these raw payloads, and
`ClientOptions.DataConverter` is ignored.

## Failures

Operation outcomes and errors have different shapes.

An operation outcome returns `424 Failed Dependency` in the Nexus Failure JSON
envelope, preserving the Temporal failure type, details, stack trace, and cause
chain.

An error returns `{"message": "..."}` and nothing else. Invalid requests are
`400 Bad Request`. Errors from Temporal carry the gRPC status message and the
HTTP status grpc-gateway's `runtime.HTTPStatusFromCode` maps their code onto.
Because the error body is a subset of the Nexus Failure envelope, a client that
parses Nexus failures reads it without a second code path.

Anything that resolves to `500 Internal Server Error` is not reported at all.
The error is written to `Options.Logger` under a generated ID and the response
carries only that ID, as `internal error (ID: ...)`, so that implementation
detail—including the messages Temporal returns with `Internal`, `Unknown`, and
`DataLoss`—never reaches a caller.

## Security

The connector intentionally does not provide HTTP authentication,
authorization, rate limiting, TLS termination, or policy middleware. The host
application owns those concerns. Temporal authentication and TLS come from the
supplied or environment-loaded client options—`Credentials`, `ConnectionOptions`,
and `HeadersProvider`. Configured payload codecs protect operation input before it
leaves the application environment and decode operation results and failures
after they return.

Every request the handler serves carries the same Temporal credentials and the
same [connector namespace](#connector-namespace), so Temporal cannot tell one
HTTP caller from another. Its
[endpoint permissions](#endpoint-permissions) bound what any caller can reach;
middleware decides which callers get that far.

## Development

Install [golangci-lint v2.13.2](https://golangci-lint.run/welcome/install/),
then run the local checks with:

```bash
gofmt -w .
golangci-lint run ./...
go test ./...
go vet ./...
```

The integration tests start their own development server, so they need no
running `temporal` server and no `temporal` binary on `PATH`:

```bash
go test -tags=integration -run TestIntegration ./...
```

Each test starts a Temporal CLI development server through
[`testsuite.StartDevServer`](https://pkg.go.dev/go.temporal.io/sdk/testsuite#StartDevServer),
which downloads the pinned `v1.7.4-standalone-nexus-operations` CLI into the
user temp directory on first use and caches it for later runs. That development
server enables Standalone Nexus Operations by default; no feature override is
needed.

## License

[MIT](LICENSE)
