//go:build integration

package nexushttp

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/temporalproto"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
)

// devServerCLIVersion is the Temporal CLI release the integration tests run
// against. Standalone Nexus Operations are enabled by default on this
// development server; no feature override is needed.
const devServerCLIVersion = "v1.7.4-standalone-nexus-operations"

// startDevServer starts a Temporal development server for a single test,
// downloading the pinned CLI into the user temp directory on first use.
func startDevServer(t *testing.T) *testsuite.DevServer {
	t.Helper()

	// Generous timeout: the first run downloads the CLI.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	devServer, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		CachedDownload: testsuite.CachedDownload{Version: devServerCLIVersion},
		ClientOptions:  &client.Options{Namespace: "default"},
	})
	require.NoError(t, err, "start dev server")
	t.Cleanup(func() {
		devServer.Client().Close()
		assert.NoError(t, devServer.Stop(), "stop dev server")
	})
	return devServer
}

func TestIntegrationStartOperation(t *testing.T) {
	devServer := startDevServer(t)
	address := devServer.FrontendHostPort()
	temporalClient := devServer.Client()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	// The dev server is exclusive to this test, so the endpoint and task queue
	// need no run-specific names.
	taskQueue, endpointName := "nexus-http-connector", "nexus-http-connector"
	endpoint, err := temporalClient.OperatorService().CreateNexusEndpoint(ctx,
		&operatorservice.CreateNexusEndpointRequest{Spec: &nexuspb.EndpointSpec{
			Name: endpointName,
			Target: &nexuspb.EndpointTarget{Variant: &nexuspb.EndpointTarget_Worker_{
				Worker: &nexuspb.EndpointTarget_Worker{
					Namespace: "default",
					TaskQueue: taskQueue,
				},
			}},
		}})
	require.NoError(t, err, "create Nexus endpoint")
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, cleanupErr := temporalClient.OperatorService().DeleteNexusEndpoint(cleanupCtx,
			&operatorservice.DeleteNexusEndpointRequest{
				Id:      endpoint.Endpoint.Id,
				Version: endpoint.Endpoint.Version,
			})
		assert.NoError(t, cleanupErr, "delete Nexus endpoint")
	})

	operation := nexus.NewSyncOperation("echo", func(
		_ context.Context,
		input map[string]any,
		_ nexus.StartOperationOptions,
	) (map[string]any, error) {
		return input, nil
	})
	service := nexus.NewService("integration")
	require.NoError(t, service.Register(operation), "register Nexus operation")
	temporalWorker := worker.New(temporalClient, taskQueue, worker.Options{})
	temporalWorker.RegisterNexusService(service)
	require.NoError(t, temporalWorker.Start(), "start worker")
	t.Cleanup(temporalWorker.Stop)

	// The connector dials its own connection from client options, so this
	// exercises NewHandler rather than the in-process test constructor.
	handler, err := NewHandler(ctx, Options{ClientOptions: &client.Options{
		HostPort:  address,
		Namespace: "default",
	}})
	require.NoError(t, err, "new handler")
	t.Cleanup(handler.Close)

	body := fmt.Sprintf(`{
		"endpoint":%q,
		"service":"integration",
		"operation":"echo",
		"input":{"message":"hello"},
		"scheduleToCloseTimeout":"20s"
	}`, endpointName)
	request := httptest.NewRequest(http.MethodPost, "/operations/"+endpointName, bytes.NewBufferString(body)).WithContext(ctx)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	requireStatus(t, http.StatusCreated, response)

	result := &workflowservice.StartNexusOperationExecutionResponse{}
	err = temporalproto.CustomJSONUnmarshalOptions{Metadata: newRequestQuery(request).payloadMetadata()}.Unmarshal(response.Body.Bytes(), result)
	require.NoError(t, err, "decode start response")
	require.NotEmpty(t, result.RunId)
}
