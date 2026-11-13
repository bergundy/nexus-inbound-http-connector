package nexushttp

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type describeServer struct {
	workflowservice.UnimplementedWorkflowServiceServer
}

func (describeServer) DescribeNexusOperationExecution(context.Context, *workflowservice.DescribeNexusOperationExecutionRequest) (*workflowservice.DescribeNexusOperationExecutionResponse, error) {
	return &workflowservice.DescribeNexusOperationExecutionResponse{}, nil
}

// TestIdentityHeadersReachTheWire verifies the headers arrive at a real gRPC
// server on a connector request, not just out of the headers provider.
func TestIdentityHeadersReachTheWire(t *testing.T) {
	t.Parallel()

	var described metadata.MD
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		if md, ok := metadata.FromIncomingContext(ctx); ok && info.FullMethod == "/temporal.api.workflowservice.v1.WorkflowService/DescribeNexusOperationExecution" {
			described = md
		}
		return next(ctx, req)
	}))
	workflowservice.RegisterWorkflowServiceServer(server, describeServer{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	h, err := NewHandler(context.Background(), Options{Connection: ConnectionOptionsFromClientOptions{ClientOptions: &client.Options{
		HostPort:        listener.Addr().String(),
		HeadersProvider: staticHeaders{"x-a": "b"},
	}}})
	require.NoError(t, err)
	t.Cleanup(h.Close)

	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/operations/id", nil))
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

	require.Equal(t, []string{clientName}, described.Get(clientNameHeader))
	require.Equal(t, []string{clientVersion}, described.Get(clientVersionHeader))
	require.Equal(t, []string{"b"}, described.Get("x-a"))
}
