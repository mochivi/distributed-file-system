package grpcutil

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

// mockGRPCSetup returns a GRPCSetupFunc that uses an in-memory listener.
func mockGRPCSetup(closeListenerImmediately bool) GRPCSetupFunc {
	return func(wg *sync.WaitGroup, errChan chan error) (*grpc.Server, net.Listener) {
		// Use a random free port on localhost
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			errChan <- err
			return nil, nil
		}

		if closeListenerImmediately {
			lis.Close() // Force Serve to error immediately
		}

		srv := grpc.NewServer()
		return srv, lis
	}
}

// mockAgentLaunch returns an AgentLaunchFunc that optionally sends an error into errChan.
func mockAgentLaunch(sendError bool) AgentLaunchFunc {
	return func(wg *sync.WaitGroup, errChan chan error) {
		if sendError {
			errChan <- errors.New("agent error")
		}
	}
}

func TestLaunch(t *testing.T) {
	logger := logging.NewTestLogger(slog.LevelError, true)

	testCases := []struct {
		name                    string
		grpcSetup               GRPCSetupFunc
		agentLaunch             AgentLaunchFunc
		expectedLaunchError     bool
		expectedShutdownTimeout time.Duration
	}{
		{
			name:                    "success: launch with agent error shutdown",
			grpcSetup:               mockGRPCSetup(false),
			agentLaunch:             mockAgentLaunch(true), // triggers shutdown via error
			expectedLaunchError:     false,
			expectedShutdownTimeout: 2 * time.Second,
		},
		{
			name:                    "success: shutdown triggered by server error",
			grpcSetup:               mockGRPCSetup(true), // listener closed -> Serve error
			agentLaunch:             mockAgentLaunch(false),
			expectedLaunchError:     false,
			expectedShutdownTimeout: 2 * time.Second,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			resultChan := make(chan error, 1)
			go func() {
				err := Launch(ctx, cancel, logger, tc.grpcSetup, tc.agentLaunch, tc.expectedShutdownTimeout)
				resultChan <- err
			}()

			select {
			case err := <-resultChan:
				if tc.expectedLaunchError {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Launch did not return within expected time")
			}
		})
	}
}
