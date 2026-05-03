//go:build integration

package containers

import (
	"context"
	"path/filepath"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"
	"github.com/testcontainers/testcontainers-go/wait"

	suiteutils "github.com/diegoado/stream-processor/integration_test/testsuite/utilities"
)

func startLocalStack(ctx context.Context, nw *testcontainers.DockerNetwork) (*localstack.LocalStackContainer, error) {
	root := suiteutils.ProjectRoot()

	return localstack.Run(ctx,
		"localstack/localstack:4.4.0",
		testcontainers.WithEnv(map[string]string{
			"SERVICES":       "sns,sqs,s3",
			"DEFAULT_REGION": "us-east-1",
		}),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Networks:       []string{nw.Name},
				NetworkAliases: map[string][]string{nw.Name: {"localstack"}},
				Files: []testcontainers.ContainerFile{
					{
						HostFilePath:      filepath.Join(root, "integration_test/testsuite/scripts/localstack/init-aws.sh"),
						ContainerFilePath: "/etc/localstack/init/ready.d/init-aws.sh",
						FileMode:          ExecutablePermission,
					},
					{
						HostFilePath:      filepath.Join(root, "schemas/event_schema.json"),
						ContainerFilePath: "/tmp/schemas/event_schema.json",
						FileMode:          ReadOnly,
					},
				},
				WaitingFor: wait.ForLog("LocalStack init complete"),
			},
		}),
	)
}
