package eigenda

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/Layr-Labs/eigenda/api/grpc/disperser"
	env "github.com/Netflix/go-env"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/ethereum/go-ethereum/log"
)

func Init() error {
	_, err := env.UnmarshalFromEnviron(&config)
	if err != nil {
		return err
	}
	if err := config.sanitize(); err != nil {
		return err
	}
	creds := credentials.NewTLS(&tls.Config{
		InsecureSkipVerify: true,
	})
	conn, err := grpc.Dial(config.EigenDARpc, grpc.WithTransportCredentials(creds))
	if err != nil {
		return err
	}
	client = disperser.NewDisperserClient(conn)
	return nil
}

func init() {
	_ = Init()
}

type Config struct {
	EigenDARpc                      string        `env:"EIGEN_DA_RPC"`
	EigenDAQuorumID                 uint32        `env:"EIGEN_DA_QUORUM_ID"`
	EigenDAAdversaryThreshold       uint32        `env:"EIGEN_DA_ADVERSARY_THRESHOLD"`
	EigenDAQuorumThreshold          uint32        `env:"EIGEN_DA_QUORUM_THRESHOLD"`
	EigenDAStatusQueryRetryInterval time.Duration `env:"EIGEN_DA_STATUS_QUERY_RETRY_INTERVAL"`
	EigenDAStatusQueryTimeout       time.Duration `env:"EIGEN_DA_STATUS_QUERY_TIMEOUT"`
}

func (m Config) sanitize() error {
	if m.EigenDARpc == "" {
		return errors.New("invalid rpc endpoint")
	}
	if m.EigenDAAdversaryThreshold == 0 || m.EigenDAAdversaryThreshold >= 100 {
		return errors.New("invalid adversary threshold, must in range (0, 100)")
	}
	if m.EigenDAQuorumThreshold == 0 || m.EigenDAQuorumThreshold >= 100 {
		return errors.New("invalid quorum threshold, must in range (0, 100)")
	}
	if m.EigenDAStatusQueryTimeout == 0 {
		return errors.New("invalid status query timeout, must be greater than 0")
	}
	if m.EigenDAStatusQueryRetryInterval == 0 {
		return errors.New("invalid status query retry interval, must be greater than 0")
	}
	return nil
}

type loggerKey struct{}

var (
	config                  Config
	client                  disperser.DisperserClient
	ErrClientNotInitialized = errors.New("eigenda client not initialized")
)

func WithLogger(ctx context.Context, logger log.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, logger)
}

func RetrieveBlob(ctx context.Context, BatchHeaderHash []byte, BlobIndex uint32) ([]byte, error) {
	if client == nil {
		return nil, ErrClientNotInitialized
	}
	reply, err := client.RetrieveBlob(ctx, &disperser.RetrieveBlobRequest{
		BatchHeaderHash: BatchHeaderHash,
		BlobIndex:       BlobIndex,
	})
	if err != nil {
		return nil, err
	}
	return reply.Data, nil
}

func DisperseBlob(ctx context.Context, data []byte) (*disperser.BlobInfo, error) {
	if client == nil {
		return nil, ErrClientNotInitialized
	}
	logger, ok := ctx.Value(loggerKey{}).(log.Logger)
	if !ok {
		// NOTE: make sure logger is passed from context, otherwise will use default logger
		logger = log.Root()
	}
	disperseReq := &disperser.DisperseBlobRequest{
		Data: data,
		SecurityParams: []*disperser.SecurityParams{
			{
				QuorumId:           config.EigenDAQuorumID,
				AdversaryThreshold: config.EigenDAAdversaryThreshold,
				QuorumThreshold:    config.EigenDAQuorumThreshold,
			},
		},
	}
	disperseRes, err := client.DisperseBlob(ctx, disperseReq)
	if err != nil {
		return nil, err
	}
	base64RequestID := base64.StdEncoding.EncodeToString(disperseRes.RequestId)
	ticker := time.NewTicker(config.EigenDAStatusQueryRetryInterval)
	defer ticker.Stop()
	c, cancel := context.WithTimeout(ctx, config.EigenDAStatusQueryTimeout)
	defer cancel()
	for {
		select {
		case <-ticker.C:
			statusReply, err := client.GetBlobStatus(ctx, &disperser.BlobStatusRequest{
				RequestId: disperseRes.RequestId,
			})
			if err != nil {
				logger.Warn("[EigenDA] failed to get blob dispersal status", "requestID", base64RequestID, "err", err)
				continue
			}
			switch statusReply.GetStatus() {
			case disperser.BlobStatus_CONFIRMED, disperser.BlobStatus_FINALIZED:
				return statusReply.Info, nil
			case disperser.BlobStatus_FAILED:
				return nil, fmt.Errorf("[EigenDA] blob dispersal failed with reply status %v", statusReply.Status)
			default:
				logger.Info("[EigenDA] still waiting for blob dispersal", "requestID", base64RequestID)
				continue
			}
		case <-c.Done():
			return nil, fmt.Errorf("[EigenDA] blob dispersal timed out requestID: %s", base64RequestID)
		}
	}
}
