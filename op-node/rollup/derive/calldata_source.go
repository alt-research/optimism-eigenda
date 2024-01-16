package derive

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/ethereum-optimism/optimism/op-service/eigenda"
	"github.com/ethereum-optimism/optimism/op-service/pb/calldata"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/retry"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"google.golang.org/protobuf/proto"
)

// CalldataSource is a fault tolerant approach to fetching data.
// The constructor will never fail & it will instead re-attempt the fetcher
// at a later point.
type CalldataSource struct {
	// Internal state + data
	open bool
	data []eth.Data
	// Required to re-attempt fetching
	ref     eth.L1BlockRef
	dsCfg   DataSourceConfig
	fetcher L1TransactionFetcher
	log     log.Logger

	batcherAddr common.Address
}

// NewCalldataSource creates a new calldata source. It suppresses errors in fetching the L1 block if they occur.
// If there is an error, it will attempt to fetch the result on the next call to `Next`.
func NewCalldataSource(ctx context.Context, log log.Logger, dsCfg DataSourceConfig, fetcher L1TransactionFetcher, ref eth.L1BlockRef, batcherAddr common.Address) DataIter {
	_, txs, err := fetcher.InfoAndTxsByHash(ctx, ref.Hash)
	if err != nil {
		return &CalldataSource{
			open:        false,
			ref:         ref,
			dsCfg:       dsCfg,
			fetcher:     fetcher,
			log:         log,
			batcherAddr: batcherAddr,
		}
	}
	return &CalldataSource{
		open: true,
		data: DataFromEVMTransactions(dsCfg, batcherAddr, txs, log.New("origin", ref)),
	}
}

// Next returns the next piece of data if it has it. If the constructor failed, this
// will attempt to reinitialize itself. If it cannot find the block it returns a ResetError
// otherwise it returns a temporary error if fetching the block returns an error.
func (ds *CalldataSource) Next(ctx context.Context) (eth.Data, error) {
	if !ds.open {
		if _, txs, err := ds.fetcher.InfoAndTxsByHash(ctx, ds.ref.Hash); err == nil {
			ds.open = true
			ds.data = DataFromEVMTransactions(ds.dsCfg, ds.batcherAddr, txs, ds.log)
		} else if errors.Is(err, ethereum.NotFound) {
			return nil, NewResetError(fmt.Errorf("failed to open calldata source: %w", err))
		} else {
			return nil, NewTemporaryError(fmt.Errorf("failed to open calldata source: %w", err))
		}
	}
	if len(ds.data) == 0 {
		return nil, io.EOF
	} else {
		data := ds.data[0]
		ds.data = ds.data[1:]
		return data, nil
	}
}

// DataFromEVMTransactions filters all of the transactions and returns the calldata from transactions
// that are sent to the batch inbox address from the batch sender address.
// This will return an empty array if no valid transactions are found.
func DataFromEVMTransactions(dsCfg DataSourceConfig, batcherAddr common.Address, txs types.Transactions, log log.Logger) []eth.Data {
	out := []eth.Data{}
	for _, tx := range txs {
		if isValidBatchTx(tx, dsCfg.l1Signer, dsCfg.batchInboxAddress, batcherAddr) {
			data := calldata.Calldata{}
			if err := proto.Unmarshal(tx.Data(), &data); err != nil {
				log.Warn("unable to decode calldata from tx", "tx.hash", tx.Hash().Hex(), "err", err)
				return nil
			}
			switch data.Value.(type) {
			case *calldata.Calldata_Raw:
				log.Info("successfully retrieved data from raw calldata", "tx.hash", tx.Hash().Hex())
				out = append(out, data.GetRaw())
			case *calldata.Calldata_EigendaRef:
				// digest := hex.EncodeToString(data.GetDigest().GetPayload())
				result, err := retry.Do(context.Background(), 3, retry.Fixed(time.Second), func() ([]byte, error) {
					ctx := eigenda.WithLogger(context.Background(), log)
					return eigenda.RetrieveBlob(ctx, data.GetEigendaRef().GetBatchHeaderHash(), data.GetEigendaRef().GetBlobIndex())
				})
				if err != nil {
					log.Warn("get batcher transaction from eigenda failed after 3 times retry", "tx.hash", tx.Hash().Hex(), "err", err)
					return nil
				}
				log.Info("successfully retrieved data from eigenda", "tx.hash", tx.Hash().Hex())
				out = append(out, result)
			}
		}
	}
	return out
}
