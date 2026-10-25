// Package node reads chain state and blocks from a Koinos node over its
// binary RPC (the AMQP koinos.rpc exchange). Every read re-checks the chain
// id on the same connection generation, so a reconnect to a different node
// mid-request is detected (pattern lifted from koinos-indexer).
package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	bs "github.com/koinos/koinos-proto-golang/v2/koinos/rpc/block_store"
	chain "github.com/koinos/koinos-proto-golang/v2/koinos/rpc/chain"
	"google.golang.org/protobuf/proto"
)

var (
	ErrNetwork     = errors.New("node: wrong network (chain id mismatch)")
	ErrSource      = errors.New("node: inconsistent response")
	ErrUnavailable = errors.New("node: connection changed during request")
	ErrRange       = errors.New("node: unsupported range")
)

// MaxBatch is the largest num_blocks block_store accepts per request.
const MaxBatch uint32 = 1000

type RPC interface {
	Call(ctx context.Context, service string, body []byte) ([]byte, uint64, error)
	Close() error
}

type Finality struct {
	HeadHeight   uint64
	HeadID       []byte
	Irreversible uint64
}

// Source is what replay and live mode need from a node.
type Source interface {
	Finality(ctx context.Context) (Finality, error)
	Blocks(ctx context.Context, headID []byte, start uint64, count uint32) ([]*bs.BlockItem, error)
}

type Client struct {
	rpc      RPC
	expected []byte
	mu       sync.Mutex
}

func New(rpc RPC, expectedChainID []byte) *Client {
	return &Client{rpc: rpc, expected: append([]byte{}, expectedChainID...)}
}

func (c *Client) Close() error { return c.rpc.Close() }

func (c *Client) Finality(ctx context.Context) (Finality, error) {
	var resp chain.ChainResponse
	if err := c.request(ctx, "chain", &chain.ChainRequest{Request: &chain.ChainRequest_GetHeadInfo{GetHeadInfo: &chain.GetHeadInfoRequest{}}}, &resp); err != nil {
		return Finality{}, err
	}
	info := resp.GetGetHeadInfo()
	if info == nil || info.HeadTopology == nil {
		return Finality{}, ErrSource
	}
	return Finality{HeadHeight: info.HeadTopology.Height, HeadID: append([]byte{}, info.HeadTopology.Id...), Irreversible: info.LastIrreversibleBlock}, nil
}

// Blocks fetches count blocks with receipts starting at start, on the fork
// that headID belongs to.
func (c *Client) Blocks(ctx context.Context, headID []byte, start uint64, count uint32) ([]*bs.BlockItem, error) {
	if start == 0 || count == 0 || count > MaxBatch {
		return nil, fmt.Errorf("%w: start %d count %d", ErrRange, start, count)
	}
	req := &bs.BlockStoreRequest{Request: &bs.BlockStoreRequest_GetBlocksByHeight{GetBlocksByHeight: &bs.GetBlocksByHeightRequest{HeadBlockId: headID, AncestorStartHeight: start, NumBlocks: count, ReturnBlock: true, ReturnReceipt: true}}}
	var resp bs.BlockStoreResponse
	if err := c.request(ctx, "block_store", req, &resp); err != nil {
		return nil, err
	}
	result := resp.GetGetBlocksByHeight()
	if result == nil {
		return nil, ErrSource
	}
	return result.BlockItems, nil
}

func (c *Client) chainID(ctx context.Context) (uint64, error) {
	body, err := proto.Marshal(&chain.ChainRequest{Request: &chain.ChainRequest_GetChainId{GetChainId: &chain.GetChainIdRequest{}}})
	if err != nil {
		return 0, err
	}
	reply, gen, err := c.rpc.Call(ctx, "chain", body)
	if err != nil {
		return 0, err
	}
	var resp chain.ChainResponse
	if proto.Unmarshal(reply, &resp) != nil {
		return gen, ErrSource
	}
	id := resp.GetGetChainId().GetChainId()
	if len(id) == 0 {
		return gen, ErrSource
	}
	if !bytes.Equal(id, c.expected) {
		return gen, ErrNetwork
	}
	return gen, nil
}

// request checks the chain id, then performs the call on the same connection
// generation; a reconnect in between discards the unverified reply.
func (c *Client) request(ctx context.Context, service string, req, resp proto.Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	gen, err := c.chainID(ctx)
	if err != nil {
		return err
	}
	body, err := proto.Marshal(req)
	if err != nil {
		return err
	}
	reply, g, err := c.rpc.Call(ctx, service, body)
	if err != nil {
		return err
	}
	if g != gen {
		return ErrUnavailable
	}
	if err := proto.Unmarshal(reply, resp); err != nil {
		return ErrSource
	}
	if e := errorOf(resp); e != "" {
		return fmt.Errorf("%w: %s", ErrSource, e)
	}
	return nil
}

func errorOf(resp proto.Message) string {
	switch r := resp.(type) {
	case *chain.ChainResponse:
		if e := r.GetError(); e != nil {
			return e.GetMessage()
		}
	case *bs.BlockStoreResponse:
		if e := r.GetError(); e != nil {
			return e.GetMessage()
		}
	}
	return ""
}
