package node

import (
	"bytes"
	"context"
	"testing"

	"github.com/koinos/koinos-proto-golang/v2/koinos"
	bs "github.com/koinos/koinos-proto-golang/v2/koinos/rpc/block_store"
	chain "github.com/koinos/koinos-proto-golang/v2/koinos/rpc/chain"
	"google.golang.org/protobuf/proto"
	"govwatch/internal/decode"
)

// fakeRPC answers chain and block_store requests with canned responses.
type fakeRPC struct {
	chainID    []byte
	head       uint64
	headID     []byte
	lib        uint64
	blockCalls []*bs.GetBlocksByHeightRequest
	generation uint64
}

func (f *fakeRPC) Call(_ context.Context, service string, body []byte) ([]byte, uint64, error) {
	switch service {
	case "chain":
		var req chain.ChainRequest
		if err := proto.Unmarshal(body, &req); err != nil {
			return nil, f.generation, err
		}
		var resp chain.ChainResponse
		switch req.Request.(type) {
		case *chain.ChainRequest_GetChainId:
			resp.Response = &chain.ChainResponse_GetChainId{GetChainId: &chain.GetChainIdResponse{ChainId: f.chainID}}
		case *chain.ChainRequest_GetHeadInfo:
			resp.Response = &chain.ChainResponse_GetHeadInfo{GetHeadInfo: &chain.GetHeadInfoResponse{HeadTopology: &koinos.BlockTopology{Height: f.head, Id: f.headID}, LastIrreversibleBlock: f.lib}}
		}
		out, err := proto.Marshal(&resp)
		return out, f.generation, err
	case "block_store":
		var req bs.BlockStoreRequest
		if err := proto.Unmarshal(body, &req); err != nil {
			return nil, f.generation, err
		}
		r := req.GetGetBlocksByHeight()
		f.blockCalls = append(f.blockCalls, r)
		items := make([]*bs.BlockItem, 0, r.NumBlocks)
		for h := r.AncestorStartHeight; h < r.AncestorStartHeight+uint64(r.NumBlocks); h++ {
			items = append(items, &bs.BlockItem{BlockHeight: h})
		}
		out, err := proto.Marshal(&bs.BlockStoreResponse{Response: &bs.BlockStoreResponse_GetBlocksByHeight{GetBlocksByHeight: &bs.GetBlocksByHeightResponse{BlockItems: items}}})
		return out, f.generation, err
	}
	return nil, f.generation, nil
}
func (f *fakeRPC) Close() error { return nil }

func newFake() *fakeRPC {
	return &fakeRPC{chainID: decode.Mainnet.ChainID, head: 5000, headID: []byte{0x12, 0x20, 1}, lib: 4900}
}

func TestFinality_ParsesHeadInfo(t *testing.T) {
	c := New(newFake(), decode.Mainnet.ChainID)
	f, err := c.Finality(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if f.HeadHeight != 5000 || f.Irreversible != 4900 || !bytes.Equal(f.HeadID, []byte{0x12, 0x20, 1}) {
		t.Fatalf("finality = %+v", f)
	}
}

func TestBlocks_RejectsWrongChainID(t *testing.T) {
	c := New(newFake(), decode.Harbinger.ChainID)
	if _, err := c.Finality(context.Background()); err == nil {
		t.Fatal("accepted a node on the wrong chain")
	}
	if _, err := c.Blocks(context.Background(), []byte{1}, 1, 10); err == nil {
		t.Fatal("Blocks accepted a node on the wrong chain")
	}
}

func TestBlocks_RejectsCountOver1000(t *testing.T) {
	c := New(newFake(), decode.Mainnet.ChainID)
	if _, err := c.Blocks(context.Background(), []byte{1}, 1, 1001); err == nil {
		t.Fatal("accepted count > 1000")
	}
}

func TestBlocks_RequestsBlockAndReceipt(t *testing.T) {
	rpc := newFake()
	c := New(rpc, decode.Mainnet.ChainID)
	items, err := c.Blocks(context.Background(), rpc.headID, 100, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 3 || items[0].BlockHeight != 100 {
		t.Fatalf("items = %+v", items)
	}
	req := rpc.blockCalls[0]
	if !req.ReturnBlock || !req.ReturnReceipt || !bytes.Equal(req.HeadBlockId, rpc.headID) || req.AncestorStartHeight != 100 || req.NumBlocks != 3 {
		t.Fatalf("request = %+v", req)
	}
}
