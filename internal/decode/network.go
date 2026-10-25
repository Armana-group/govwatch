package decode

import (
	"encoding/base64"
	"fmt"

	"govwatch/internal/b58"
)

// Network holds the per-chain constants the decoder needs. Addresses come
// from readme.md "Verified research findings".
type Network struct {
	Name       string
	ChainID    []byte
	Governance []byte
	Koin       []byte
}

var (
	Mainnet = Network{
		Name:       "mainnet",
		ChainID:    mustB64("EiBZK_GGVP0H_fXVAM3j6EAuz3-B-l3ejxRSewi7qIBfSA=="),
		Governance: mustB58("19qj51eTbSFJYU7ZagudkpxPgNSzPMfdPX"),
		Koin:       mustB58("19GYjDBVXU7keLbYvMLazsGQn3GTWHjHkK"),
	}
	Harbinger = Network{
		Name:       "harbinger",
		ChainID:    mustB64("EiAIKVvm6-V2qmsmUvPJy09vCCLbtn9lHFpwrJbcTIEWRQ=="),
		Governance: mustB58("17MjUXDCuTX1p9Kyqy48SQkkPfKScoggo"),
		Koin:       mustB58("1FaSvLjQJsCJKq5ybmGsMMQs8RQYyVv8ju"),
	}
)

// NetworkByName returns Mainnet or Harbinger.
func NetworkByName(name string) (Network, error) {
	switch name {
	case "mainnet":
		return Mainnet, nil
	case "harbinger":
		return Harbinger, nil
	}
	return Network{}, fmt.Errorf("unknown network %q (want mainnet or harbinger)", name)
}

func mustB58(s string) []byte {
	b, err := b58.Decode(s)
	if err != nil {
		panic(err)
	}
	return b
}

func mustB64(s string) []byte {
	b, err := base64.URLEncoding.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}
