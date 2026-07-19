package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"math/big"
	"os"
	"time"

	provisionpb "github.com/MiltonJ23/WouriFS/api/gen/v1/provision"
	"github.com/MiltonJ23/WouriFS/internal/shamir"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

func main() {
	splitCmd := flag.NewFlagSet("split", flag.ExitOnError)
	splitSecret := splitCmd.String("secret", "", "secret hex string to split (or - for stdin)")
	splitK := splitCmd.Int("k", 2, "threshold shares needed to reconstruct")
	splitN := splitCmd.Int("n", 2, "total shares to generate")
	splitOut := splitCmd.String("out", "", "output directory for share files (default: stdout)")

	combineCmd := flag.NewFlagSet("combine", flag.ExitOnError)
	combineShareA := combineCmd.String("share-a", "", "path to first share file or COBAC server URL")
	combineShareB := combineCmd.String("share-b", "", "path to second share file")
	combineUsb := combineCmd.String("usb", "", "path to USB mount containing admin share")
	combineCobac := combineCmd.String("cobac", "share-service.cobac.cm:443", "COBAC share-service address")
	combineInst := combineCmd.String("institution", "", "institution ID for COBAC request")
	combineNode := combineCmd.String("node", "", "node ID being provisioned")
	combineOut := combineCmd.String("out", "", "output directory for generated credentials")
	combineJWT := combineCmd.String("jwt", "", "JWT token for share-service authentication")
	combineTLSCA := combineCmd.String("tls-ca", "", "CA cert for share-service TLS (required)")

	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: wouri-provision <split|combine>\n")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "split":
		splitCmd.Parse(os.Args[2:])
		handleSplit(splitSecret, *splitK, *splitN, *splitOut)
	case "combine":
		combineCmd.Parse(os.Args[2:])
		handleCombine(combineShareA, combineShareB, combineUsb, combineCobac, combineInst, combineNode, combineOut, combineJWT, combineTLSCA)
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", os.Args[1])
		os.Exit(1)
	}
}

func handleSplit(secretHex *string, k, n int, outDir string) {
	secretBytes, err := hex.DecodeString(*secretHex)
	if err != nil {
		log.Fatalf("invalid hex secret: %v", err)
	}

	shares, err := shamir.Split(secretBytes, k, n)
	if err != nil {
		log.Fatalf("split: %v", err)
	}

	for i, s := range shares {
		data := []byte(fmt.Sprintf("%s\n%s\n", s[0].Text(16), s[1].Text(16)))
		if outDir != "" {
			path := fmt.Sprintf("%s/share-%d.shamir", outDir, i+1)
			if err := os.MkdirAll(outDir, 0750); err != nil {
				log.Fatalf("mkdir: %v", err)
			}
			os.WriteFile(path, data, 0600)
			fmt.Printf("share %d written to %s\n", i+1, path)
		} else {
			fmt.Printf("--- share %d ---\n%s", i+1, string(data))
		}
	}
}

func handleCombine(shareA, shareB, usb, cobacAddr, institution, node, outDir, jwtToken, tlsCA *string) {
	var s1, s2 [2]*big.Int
	loaded := 0

	// Load share 1 from USB key
	if *usb != "" {
		data, err := os.ReadFile(*usb + "/share-1.shamir")
		if err != nil {
			log.Fatalf("read USB share: %v", err)
		}
		s1 = parseShareFile(data)
		loaded++
	}

	// Load share 2 from local file
	if *shareB != "" {
		data, err := os.ReadFile(*shareB)
		if err != nil {
			log.Fatalf("read share B: %v", err)
		}
		if loaded == 0 {
			s1 = parseShareFile(data)
		} else {
			s2 = parseShareFile(data)
		}
		loaded++
	}

	// Load share from COBAC over network
	if loaded < 2 && *institution != "" {
		shareBytes, err := fetchFromCOBAC(*cobacAddr, *institution, *node, *jwtToken, *tlsCA)
		if err != nil {
			log.Fatalf("cobac fetch: %v", err)
		}
		if loaded == 0 {
			s1 = [2]*big.Int{big.NewInt(2), new(big.Int).SetBytes(shareBytes)} // COBAC is share-2
		} else {
			s2 = [2]*big.Int{big.NewInt(2), new(big.Int).SetBytes(shareBytes)}
		}
		loaded++
	}

	if loaded < 2 {
		log.Fatal("need exactly 2 shares (USB + COBAC network)")
	}

	secret, err := shamir.Combine([][2]*big.Int{s1, s2})
	if err != nil {
		log.Fatalf("combine: %v", err)
	}

	if *outDir != "" {
		if err := os.MkdirAll(*outDir, 0700); err != nil {
			log.Fatalf("mkdir: %v", err)
		}
		os.WriteFile(*outDir+"/wireguard.key", secret, 0600)
		fmt.Printf("credentials written to %s/\n", *outDir)
	} else {
		fmt.Printf("reconstructed secret: %x\n", secret)
	}

	// Wipe shares from memory (best effort)
	s1 = [2]*big.Int{}
	s2 = [2]*big.Int{}
}

func parseShareFile(data []byte) [2]*big.Int {
	var xHex, yHex string
	fmt.Sscanf(string(data), "%s\n%s", &xHex, &yHex)

	x := new(big.Int)
	y := new(big.Int)
	x.SetString(xHex, 16)
	y.SetString(yHex, 16)

	return [2]*big.Int{x, y}
}

func fetchFromCOBAC(addr, institution, node, jwtToken, tlsCAPath string) ([]byte, error) {
	if tlsCAPath == "" {
		return nil, fmt.Errorf("TLS CA certificate required (--tls-ca)")
	}

	caBytes, err := os.ReadFile(tlsCAPath)
	if err != nil {
		return nil, fmt.Errorf("read CA cert: %w", err)
	}
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caBytes) {
		return nil, fmt.Errorf("invalid CA certificate")
	}

	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS13,
		RootCAs:    caPool,
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	if err != nil {
		return nil, fmt.Errorf("dial cobac: %w", err)
	}
	defer conn.Close()

	client := provisionpb.NewShareServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if jwtToken != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+jwtToken)
	}

	resp, err := client.GetShare(ctx, &provisionpb.GetShareRequest{
		InstitutionId: institution,
		NodeId:        node,
	})
	if err != nil {
		return nil, fmt.Errorf("get share: %w", err)
	}

	return resp.Share, nil
}
