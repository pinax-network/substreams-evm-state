// Test-only local transport adapter. The production CLI sends S2-compressed
// gRPC requests; grpc-python supports gzip/deflate but rejects S2 before dispatch.
// This adapter decodes S2 with the CLI's own library and forwards unchanged
// protobuf bytes to the Python fault-injection server over uncompressed gRPC.
package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"

	_ "github.com/mostynb/go-grpc-compression/experimental/s2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type rawCodec struct{}

func (rawCodec) Name() string                      { return "proto" }
func (rawCodec) Marshal(value any) ([]byte, error) { return *value.(*[]byte), nil }
func (rawCodec) Unmarshal(data []byte, value any) error {
	*value.(*[]byte) = append([]byte(nil), data...)
	return nil
}

func main() {
	target := flag.String("target", "", "local Python test server")
	flag.Parse()
	conn, err := grpc.NewClient(*target, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(rawCodec{}), grpc.MaxCallRecvMsgSize(64<<20)))
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	server := grpc.NewServer(grpc.ForceServerCodec(rawCodec{}), grpc.MaxRecvMsgSize(64<<20),
		grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
			method, _ := grpc.MethodFromServerStream(stream)
			if method != "/sf.substreams.rpc.v2.Stream/Blocks" {
				return status.Error(codes.Unimplemented, "only the Blocks test method is supported")
			}
			var request []byte
			if err := stream.RecvMsg(&request); err != nil {
				return err
			}
			backend, err := conn.NewStream(stream.Context(), &grpc.StreamDesc{ServerStreams: true}, method)
			if err != nil {
				return err
			}
			if err := backend.SendMsg(&request); err != nil {
				return err
			}
			if err := backend.CloseSend(); err != nil {
				return err
			}
			for {
				var response []byte
				if err := backend.RecvMsg(&response); err == io.EOF {
					return nil
				} else if err != nil {
					return err
				}
				if err := stream.SendMsg(&response); err != nil {
					return err
				}
			}
		}))
	fmt.Fprintln(os.Stdout, listener.Addr().String())
	if err := server.Serve(listener); err != nil {
		panic(err)
	}
}
