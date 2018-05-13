package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/emitio/emitio-forwarder-mock/pkg/emitio"
)

func main() {
	ctx := context.Background()
	l, _ := zap.NewDevelopment()
	zap.ReplaceGlobals(l)
	const target = "unix:/var/run/emitio/emitio.sock"
	conn, err := grpc.DialContext(ctx, target, grpc.WithInsecure())
	if err != nil {
		zap.L().With(zap.Error(err)).With(zap.String("target", target)).Fatal("dialing grpc")
	}
	client := emitio.NewEmitIOClient(conn)
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return runTickerProducer(ctx, client)
	})
	eg.Go(func() error {
		return runStdinProducer(ctx, client)
	})
	err = eg.Wait()
	if err != nil {
		zap.L().With(zap.Error(err)).Fatal("error from producer")
	}
}

func runStdinProducer(ctx context.Context, client emitio.EmitIOClient) error {
	stream, err := client.Emit(ctx)
	if err != nil {
		return err
	}
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		// reader
		for {
			output, err := stream.Recv()
			if err != nil {
				return err
			}
			fmt.Println(output)
		}
	})
	eg.Go(func() error {
		// writer
		// send required header first
		err := stream.Send(&emitio.EmitInput{
			Inputs: &emitio.EmitInput_Header{
				Header: &emitio.EmitHeader{
					Name: "stdin",
				},
			},
		})
		if err != nil {
			return err
		}
		// send optional metadata
		err = stream.Send(&emitio.EmitInput{
			Inputs: &emitio.EmitInput_Meta{
				Meta: &emitio.EmitMeta{
					Meta: map[string]string{
						"environment": "dev",
					},
				},
			},
		})
		if err != nil {
			return err
		}
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			err := stream.Send(&emitio.EmitInput{
				Inputs: &emitio.EmitInput_Body{
					Body: &emitio.EmitBody{
						Body: scanner.Bytes(),
					},
				},
			})
			if err != nil {
				return err
			}
		}
		if err := scanner.Err(); err != nil {
			return err
		}
		return nil
	})
	return eg.Wait()
}

func runTickerProducer(ctx context.Context, client emitio.EmitIOClient) error {
	stream, err := client.Emit(ctx)
	if err != nil {
		return err
	}
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		// reader
		for {
			output, err := stream.Recv()
			if err != nil {
				return err
			}
			fmt.Println(output)
		}
	})
	eg.Go(func() error {
		const period = time.Second
		t := time.NewTicker(period)
		defer t.Stop()
		// writer
		// send required header first
		err := stream.Send(&emitio.EmitInput{
			Inputs: &emitio.EmitInput_Header{
				Header: &emitio.EmitHeader{
					Name: "ticker",
				},
			},
		})
		if err != nil {
			return err
		}
		// send optional metadata
		err = stream.Send(&emitio.EmitInput{
			Inputs: &emitio.EmitInput_Meta{
				Meta: &emitio.EmitMeta{
					Meta: map[string]string{
						"environment": "dev",
						"period":      fmt.Sprintf("%s", period),
					},
				},
			},
		})
		if err != nil {
			return err
		}
		for {
			select {
			case <-ctx.Done():
				return nil
			case at := <-t.C:
				err := stream.Send(&emitio.EmitInput{
					Inputs: &emitio.EmitInput_Body{
						Body: &emitio.EmitBody{
							Body: []byte(fmt.Sprintf("hello from a message generated at %s", at)),
						},
					},
				})
				if err != nil {
					return err
				}
			}
		}
	})
	return eg.Wait()
}
