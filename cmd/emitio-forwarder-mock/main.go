package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"math/rand"
	"os"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/golang/protobuf/ptypes"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/emitio/emitio-forwarder-mock/pkg/emitio/v1"
)

func main() {
	ctx := context.Background()
	l, _ := zap.NewDevelopment()
	zap.ReplaceGlobals(l)
	const target = "127.0.0.1:3648"
	zap.L().With(zap.String("target", target)).Info("dialing")
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

func spanID() [8]byte {
	id := rand.Uint64()
	var sid [8]byte
	binary.LittleEndian.PutUint64(sid[:], id)
	return sid
}

func runStdinProducer(ctx context.Context, client emitio.EmitIOClient) error {
	scanner := bufio.NewScanner(os.Stdin) // TODO replace with interruptable io.Reader that will error on ctx close
	var id uint64
	for scanner.Scan() {
		line := scanner.Text()
		id++
		sid := spanID()
		_, err := client.Emit(ctx, &emitio.EmitRequest{
			Spans: []*emitio.Span{
				&emitio.Span{
					SpanId: sid[:],
					Name: &emitio.TruncatableString{
						Value: "stdin line received",
					},
					EndTime:  ptypes.TimestampNow(),
					Duration: ptypes.DurationProto(time.Duration(0)),
					Attributes: &emitio.Span_Attributes{
						AttributeMap: map[string]*emitio.AttributeValue{
							"sample.bool.value": &emitio.AttributeValue{
								Value: &emitio.AttributeValue_BoolValue{
									BoolValue: true,
								},
							},
							"sample.string.value": &emitio.AttributeValue{
								Value: &emitio.AttributeValue_StringValue{
									StringValue: &emitio.TruncatableString{
										Value: "greetings",
									},
								},
							},
							"sample.int.value": &emitio.AttributeValue{
								Value: &emitio.AttributeValue_IntValue{
									IntValue: -74,
								},
							},
						},
					},
					TimeEvents: &emitio.Span_TimeEvents{
						TimeEvent: []*emitio.Span_TimeEvent{
							&emitio.Span_TimeEvent{
								Time: ptypes.TimestampNow(),
								Value: &emitio.Span_TimeEvent_Annotation_{
									Annotation: &emitio.Span_TimeEvent_Annotation{
										Description: &emitio.TruncatableString{
											Value: "stdin received",
										},
										Attributes: &emitio.Span_Attributes{
											AttributeMap: map[string]*emitio.AttributeValue{
												"stdin.line.length": &emitio.AttributeValue{
													Value: &emitio.AttributeValue_IntValue{
														IntValue: int64(len(line)),
													},
												},
											},
										},
									},
								},
							},
							&emitio.Span_TimeEvent{
								Time: ptypes.TimestampNow(),
								Value: &emitio.Span_TimeEvent_MessageEvent_{
									MessageEvent: &emitio.Span_TimeEvent_MessageEvent{
										Type:             emitio.Span_TimeEvent_MessageEvent_RECEIVED,
										Id:               id,
										UncompressedSize: uint64(len(line)),
									},
								},
							},
						},
					},
					Status: &emitio.Status{
						Code:    int32(codes.OK),
						Message: "read line from stdin",
					},
					Severity: emitio.Span_DEBUG,
					Unstructured: &emitio.TruncatableString{
						Value: line,
					},
				},
			},
		})
		if err != nil {
			return err
		}
	}
	err := scanner.Err()
	if err != nil {
		return err
	}
	return nil
}

func runTickerProducer(ctx context.Context, client emitio.EmitIOClient) error {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	var id uint64
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			id++
			sid := spanID()
			_, err := client.Emit(ctx, &emitio.EmitRequest{
				Spans: []*emitio.Span{
					&emitio.Span{
						SpanId: sid[:],
						Name: &emitio.TruncatableString{
							Value: "ticker value produced",
						},
						EndTime:  ptypes.TimestampNow(),
						Duration: ptypes.DurationProto(time.Duration(0)),
						Attributes: &emitio.Span_Attributes{
							AttributeMap: map[string]*emitio.AttributeValue{
								"sample.bool.value": &emitio.AttributeValue{
									Value: &emitio.AttributeValue_BoolValue{
										BoolValue: true,
									},
								},
								"sample.string.value": &emitio.AttributeValue{
									Value: &emitio.AttributeValue_StringValue{
										StringValue: &emitio.TruncatableString{
											Value: "greetings",
										},
									},
								},
								"sample.int.value": &emitio.AttributeValue{
									Value: &emitio.AttributeValue_IntValue{
										IntValue: -74,
									},
								},
							},
						},
						Status: &emitio.Status{
							Code:    int32(codes.OK),
							Message: "read line from stdin",
						},
						Severity: emitio.Span_DEBUG,
					},
				},
			})
			if err != nil {
				return err
			}
		}
	}
}
