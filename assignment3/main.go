import (
	proto "timeapp/grpc/proto"
	"context"
	"log"
	"net"
	"errors"
	"time"
	"google.golang.org/grpc"
)

type timeServer struct {
	proto.UnimplementedTimeKeeperServer
}

func (s timeServer) GetTime(c context.Context, e *proto.Empty) (*proto.TimeResponse, error) {
	now := time.Now()
	nanos := now.UnixNano()
	return &proto.TimeResponse{
		UnixNanos: nanos,
	}, nil
}
