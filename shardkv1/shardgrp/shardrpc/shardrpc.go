package shardrpc

import (
	"github.com/shrtyk/raft/kvsrv1/rpc"
	"github.com/shrtyk/raft/shardkv1/shardcfg"
)

type FreezeShardArgs struct {
	Shard shardcfg.Tshid
	Num   shardcfg.Tnum
}

type FreezeShardReply struct {
	State []byte
	Num   shardcfg.Tnum
	Err   rpc.Err
}

type InstallShardArgs struct {
	Shard shardcfg.Tshid
	State []byte
	Num   shardcfg.Tnum
}

type InstallShardReply struct {
	Err rpc.Err
}

type DeleteShardArgs struct {
	Shard shardcfg.Tshid
	Num   shardcfg.Tnum
}

type DeleteShardReply struct {
	Err rpc.Err
}
