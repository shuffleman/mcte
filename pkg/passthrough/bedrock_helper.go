package passthrough

import "github.com/shuffleman/mcte/pkg/protocol/bedrock"

// assembleBatch 用 zlib 重新打包一组 packet 为 game packet。
func assembleBatch(pkts [][]byte) ([]byte, error) {
	return bedrock.AssembleGamePacket(bedrock.CompZlib, pkts)
}
