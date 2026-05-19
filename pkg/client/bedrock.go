package client

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"net"
	"strconv"

	"github.com/google/uuid"

	"github.com/shuffleman/mcte/pkg/protocol/bedrock"
	"github.com/shuffleman/mcte/pkg/protocol/bedrock/raknet"
)

// dialBedrock 完成 Bedrock 隧道握手。
func (c *Client) dialBedrock(ctx context.Context, destHost string, destPort uint16) (net.Conn, error) {
	target := net.JoinHostPort(c.cfg.Server, strconv.Itoa(int(c.cfg.Port)))
	sess, err := raknet.Dial(ctx, target)
	if err != nil {
		return nil, err
	}

	// 构造一个最小 Login 包：clientData JWT claim 中带 UUID 与 target
	loginPayload, err := buildBedrockLoginPayload(c.id, destHost, destPort, c.cfg.UUIDField, c.cfg.TargetField)
	if err != nil {
		_ = sess.Close()
		return nil, err
	}
	gp, err := bedrock.AssembleSingle(bedrock.CompZlib, bedrock.PacketLogin, loginPayload)
	if err != nil {
		_ = sess.Close()
		return nil, err
	}
	if err := sess.WriteAppCtx(ctx, gp); err != nil {
		_ = sess.Close()
		return nil, err
	}

	// 等 PlayStatus(LoginSuccess)：跳过其他包
	for {
		body, rerr := sess.ReadApp(ctx)
		if rerr != nil {
			_ = sess.Close()
			return nil, rerr
		}
		if len(body) == 0 || body[0] != bedrock.IDGamePacket {
			continue
		}
		break // 简化：拿到一个 game packet 视为已 ready
	}

	return newBedrockBridgeConn(ctx, sess), nil
}

// buildBedrockLoginPayload 构造一个最小可解析的 Login payload：
//   proto BE int32 + payloadLen VarUint32 + chainLen LE int32 + chainJSON + cdLen LE int32 + clientDataJWT
// chainJSON：{"chain":["<base64.unsigned-jwt>"]}
// clientDataJWT：包含 MCTEUuid + MCTETarget 的未签名 JWT。
func buildBedrockLoginPayload(id uuid.UUID, host string, port uint16, uuidField, targetField string) ([]byte, error) {
	// JWT 头/payload base64
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))

	chainClaims := map[string]any{
		"extraData": map[string]any{
			"displayName": "mcte-client",
			"identity":    id.String(),
			"XUID":        "0",
		},
	}
	chainBytes, _ := json.Marshal(chainClaims)
	chainPayload := base64.RawURLEncoding.EncodeToString(chainBytes)
	chainJWT := header + "." + chainPayload + "."

	chain := map[string][]string{"chain": {chainJWT}}
	chainJSON, _ := json.Marshal(chain)

	cdClaims := map[string]any{
		uuidField:   id.String(),
		targetField: host + ":" + strconv.Itoa(int(port)),
	}
	cdBytes, _ := json.Marshal(cdClaims)
	cdPayload := base64.RawURLEncoding.EncodeToString(cdBytes)
	cdJWT := header + "." + cdPayload + "."

	// 拼装 payload
	body := make([]byte, 0, 8+len(chainJSON)+len(cdJWT))
	body = binary.BigEndian.AppendUint32(body, uint32(bedrock.BedrockProtocolV1_21_4))
	// 内层 payloadLen 与内容
	var inner []byte
	inner = binary.LittleEndian.AppendUint32(inner, uint32(len(chainJSON)))
	inner = append(inner, chainJSON...)
	inner = binary.LittleEndian.AppendUint32(inner, uint32(len(cdJWT)))
	inner = append(inner, cdJWT...)
	body = bedrock.AppendVarUint32(body, uint32(len(inner)))
	body = append(body, inner...)
	return body, nil
}
