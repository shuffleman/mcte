package bedrock

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

// Bedrock 协议版本（仅 1.21.4+）。
const (
	BedrockProtocolV1_21_4 int32 = 766
	BedrockProtocolV1_21_5 int32 = 776
	BedrockProtocolV1_21_6 int32 = 786
	BedrockProtocolV1_21_8 int32 = 800
	MinBedrockProtocol           = BedrockProtocolV1_21_4
)

// IsSupportedBedrockProtocol 是否支持。
func IsSupportedBedrockProtocol(p int32) bool {
	return p >= MinBedrockProtocol
}

// LoginPacket Bedrock Login。
type LoginPacket struct {
	ProtocolVersion int32
	IdentityChain   []string
	ClientData      string
	ClientClaims    map[string]any
	ExtraData       map[string]any // identityChain 最后一段的 extraData（含 displayName/identity/XUID）
}

// ParseLogin 解析批包内的 Login 单包 body（header + payload）。
// payload 由 EncodeLogin 的反向定义；header 是 VarUint32（包 ID 在低 10 位）。
func ParseLogin(pkt []byte) (*LoginPacket, error) {
	hdr, n, err := ReadVarUint32(pkt)
	if err != nil {
		return nil, err
	}
	if hdr&0x3FF != PacketLogin {
		return nil, errors.New("bedrock: not a login packet")
	}
	body := pkt[n:]
	if len(body) < 4 {
		return nil, errors.New("bedrock: login body too short")
	}
	proto := int32(binary.BigEndian.Uint32(body[:4]))
	body = body[4:]

	payloadLen, m, err := ReadVarUint32(body)
	if err != nil {
		return nil, err
	}
	body = body[m:]
	if int(payloadLen) > len(body) {
		return nil, errors.New("bedrock: login payload truncated")
	}
	body = body[:payloadLen]

	if len(body) < 4 {
		return nil, errors.New("bedrock: chain len missing")
	}
	chainLen := int(binary.LittleEndian.Uint32(body[:4]))
	body = body[4:]
	if chainLen > len(body) {
		return nil, errors.New("bedrock: chain truncated")
	}
	chainJSON := body[:chainLen]
	body = body[chainLen:]

	var chainStruct struct {
		Chain []string `json:"chain"`
	}
	if err := json.Unmarshal(chainJSON, &chainStruct); err != nil {
		return nil, err
	}

	if len(body) < 4 {
		return nil, errors.New("bedrock: clientdata len missing")
	}
	cdLen := int(binary.LittleEndian.Uint32(body[:4]))
	body = body[4:]
	if cdLen > len(body) {
		return nil, errors.New("bedrock: clientdata truncated")
	}
	clientDataJWT := string(body[:cdLen])

	// 解析 clientData JWT claims（不验签）
	cdClaims := jwt.MapClaims{}
	if parts := strings.Split(clientDataJWT, "."); len(parts) == 3 {
		_, _, _ = jwt.NewParser(jwt.WithoutClaimsValidation()).ParseUnverified(clientDataJWT, cdClaims)
	}

	// 解析 chain 最后一段以拿到 extraData（包含玩家身份）
	var extra map[string]any
	if len(chainStruct.Chain) > 0 {
		lastJWT := chainStruct.Chain[len(chainStruct.Chain)-1]
		c := jwt.MapClaims{}
		if _, _, err := jwt.NewParser(jwt.WithoutClaimsValidation()).ParseUnverified(lastJWT, c); err == nil {
			if ed, ok := c["extraData"].(map[string]any); ok {
				extra = ed
			}
		}
	}

	return &LoginPacket{
		ProtocolVersion: proto,
		IdentityChain:   chainStruct.Chain,
		ClientData:      clientDataJWT,
		ClientClaims:    cdClaims,
		ExtraData:       extra,
	}, nil
}

// TokenFromClientData 从 clientData claim 中取自定义 token 字段。
func TokenFromClientData(lp *LoginPacket, field string) string {
	if lp == nil || lp.ClientClaims == nil {
		return ""
	}
	if v, ok := lp.ClientClaims[field].(string); ok {
		return v
	}
	return ""
}

// TargetFromClientData 从 clientData 中取隧道目标信息（host:port）。
// 客户端约定字段：MCTETarget = "host:port"。
func TargetFromClientData(lp *LoginPacket, field string) (host string, port uint16) {
	if lp == nil || lp.ClientClaims == nil {
		return "", 0
	}
	v, _ := lp.ClientClaims[field].(string)
	if v == "" {
		return "", 0
	}
	for i := len(v) - 1; i >= 0; i-- {
		if v[i] == ':' {
			host = v[:i]
			var p uint32
			for _, c := range v[i+1:] {
				if c < '0' || c > '9' {
					return "", 0
				}
				p = p*10 + uint32(c-'0')
			}
			if p > 0xFFFF {
				return "", 0
			}
			return host, uint16(p)
		}
	}
	return "", 0
}
