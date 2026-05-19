package detector

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/shuffleman/mcte/pkg/auth"
	"github.com/shuffleman/mcte/pkg/protocol/bedrock"
	"github.com/shuffleman/mcte/pkg/protocol/bedrock/raknet"
)

// BedrockDetectOption Bedrock 侧识别选项。
type BedrockDetectOption struct {
	Validator   *auth.Validator
	UUIDField   string // clientData claim 中 UUID 字段名，默认 MCTEUuid
	TargetField string // 携带 host:port 的字段名
}

// BedrockResult Bedrock 识别结果。
type BedrockResult struct {
	Kind      ClientKind
	Login     *bedrock.LoginPacket
	LoginRaw  []byte
	BatchTail [][]byte
	Target    string
	Port      uint16
	User      *auth.User
}

// DetectBedrock 从 RakNet Session 读取首个含 Login 的 game packet 并完成识别。
func DetectBedrock(ctx context.Context, s *raknet.Session, opt BedrockDetectOption) (*BedrockResult, error) {
	rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	for {
		body, err := s.ReadApp(rctx)
		if err != nil {
			return nil, err
		}
		if len(body) == 0 || body[0] != bedrock.IDGamePacket {
			continue
		}
		decompressed, err := bedrock.DecompressBatch(body[1:])
		if err != nil {
			return nil, err
		}
		packets, err := bedrock.SplitBatchPackets(decompressed)
		if err != nil {
			return nil, err
		}
		if len(packets) == 0 {
			continue
		}
		lp, err := bedrock.ParseLogin(packets[0])
		if err != nil {
			return &BedrockResult{Kind: KindMC, BatchTail: packets}, nil
		}
		res := &BedrockResult{
			Login:     lp,
			LoginRaw:  packets[0],
			BatchTail: packets[1:],
		}
		uuidField := opt.UUIDField
		if uuidField == "" {
			uuidField = "MCTEUuid"
		}
		tf := opt.TargetField
		if tf == "" {
			tf = "MCTETarget"
		}
		uuidStr := bedrock.TokenFromClientData(lp, uuidField)
		if uuidStr == "" {
			res.Kind = KindMC
			return res, nil
		}
		id, err := uuid.Parse(uuidStr)
		if err != nil {
			res.Kind = KindMC
			return res, nil
		}
		if opt.Validator == nil {
			res.Kind = KindMC
			return res, nil
		}
		user := opt.Validator.Lookup(id)
		if user == nil {
			res.Kind = KindMC
			return res, nil
		}
		res.Kind = KindTunnel
		res.User = user
		res.Target, res.Port = bedrock.TargetFromClientData(lp, tf)
		if res.Target == "" || res.Port == 0 {
			return nil, errors.New("bedrock: tunnel target missing in client data")
		}
		return res, nil
	}
}
