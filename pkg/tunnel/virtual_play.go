package tunnel

import (
	"github.com/google/uuid"

	"github.com/shuffleman/mcte/pkg/protocol/java"
)

// EnterPlayState 向客户端发送让其进入 Play 状态所需的最小包序列。
// fr 必须已经在 Login 状态；此函数返回前 fr 已切换到 Play 状态语义（客户端进入 Play）。
//
// 必须按版本号分支处理 1.18+ vs 1.17- 的 ChunkData；本项目仅支持 1.20.6+，统一走现代格式。
func EnterPlayState(fr *java.Framer, proto int32, username string, uid uuid.UUID) error {
	// LoginSuccess
	{
		id := java.PacketID(proto, java.StateLogin, java.ClientBound, java.PLoginSuccess)
		var u [16]byte
		copy(u[:], uid[:])
		payload := java.EncodeLoginSuccess(&java.LoginSuccess{
			UUID:      u,
			Username:  username,
			StrictErr: false,
		})
		if err := fr.WritePacket(java.Packet{ID: id, Payload: payload}); err != nil {
			return err
		}
	}

	// 期待客户端发 LoginAcknowledged（1.20.2+），但隧道客户端可省略此校验。
	// 进入 Configuration 状态，立即发送 FinishConfiguration。
	{
		id := java.PacketID(proto, java.StateConfiguration, java.ClientBound, java.PFinishConfiguration)
		if err := fr.WritePacket(java.Packet{ID: id, Payload: nil}); err != nil {
			return err
		}
	}

	// 进入 Play 状态：发 PlayLogin（最小可用）
	{
		id := java.PacketID(proto, java.StatePlay, java.ClientBound, java.PPlayLogin)
		payload := java.EncodePlayLogin(&java.PlayLogin{
			EntityID:            1,
			IsHardcore:          false,
			Dimensions:          []string{"minecraft:overworld"},
			MaxPlayers:          1,
			ViewDistance:        2,
			SimulationDistance:  2,
			ReducedDebugInfo:    true,
			EnableRespawnScreen: false,
			DoLimitedCrafting:   false,
			DimensionType:       0,
			DimensionName:       "minecraft:overworld",
			HashedSeed:          0,
			Gamemode:            3, // Spectator
			PreviousGamemode:    0xFF,
			IsDebug:             false,
			IsFlat:              true,
			HasDeathLocation:    false,
			PortalCooldown:      0,
			EnforcesSecureChat:  false,
		})
		if err := fr.WritePacket(java.Packet{ID: id, Payload: payload}); err != nil {
			return err
		}
	}

	// SynchronizePlayerPosition
	{
		id := java.PacketID(proto, java.StatePlay, java.ClientBound, java.PSyncPlayerPosition)
		payload := java.EncodeSyncPlayerPosition(&java.SyncPlayerPosition{
			X: 0, Y: 64, Z: 0, Yaw: 0, Pitch: 0, Flags: 0, TeleportID: 1,
		})
		if err := fr.WritePacket(java.Packet{ID: id, Payload: payload}); err != nil {
			return err
		}
	}

	// ChunkData (0,0)
	{
		id := java.PacketID(proto, java.StatePlay, java.ClientBound, java.PChunkData)
		// 1.18+ 现代世界默认 24 个 section（-64..320 / 16）
		payload := java.EncodeChunkData(&java.ChunkData{ChunkX: 0, ChunkZ: 0}, 24)
		if err := fr.WritePacket(java.Packet{ID: id, Payload: payload}); err != nil {
			return err
		}
	}

	// GameEvent: world loaded（event 13 / value 0）
	{
		id := java.PacketID(proto, java.StatePlay, java.ClientBound, java.PGameEvent)
		payload := java.EncodeGameEvent(&java.GameEvent{Event: 13, Value: 0})
		if err := fr.WritePacket(java.Packet{ID: id, Payload: payload}); err != nil {
			return err
		}
	}
	return nil
}
