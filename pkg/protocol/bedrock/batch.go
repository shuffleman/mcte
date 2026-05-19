package bedrock

import (
	"bytes"
	"compress/zlib"
	"errors"
	"io"

	"github.com/golang/snappy"
)

// CompressionAlgo Bedrock 批包压缩算法。
type CompressionAlgo byte

const (
	CompZlib   CompressionAlgo = 0
	CompSnappy CompressionAlgo = 1
	CompNone   CompressionAlgo = 0xFF
)

// 0xFE Game Packet 内部：1B compression + 批包数据。
// 1.20.60 起首字节为压缩算法标识；之前默认 zlib。

// DecompressBatch 解压一个 game packet body（不含首字节 0xFE）。
// 首字节为压缩算法标识：0 zlib / 1 snappy / 0xFF none。
func DecompressBatch(buf []byte) ([]byte, error) {
	if len(buf) < 1 {
		return nil, io.ErrUnexpectedEOF
	}
	algo := CompressionAlgo(buf[0])
	payload := buf[1:]
	switch algo {
	case CompNone:
		return payload, nil
	case CompSnappy:
		return snappy.Decode(nil, payload)
	case CompZlib:
		zr, err := zlib.NewReader(bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		defer zr.Close()
		var out bytes.Buffer
		if _, err := io.Copy(&out, zr); err != nil {
			return nil, err
		}
		return out.Bytes(), nil
	}
	return nil, errors.New("bedrock: unknown compression algo")
}

// CompressBatch 用指定算法压缩。
func CompressBatch(algo CompressionAlgo, payload []byte) ([]byte, error) {
	switch algo {
	case CompNone:
		out := make([]byte, 1+len(payload))
		out[0] = byte(CompNone)
		copy(out[1:], payload)
		return out, nil
	case CompSnappy:
		compressed := snappy.Encode(nil, payload)
		out := make([]byte, 1+len(compressed))
		out[0] = byte(CompSnappy)
		copy(out[1:], compressed)
		return out, nil
	case CompZlib:
		var buf bytes.Buffer
		buf.WriteByte(byte(CompZlib))
		zw, _ := zlib.NewWriterLevel(&buf, zlib.BestSpeed)
		if _, err := zw.Write(payload); err != nil {
			_ = zw.Close()
			return nil, err
		}
		if err := zw.Close(); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}
	return nil, errors.New("bedrock: unknown compression algo")
}

// SplitBatchPackets 拆开批包：内部是若干 VarUint32 长度前缀的 packet。
func SplitBatchPackets(buf []byte) ([][]byte, error) {
	var out [][]byte
	for len(buf) > 0 {
		n, m, err := ReadVarUint32(buf)
		if err != nil {
			return nil, err
		}
		buf = buf[m:]
		if int(n) > len(buf) {
			return nil, io.ErrUnexpectedEOF
		}
		out = append(out, buf[:n])
		buf = buf[n:]
	}
	return out, nil
}

// AssembleGamePacket 将一组（packetID, payload）压缩为 0xFE Game Packet 字节序列。
func AssembleGamePacket(algo CompressionAlgo, packets [][]byte) ([]byte, error) {
	var batch []byte
	for _, p := range packets {
		batch = AppendVarUint32(batch, uint32(len(p)))
		batch = append(batch, p...)
	}
	compressed, err := CompressBatch(algo, batch)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, 1+len(compressed))
	out = append(out, 0xFE)
	out = append(out, compressed...)
	return out, nil
}

// AssembleSingle 仅打包一个 (packetID, payload) 为 game packet。
func AssembleSingle(algo CompressionAlgo, packetID uint32, payload []byte) ([]byte, error) {
	one := EncodePacketIntoBatch(nil, packetID, payload)
	// EncodePacketIntoBatch 把长度前缀写入；为对齐 AssembleGamePacket 行为，去掉长度前缀重组：
	hdr := PackHeader(packetID)
	pkt := make([]byte, 0, len(hdr)+len(payload))
	pkt = append(pkt, hdr...)
	pkt = append(pkt, payload...)
	_ = one
	return AssembleGamePacket(algo, [][]byte{pkt})
}
