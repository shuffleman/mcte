package java

import (
	"crypto/aes"
	"crypto/cipher"
	"io"
)

// cfb8Stream 是 AES-CFB8 单向 stream。
type cfb8Stream struct {
	block   cipher.Block
	iv      []byte
	encrypt bool
}

func newCFB8(block cipher.Block, iv []byte, encrypt bool) *cfb8Stream {
	cp := make([]byte, len(iv))
	copy(cp, iv)
	return &cfb8Stream{block: block, iv: cp, encrypt: encrypt}
}

func (s *cfb8Stream) XORKeyStream(dst, src []byte) {
	bs := s.block.BlockSize()
	tmp := make([]byte, bs)
	for i := 0; i < len(src); i++ {
		s.block.Encrypt(tmp, s.iv)
		c := src[i] ^ tmp[0]
		if s.encrypt {
			copy(s.iv, s.iv[1:])
			s.iv[bs-1] = c
			dst[i] = c
		} else {
			copy(s.iv, s.iv[1:])
			s.iv[bs-1] = src[i]
			dst[i] = c
		}
	}
}

// CryptoStream 封装一对 AES-CFB8 stream（加密/解密）。
type CryptoStream struct {
	enc *cfb8Stream
	dec *cfb8Stream
}

// NewCryptoStream 用同一把 key（恰好 16 字节）和 iv（与 key 相同）构造。
func NewCryptoStream(key []byte) (*CryptoStream, error) {
	blockEnc, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	blockDec, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return &CryptoStream{
		enc: newCFB8(blockEnc, key, true),
		dec: newCFB8(blockDec, key, false),
	}, nil
}

// EncryptReader 返回加密的 Reader 包装。
func (c *CryptoStream) DecryptReader(r io.Reader) io.Reader {
	return &cryptoReader{r: r, stream: c.dec}
}

// EncryptWriter 返回加密的 Writer 包装。
func (c *CryptoStream) EncryptWriter(w io.Writer) io.Writer {
	return &cryptoWriter{w: w, stream: c.enc}
}

type cryptoReader struct {
	r      io.Reader
	stream *cfb8Stream
}

func (c *cryptoReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if n > 0 {
		c.stream.XORKeyStream(p[:n], p[:n])
	}
	return n, err
}

type cryptoWriter struct {
	w      io.Writer
	stream *cfb8Stream
}

func (c *cryptoWriter) Write(p []byte) (int, error) {
	buf := make([]byte, len(p))
	c.stream.XORKeyStream(buf, p)
	return c.w.Write(buf)
}
