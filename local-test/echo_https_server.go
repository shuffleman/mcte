// 简易 HTTPS echo server，自签证书，监听 127.0.0.1:8443。
// 收到 HTTP 请求时返回 client IP + 一段确认字节。
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"os"
	"time"
)

func main() {
	cert, key, err := makeSelfSigned()
	if err != nil {
		log.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintf(w, "OK\nremote=%s\npath=%s\nheader-host=%s\n",
			r.RemoteAddr, r.URL.Path, r.Host)
		// 给一段足够长的 payload 触发分片
		filler := make([]byte, 8192)
		for i := range filler {
			filler[i] = byte('A' + i%26)
		}
		w.Write(filler)
	})
	mux.HandleFunc("/large", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		buf := make([]byte, 1024)
		for i := range buf {
			buf[i] = byte(i)
		}
		// 写 64KB 用于压力测试
		for i := 0; i < 64; i++ {
			w.Write(buf)
		}
	})
	mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", r.Header.Get("Content-Type"))
		io.Copy(w, r.Body)
	})

	srv := &http.Server{
		Addr:    "127.0.0.1:8443",
		Handler: mux,
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{
				{
					Certificate: [][]byte{cert.Raw},
					PrivateKey:  key,
				},
			},
		},
	}

	fmt.Println("echo-https listening on https://127.0.0.1:8443/")
	if err := srv.ListenAndServeTLS("", ""); err != nil {
		fmt.Fprintln(os.Stderr, "serve:", err)
	}
}

func makeSelfSigned() (*x509.Certificate, *ecdsa.PrivateKey, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  nil,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, nil, err
	}
	// 顺便打印证书指纹方便调试
	_ = pem.EncodeToMemory
	return cert, key, nil
}
