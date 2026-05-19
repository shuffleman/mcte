package singbox

// 注册说明：
//
// sing-box 的 inbound/outbound 都通过 Registry 注册。在 sing-box fork 中：
//
// 1. inbound.go:
//
//    inbound.Register[singbox.Options](registry, "minecraft", func(ctx, router, log, tag, opts) (adapter.Inbound, error) {
//        return mcteSingbox.New(opts, log, &singboxRouteAdapter{router})
//    })
//
//    type singboxRouteAdapter struct{ r adapter.ConnectionRouterEx }
//    func (a *singboxRouteAdapter) RouteConnection(ctx context.Context, conn net.Conn, host string, port uint16) error {
//        meta := adapter.InboundContext{
//            Destination: M.ParseSocksaddrHostPort(host, port),
//        }
//        a.r.RouteConnectionEx(ctx, conn, meta, nil)
//        return nil
//    }
//
// 2. outbound.go:
//
//    outbound.Register[singbox.OutboundOptions](registry, "minecraft", func(ctx, router, log, tag, opts) (adapter.Outbound, error) {
//        impl, err := mcteSingbox.NewOutbound(opts, log)
//        if err != nil { return nil, err }
//        return &mcteOutboundAdapter{outbound.NewAdapter("minecraft", tag, []string{"tcp", "udp"}), impl}, nil
//    })
//
//    type mcteOutboundAdapter struct {
//        outbound.Adapter
//        impl *mcteSingbox.Outbound
//    }
//    func (h *mcteOutboundAdapter) DialContext(ctx context.Context, network string, dest M.Socksaddr) (net.Conn, error) {
//        return h.impl.DialContext(ctx, dest.AddrString(), uint16(dest.Port))
//    }
//
// 完整示例参见 docs/singbox-integration.md（如有）。
func init() {}
