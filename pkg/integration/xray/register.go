package xray

// 注册说明：
//
// 在 Xray fork 中，你需要在 main 或 init 包里写一个胶水文件来完成两件事：
//
// 1. JSON config 解析：把 streamSettings 中的 mcteSettings 解析成 MCTEConfig，
//    或者把 outbound settings 解析成 MCTEClientConfig。
//
// 2. inbound / outbound 注册：
//
//    // inbound
//    common.Must(common.RegisterConfig((*MCTEInboundConfig)(nil), func(ctx, cfg) (any, error) {
//        var conf MCTEConfig // 从你的 protobuf/json 解析
//        return mcteXray.NewServer(conf, logger, &xrayDispatcherAdapter{routing.DispatcherFromContext(ctx)})
//    }))
//
//    // outbound
//    common.Must(common.RegisterConfig((*MCTEOutboundConfig)(nil), func(ctx, cfg) (any, error) {
//        var conf MCTEClientConfig
//        return &XrayOutbound{client: mcteXray.NewClient(conf, logger)}, nil
//    }))
//
// 3. Dispatcher 适配：
//
//    type xrayDispatcherAdapter struct{ d routing.Dispatcher }
//    func (a *xrayDispatcherAdapter) Dispatch(ctx context.Context, conn net.Conn, host string, port uint16) error {
//        link, err := a.d.Dispatch(ctx, net.Destination{
//            Network: net.Network_TCP,
//            Address: net.ParseAddress(host),
//            Port:    net.Port(port),
//        })
//        if err != nil { return err }
//        // 双向桥接 conn ↔ link.Reader/link.Writer
//        return bridgeConnLink(conn, link)
//    }
//
// 4. Xray Outbound.Process 调用：
//
//    func (o *XrayOutbound) Process(ctx context.Context, link *transport.Link, dialer internet.Dialer) error {
//        ob := session.OutboundFromContext(ctx)
//        host := ob.Target.Address.Domain()
//        return o.client.Process(ctx, link.Reader, link.Writer, host, uint16(ob.Target.Port))
//    }
//
// 完整示例见 docs/xray-integration.md（如有）。
func init() {}
