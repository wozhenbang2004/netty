/*
 * Copyright 2026 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.handler.codec.http2;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.ContinuationWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;
import io.netty.pkitesting.CertificateBuilder;
import io.netty.pkitesting.X509Bundle;
import io.netty.util.CharsetUtil;
import io.netty.util.NetUtil;
import io.netty.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.net.InetSocketAddress;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static io.netty.handler.codec.http.HttpHeaderNames.ORIGIN;
import static io.netty.handler.codec.http.HttpHeaderNames.SEC_WEBSOCKET_EXTENSIONS;
import static io.netty.handler.codec.http.HttpHeaderNames.SEC_WEBSOCKET_PROTOCOL;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Timeout(30)
class Http2WebSocketTransportTest {
    private static final int MAX_FRAME_SIZE = 65536;
    private static final int STREAM_WINDOW = 1024;

    private final EventLoopGroup group = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
    private final CompletableFuture<Http2Settings> settings = new CompletableFuture<Http2Settings>();
    private final CompletableFuture<Http2Headers> serverRequest = new CompletableFuture<Http2Headers>();
    private final CompletableFuture<Http2StreamChannel> serverStream = new CompletableFuture<Http2StreamChannel>();
    private final CompletableFuture<Integer> serverCloseCode = new CompletableFuture<Integer>();
    private final CompletableFuture<String> clientCloseReason = new CompletableFuture<String>();
    private final CompletableFuture<byte[]> binaryEcho = new CompletableFuture<byte[]>();
    private final BlockingQueue<String> textEchoes = new LinkedBlockingQueue<String>();
    private final Wire serverWire = new Wire();
    private final Wire clientWire = new Wire();
    private Channel listener;
    private Channel client;
    private Http2StreamChannel webSocket;
    private String scheme;
    private Http2Headers response;

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
        if (listener != null) {
            listener.close();
        }
        group.shutdownGracefully(0, 0, SECONDS).syncUninterruptibly();
    }

    @ParameterizedTest
    @CsvSource({ "false, false, false", "false, true, true", "false, true, false", "false, false, true",
                 "true, false, false", "true, true, true" })
    void handshakeEchoAndClose(boolean tls, boolean clientCompression, boolean serverCompression) throws Exception {
        start(tls, serverCompression, false);
        connectWebSocket(clientCompression);
        boolean compressed = clientCompression && serverCompression;
        assertThat(response.status().toString()).isEqualTo("200");
        assertThat(response.get(SEC_WEBSOCKET_PROTOCOL).toString()).isEqualTo("chat");
        assertThat(response.contains(SEC_WEBSOCKET_EXTENSIONS)).isEqualTo(compressed);
        assertThat(serverRequest.get().get(ORIGIN).toString()).isEqualTo("https://example.org");
        assertThat(serverRequest.get().contains(SEC_WEBSOCKET_EXTENSIONS)).isEqualTo(clientCompression);
        if (compressed) {
            assertThat(response.get(SEC_WEBSOCKET_EXTENSIONS).toString()).startsWith("permessage-deflate");
        }
        if (tls) {
            assertThat(client.pipeline().get(SslHandler.class).applicationProtocol())
                    .isEqualTo(ApplicationProtocolNames.HTTP_2);
            assertThat(serverStream.get().parent().pipeline().get(SslHandler.class).applicationProtocol())
                    .isEqualTo(ApplicationProtocolNames.HTTP_2);
        }

        webSocket.writeAndFlush(new TextWebSocketFrame(false, 0, "Hello ")).sync();
        webSocket.writeAndFlush(new ContinuationWebSocketFrame(true, 0, "world")).sync();
        assertThat(textEchoes.take()).isEqualTo("Hello ");
        assertThat(textEchoes.take()).isEqualTo("world");
        // FIN=0, opcode=TEXT, optional RSV1, then the mask bit in the second octet.
        int flags = compressed ? 0x4100 : 0x0100;
        assertThat(serverWire.firstHeader.get() & 0xff80).isEqualTo(flags | 0x80);
        assertThat(clientWire.firstHeader.get() & 0xff80).isEqualTo(flags);
        getHealth();

        webSocket.writeAndFlush(new CloseWebSocketFrame(1000, "finished")).sync();
        assertGracefulClose();
        assertThat(clientCloseReason.get()).isEqualTo("finished");
    }

    @Test
    void closingTheServerChannelCompletesTheWebSocketClosingHandshake() throws Exception {
        start(false, false, false);
        connectWebSocket(false);
        serverStream.get().close().sync();
        assertGracefulClose();
    }

    private void assertGracefulClose() throws Exception {
        webSocket.closeFuture().sync();
        serverStream.get().closeFuture().sync();
        assertThat(serverCloseCode.get()).isEqualTo(1000);
        assertThat(clientWire.endStream.isDone()).isTrue();
        assertThat(serverWire.endStream.isDone()).isTrue();
        assertThat(clientWire.reset.isDone()).isFalse();
        assertThat(serverWire.reset.isDone()).isFalse();
        getHealth();
    }

    @Test
    void webSocketFramesCanCrossAndShareDataFrames() throws Exception {
        start(false, false, false);
        connectWebSocket(false);
        ChannelHandlerContext wire = webSocket.pipeline().context(clientWire);
        // A single octet of the first frame, then its remaining bytes and a complete second frame.
        wire.writeAndFlush(new DefaultHttp2DataFrame(Unpooled.wrappedBuffer(new byte[] { (byte) 0x81 }))).sync();
        byte[] remaining = { (byte) 0x81, 1, 2, 3, 4, 'a' ^ 1,
                (byte) 0x81, (byte) 0x81, 9, 8, 7, 6, 'b' ^ 9 };
        wire.writeAndFlush(new DefaultHttp2DataFrame(Unpooled.wrappedBuffer(remaining))).sync();
        assertThat(textEchoes.take()).isEqualTo("a");
        assertThat(textEchoes.take()).isEqualTo("b");
        assertThat(serverWire.dataFrames.get()).isEqualTo(2);
    }

    @Test
    void slowReaderStallsOnlyItsOwnStream() throws Exception {
        start(false, false, true);
        connectWebSocket(false);
        byte[] payload = new byte[32 * STREAM_WINDOW];
        new Random(1).nextBytes(payload);
        ChannelFuture write = webSocket.writeAndFlush(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(payload)));
        // The parent still reads the network while the receiving stream has auto-read disabled.
        serverWire.firstHeader.get();
        getHealth();
        client.eventLoop().submit(() -> {
            Http2Connection connection = client.pipeline().get(Http2FrameCodec.class).connection();
            Http2Stream stream = connection.stream(webSocket.stream().id());
            Http2RemoteFlowController flow = connection.remote().flowController();
            assertThat(flow.windowSize(stream)).isZero();
            assertThat(flow.hasFlowControlled(stream)).isTrue();
            assertThat(write.isDone()).isFalse();
        }).sync();

        Http2StreamChannel slowReader = serverStream.get();
        slowReader.eventLoop().submit(() -> slowReader.config().setAutoRead(true)).sync();
        write.sync();
        assertThat(binaryEcho.get()).containsExactly(payload);
        assertThat(serverWire.dataFrames.get()).isGreaterThan(1);
    }

    @Test
    void resetOnlyClosesTheWebSocketStream() throws Exception {
        start(false, false, false);
        connectWebSocket(false);
        Http2StreamChannel peer = serverStream.get();
        peer.parent().writeAndFlush(new DefaultHttp2ResetFrame(Http2Error.CANCEL).stream(peer.stream())).sync();
        webSocket.closeFuture().sync();
        assertThat(clientWire.reset.get()).isEqualTo(Http2Error.CANCEL.code());
        getHealth();
    }

    private void start(boolean tls, boolean compression, boolean pauseReads) throws Exception {
        scheme = tls ? "https" : "http";
        final SslContext serverSsl;
        final SslContext clientSsl;
        if (tls) {
            assumeTrue(SslProvider.isAlpnSupported(SslProvider.JDK));
            X509Bundle cert = new CertificateBuilder().subject("cn=localhost")
                    .setIsCertificateAuthority(true).buildSelfSigned();
            ApplicationProtocolConfig alpn = new ApplicationProtocolConfig(ApplicationProtocolConfig.Protocol.ALPN,
                    ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                    ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT, ApplicationProtocolNames.HTTP_2);
            serverSsl = SslContextBuilder.forServer(cert.getKeyPair().getPrivate(), cert.getCertificatePath())
                    .sslProvider(SslProvider.JDK).applicationProtocolConfig(alpn).build();
            clientSsl = SslContextBuilder.forClient().trustManager(cert.getCertificatePath())
                    .sslProvider(SslProvider.JDK).applicationProtocolConfig(alpn).build();
        } else {
            clientSsl = null;
            serverSsl = null;
        }
        Http2Settings initial = new Http2Settings().connectProtocolEnabled(true).initialWindowSize(STREAM_WINDOW);
        Http2WebSocketHandshaker handshaker = new Http2WebSocketHandshaker(MAX_FRAME_SIZE, compression);
        listener = new ServerBootstrap().group(group).channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel ch) {
                        if (serverSsl != null) {
                            ch.pipeline().addLast(serverSsl.newHandler(ch.alloc()));
                        }
                        ch.pipeline().addLast(Http2FrameCodecBuilder.forServer().initialSettings(initial).build(),
                                serverWire, new Http2MultiplexHandler(new ChannelInitializer<Channel>() {
                                    @Override
                                    protected void initChannel(Channel stream) {
                                        stream.pipeline().addLast(new EchoServer(handshaker, pauseReads));
                                    }
                                }));
                    }
                }).bind(new InetSocketAddress(NetUtil.LOCALHOST4, 0)).sync().channel();

        Bootstrap bootstrap = new Bootstrap().group(group).channel(NioSocketChannel.class);
        client = bootstrap.handler(new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel ch) {
                if (clientSsl != null) {
                    int port = ((InetSocketAddress) listener.localAddress()).getPort();
                    ch.pipeline().addLast(clientSsl.newHandler(ch.alloc(), "localhost", port));
                }
                ch.pipeline().addLast(Http2FrameCodecBuilder.forClient().build(),
                        new Http2MultiplexHandler(new ChannelInitializer<Channel>() {
                            @Override
                            protected void initChannel(Channel stream) {
                                stream.close();
                            }
                        }), new SimpleChannelInboundHandler<Http2SettingsFrame>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, Http2SettingsFrame msg) {
                                settings.complete(msg.settings());
                            }
                        });
            }
        }).connect(listener.localAddress()).sync().channel();
        assertThat(settings.get().connectProtocolEnabled()).isTrue();
    }

    private void connectWebSocket(boolean compression) throws Exception {
        Http2WebSocketHandshaker handshaker = new Http2WebSocketHandshaker(MAX_FRAME_SIZE, compression);
        SimpleChannelInboundHandler<WebSocketFrame> echoes = new SimpleChannelInboundHandler<WebSocketFrame>() {
            @Override
            protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame msg) {
                if (msg instanceof TextWebSocketFrame || msg instanceof ContinuationWebSocketFrame) {
                    textEchoes.add(msg.content().toString(CharsetUtil.UTF_8));
                } else if (msg instanceof BinaryWebSocketFrame) {
                    binaryEcho.complete(ByteBufUtil.getBytes(msg.content()));
                } else if (msg instanceof CloseWebSocketFrame) {
                    clientCloseReason.complete(((CloseWebSocketFrame) msg).reasonText());
                }
            }
        };
        webSocket = new Http2StreamChannelBootstrap(client).handler(new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel stream) {
                stream.pipeline().addLast(clientWire, echoes);
            }
        }).open().sync().getNow();
        Http2Headers headers = new DefaultHttp2Headers().scheme(scheme).authority("localhost").path("/chat")
                .set(SEC_WEBSOCKET_PROTOCOL, "chat, superchat").set(ORIGIN, "https://example.org");
        Future<Http2Headers> handshake = webSocket.eventLoop().submit(
                () -> handshaker.connect(webSocket.pipeline().context(echoes), headers)).sync().getNow();
        response = handshake.sync().getNow();
    }

    private void getHealth() throws Exception {
        CompletableFuture<String> status = new CompletableFuture<String>();
        CompletableFuture<String> body = new CompletableFuture<String>();
        Http2StreamChannel stream = new Http2StreamChannelBootstrap(client)
                .handler(new SimpleChannelInboundHandler<Http2StreamFrame>() {
                    @Override
                    protected void channelRead0(ChannelHandlerContext ctx, Http2StreamFrame msg) {
                        if (msg instanceof Http2HeadersFrame) {
                            status.complete(((Http2HeadersFrame) msg).headers().status().toString());
                        } else if (msg instanceof Http2DataFrame && ((Http2DataFrame) msg).isEndStream()) {
                            body.complete(((Http2DataFrame) msg).content().toString(CharsetUtil.US_ASCII));
                        }
                    }
                }).open().sync().getNow();
        Http2Headers request = new DefaultHttp2Headers().method("GET").scheme(scheme)
                .authority("localhost").path("/health");
        stream.writeAndFlush(new DefaultHttp2HeadersFrame(request, true)).sync();
        assertThat(status.get()).isEqualTo("200");
        assertThat(body.get()).isEqualTo("ok");
        assertThat(stream.parent()).isSameAs(webSocket.parent());
        assertThat(stream.stream().id()).isNotEqualTo(webSocket.stream().id());
    }

    private final class EchoServer extends SimpleChannelInboundHandler<Object> {
        private final Http2WebSocketHandshaker handshaker;
        private final boolean pauseReads;

        EchoServer(Http2WebSocketHandshaker handshaker, boolean pauseReads) {
            this.handshaker = handshaker;
            this.pauseReads = pauseReads;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof Http2HeadersFrame) {
                Http2HeadersFrame request = (Http2HeadersFrame) msg;
                if (Http2WebSocketHandshaker.isWebSocketRequest(request.headers())) {
                    if (pauseReads) {
                        ctx.channel().config().setAutoRead(false);
                    }
                    serverRequest.complete(request.headers());
                    Http2Headers headers = new DefaultHttp2Headers().set(SEC_WEBSOCKET_PROTOCOL, "chat");
                    handshaker.accept(ctx, request, headers).addListener(f -> {
                        if (f.isSuccess()) {
                            serverStream.complete((Http2StreamChannel) ctx.channel());
                        } else {
                            serverStream.completeExceptionally(f.cause());
                        }
                    });
                } else if ("GET".contentEquals(request.headers().method()) &&
                        "/health".contentEquals(request.headers().path())) {
                    ctx.write(new DefaultHttp2HeadersFrame(new DefaultHttp2Headers().status("200")));
                    ctx.writeAndFlush(new DefaultHttp2DataFrame(Unpooled.copiedBuffer("ok", CharsetUtil.US_ASCII),
                            true));
                }
            } else if (msg instanceof CloseWebSocketFrame) {
                serverCloseCode.complete(((CloseWebSocketFrame) msg).statusCode());
            } else if (msg instanceof WebSocketFrame) {
                ctx.writeAndFlush(((WebSocketFrame) msg).retain());
            }
        }
    }

    private static final class Wire extends ChannelInboundHandlerAdapter {
        final CompletableFuture<Integer> firstHeader = new CompletableFuture<Integer>();
        final CompletableFuture<Void> endStream = new CompletableFuture<Void>();
        final CompletableFuture<Long> reset = new CompletableFuture<Long>();
        final AtomicInteger dataFrames = new AtomicInteger();

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof Http2DataFrame) {
                Http2DataFrame data = (Http2DataFrame) msg;
                dataFrames.incrementAndGet();
                ByteBuf bytes = data.content();
                if (bytes.readableBytes() >= 2) {
                    firstHeader.complete(bytes.getUnsignedShort(bytes.readerIndex()));
                }
                if (data.isEndStream()) {
                    endStream.complete(null);
                }
            } else if (msg instanceof Http2ResetFrame) {
                reset.complete(((Http2ResetFrame) msg).errorCode());
            }
            ctx.fireChannelRead(msg);
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
            if (evt instanceof Http2ResetFrame) {
                reset.complete(((Http2ResetFrame) evt).errorCode());
            }
            ctx.fireUserEventTriggered(evt);
        }
    }
}
