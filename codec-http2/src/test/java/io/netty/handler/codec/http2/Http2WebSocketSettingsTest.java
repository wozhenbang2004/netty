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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

class Http2WebSocketSettingsTest {
    @Test
    void tracksTheSettingsActuallySentInThePreface() {
        Http2Settings initial = new Http2Settings().connectProtocolEnabled(false);
        Http2FrameCodec serverCodec = Http2FrameCodecBuilder.forServer().initialSettings(initial).build();
        initial.connectProtocolEnabled(true);
        EmbeddedChannel server = new EmbeddedChannel(serverCodec);
        Http2FrameCodec clientCodec = Http2FrameCodecBuilder.forClient().autoAckSettingsFrame(false).build();
        EmbeddedChannel client = new EmbeddedChannel(clientCodec);
        try {
            ByteBuf bytes;
            while ((bytes = server.readOutbound()) != null) {
                client.writeInbound(bytes);
            }
            assertThat(clientCodec.isConnectProtocolEnabled()).isTrue();
            assertThat(serverCodec.encoder().isLocalConnectProtocolEnabled()).isTrue();
            assertThat(serverCodec.isConnectProtocolEnabled()).isTrue();
        } finally {
            client.finishAndReleaseAll();
            server.finishAndReleaseAll();
        }
    }

    @Test
    void failedAdvertisementAllowsRetryWithoutForgettingOtherPendingWrites() {
        // Hold the first SETTINGS in the real outbound buffer and fail the second before it reaches the transport.
        FailNextWrite failNext = new FailNextWrite();
        EmbeddedChannel channel = new EmbeddedChannel(failNext, new ChannelInboundHandlerAdapter());
        Http2ConnectionEncoder encoder = new DecoratingHttp2ConnectionEncoder(new DefaultHttp2ConnectionEncoder(
                new DefaultHttp2Connection(true), new DefaultHttp2FrameWriter()));
        try {
            ChannelHandlerContext ctx = channel.pipeline().lastContext();
            Http2Settings enable = new Http2Settings().connectProtocolEnabled(true);
            Http2Settings disable = new Http2Settings().connectProtocolEnabled(false);
            failNext.fail = true;
            assertThat(encoder.writeSettings(ctx, enable, channel.newPromise()).cause())
                    .isInstanceOf(IOException.class);
            assertThat(encoder.isLocalConnectProtocolEnabled()).isFalse();
            assertThat(encoder.pollSentSettings()).isNull();

            // With no successful or pending enable, an explicit zero is still legal.
            ChannelFuture zero = encoder.writeSettings(ctx, disable, channel.newPromise());
            channel.flush();
            assertThat(zero.isSuccess()).isTrue();
            assertThat(encoder.pollSentSettings()).isSameAs(disable);

            ChannelFuture pending = encoder.writeSettings(ctx, enable, channel.newPromise());
            assertThat(pending.isDone()).isFalse();
            failNext.fail = true;
            assertThat(encoder.writeSettings(ctx, enable, channel.newPromise()).cause())
                    .isInstanceOf(IOException.class);
            assertCannotRevoke(encoder.writeSettings(ctx, disable, channel.newPromise()));
            channel.flush();
            assertThat(pending.isSuccess()).isTrue();
            assertThat(encoder.isLocalConnectProtocolEnabled()).isTrue();
            assertThat(encoder.pollSentSettings()).isSameAs(enable);

            // An unrelated setting must not clear the opt-in.
            ChannelFuture other = encoder.writeSettings(ctx, new Http2Settings().maxConcurrentStreams(10),
                    channel.newPromise());
            channel.flush();
            assertThat(other.isSuccess()).isTrue();
            assertCannotRevoke(encoder.writeSettings(ctx, disable, channel.newPromise()));
            assertThat(encoder.pollSentSettings().maxConcurrentStreams()).isEqualTo(10L);
            assertThat(encoder.pollSentSettings()).isNull();
        } finally {
            encoder.close();
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void synchronousWriterFailureReleasesPendingAdvertisement() {
        Http2FrameWriter writer = new DefaultHttp2FrameWriter() {
            @Override
            public ChannelFuture writeSettings(ChannelHandlerContext ctx, Http2Settings settings,
                                               ChannelPromise promise) {
                if (Boolean.TRUE.equals(settings.connectProtocolEnabled())) {
                    throw new IllegalStateException("failed to write settings");
                }
                return super.writeSettings(ctx, settings, promise);
            }
        };
        EmbeddedChannel channel = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        Http2ConnectionEncoder encoder = new DefaultHttp2ConnectionEncoder(new DefaultHttp2Connection(true), writer);
        try {
            ChannelHandlerContext ctx = channel.pipeline().lastContext();
            ChannelFuture failed = encoder.writeSettings(ctx, new Http2Settings().connectProtocolEnabled(true),
                    channel.newPromise());
            assertThat(failed.cause()).isInstanceOf(IllegalStateException.class);
            assertThat(encoder.pollSentSettings()).isNull();
            ChannelFuture zero = encoder.writeSettings(ctx, new Http2Settings().connectProtocolEnabled(false),
                    channel.newPromise());
            channel.flush();
            assertThat(zero.isSuccess()).isTrue();
            assertThat(encoder.isLocalConnectProtocolEnabled()).isFalse();
        } finally {
            encoder.close();
            channel.finishAndReleaseAll();
        }
    }

    private static void assertCannotRevoke(ChannelFuture future) {
        assertThat(future.cause()).isInstanceOf(Http2Exception.class);
        assertThat(((Http2Exception) future.cause()).error()).isEqualTo(Http2Error.PROTOCOL_ERROR);
    }

    private static final class FailNextWrite extends ChannelOutboundHandlerAdapter {
        boolean fail;

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
            if (fail) {
                fail = false;
                ReferenceCountUtil.release(msg);
                promise.setFailure(new IOException("SETTINGS write failed"));
            } else {
                ctx.write(msg, promise);
            }
        }
    }
}
