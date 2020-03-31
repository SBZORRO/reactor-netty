/*
 * Copyright (c) 2011-Present VMware, Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.netty.udp;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFactory;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ReflectiveChannelFactory;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.logging.LoggingHandler;
import reactor.netty.Connection;
import reactor.netty.ConnectionObserver;
import reactor.netty.channel.ChannelMetricsRecorder;
import reactor.netty.channel.ChannelOperations;
import reactor.netty.channel.MicrometerChannelMetricsRecorder;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;
import reactor.netty.transport.TransportClientConfig;

import javax.annotation.Nullable;
import java.net.SocketAddress;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Encapsulate all necessary configuration for UDP client transport.
 *
 * @author Violeta Georgieva
 * @since 1.0.0
 */
public final class UdpClientConfig extends TransportClientConfig<UdpClientConfig, Connection> {

	UdpClientConfig(ConnectionProvider connectionProvider, Map<ChannelOption<?>, ?> options, SocketAddress remoteAddress) {
		super(connectionProvider, options, remoteAddress);
	}

	UdpClientConfig(UdpClientConfig parent) {
		super(parent);
	}

	@Override
	public ChannelOperations.OnSetup channelOperationsProvider() {
		return DEFAULT_OPS;
	}

	@Override
	protected ChannelFactory<? extends Channel> connectionFactory(EventLoopGroup elg) {
		LoopResources loopResources = loopResources() != null ? loopResources() : defaultLoopResources();
		ChannelFactory<DatagramChannel> channelFactory;
		if (isPreferNative()) {
			channelFactory = new ReflectiveChannelFactory<>(loopResources.onDatagramChannel(elg));
		}
		else {
			channelFactory = () -> new NioDatagramChannel(family());
		}
		return channelFactory;
	}

	@Override
	protected LoggingHandler defaultLoggingHandler() {
		return LOGGING_HANDLER;
	}

	@Override
	protected LoopResources defaultLoopResources() {
		return UdpResources.get();
	}

	@Override
	protected ChannelMetricsRecorder defaultMetricsRecorder() {
		return MicrometerUdpClientMetricsRecorder.INSTANCE;
	}

	@Override
	protected EventLoopGroup group() {
		LoopResources loopResources = loopResources() != null ? loopResources() : defaultLoopResources();
		return loopResources.onClient(isPreferNative());
	}

	@Nullable
	@Override
	protected ConnectionObserver lifecycleObserver() {
		if (doOnConnected() == null && doOnDisconnected() == null) {
			return null;
		}
		return new UdpClientDoOn(doOnConnected(), doOnDisconnected());
	}

	static final LoggingHandler LOGGING_HANDLER = new LoggingHandler(UdpClient.class);

	static final ChannelOperations.OnSetup DEFAULT_OPS = (ch, c, msg) -> new UdpOperations(ch, c);

	static final class UdpClientDoOn implements ConnectionObserver {

		Consumer<? super Connection> doOnConnected;
		Consumer<? super Connection> doOnDisconnected;

		UdpClientDoOn(@Nullable Consumer<? super Connection> doOnConnected,
		              @Nullable Consumer<? super Connection> doOnDisconnected) {
			this.doOnConnected = doOnConnected;
			this.doOnDisconnected = doOnDisconnected;
		}

		@Override
		public void onStateChange(Connection connection, State newState) {
			if (doOnConnected != null && newState == State.CONFIGURED) {
				doOnConnected.accept(connection);
				return;
			}
			if (doOnDisconnected != null) {
				if (newState == State.DISCONNECTING) {
					connection.onDispose(() -> doOnDisconnected.accept(connection));
				}
				else if (newState == State.RELEASED) {
					doOnDisconnected.accept(connection);
				}
			}
		}
	}

	static final class MicrometerUdpClientMetricsRecorder extends MicrometerChannelMetricsRecorder {

		static final MicrometerUdpClientMetricsRecorder INSTANCE =
				new MicrometerUdpClientMetricsRecorder(reactor.netty.Metrics.UDP_CLIENT_PREFIX, "udp");

		MicrometerUdpClientMetricsRecorder(String name, String protocol) {
			super(name, protocol);
		}
	}
}
