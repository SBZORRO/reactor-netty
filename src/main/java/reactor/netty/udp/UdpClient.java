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

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import io.netty.channel.ChannelOption;
import io.netty.util.NetUtil;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.transport.TransportClient;
import reactor.util.Logger;
import reactor.util.Loggers;

import static reactor.netty.ReactorNetty.format;

/**
 * A UdpClient allows to build in a safe immutable way a UDP client that is materialized
 * and connecting when {@link #connect()} is ultimately called.
 * <p>
 * <p> Example:
 * <pre>
 * {@code
 * UdpClient.create()
 *          .doOnConnect(startMetrics)
 *          .doOnConnected(startedMetrics)
 *          .doOnDisconnected(stopMetrics)
 *          .host("127.0.0.1")
 *          .port(1234)
 *          .connect()
 *          .block()
 * }
 *
 * @author Stephane Maldini
 * @author Violeta Georgieva
 */
public class UdpClient extends TransportClient<UdpClient, Connection, UdpClientConfig> {

	static final UdpClient INSTANCE = new UdpClient();

	/**
	 * Prepare a {@link UdpClient}
	 *
	 * @return a {@link UdpClient}
	 */
	public static UdpClient create() {
		return UdpClient.INSTANCE;
	}

	final UdpClientConfig config;

	UdpClient() {
		this.config = new UdpClientConfig(
				ConnectionProvider.newConnection(),
				Collections.singletonMap(ChannelOption.AUTO_READ, false),
				new InetSocketAddress(NetUtil.LOCALHOST, DEFAULT_PORT));
	}

	UdpClient(UdpClientConfig config) {
		this.config = config;
	}

	@Override
	public UdpClientConfig configuration() {
		return config;
	}

	/**
	 * Attach an IO handler to react on connected client
	 *
	 * @param handler an IO handler that can dispose underlying connection when {@link
	 * Publisher} terminates.
	 *
	 * @return a new {@link UdpClient}
	 */
	public final UdpClient handle(BiFunction<? super UdpInbound, ? super UdpOutbound, ? extends Publisher<Void>> handler) {
		Objects.requireNonNull(handler, "handler");
		return doOnConnected(new OnConnectedHandle(handler));
	}

	@Override
	protected UdpClient duplicate() {
		return new UdpClient(new UdpClientConfig(configuration()));
	}

	static final Logger log = Loggers.getLogger(UdpClient.class);

	/**
	 * The default port for reactor-netty servers. Defaults to 12012 but can be tuned via
	 * the {@code PORT} <b>environment variable</b>.
	 */
	static final int DEFAULT_PORT = System.getenv("PORT") != null ? Integer.parseInt(System.getenv("PORT")) : 12012;

	static final class OnConnectedHandle implements Consumer<Connection> {

		final BiFunction<? super UdpInbound, ? super UdpOutbound, ? extends Publisher<Void>> handler;

		OnConnectedHandle(BiFunction<? super UdpInbound, ? super UdpOutbound, ? extends Publisher<Void>> handler) {
			this.handler = handler;
		}

		@Override
		public void accept(Connection c) {
			if (log.isDebugEnabled()) {
				log.debug(format(c.channel(), "Handler is being applied: {}"), handler);
			}

			Mono.fromDirect(handler.apply((UdpInbound) c, (UdpOutbound) c))
			    .subscribe(c.disposeSubscriber());
		}
	}
}
