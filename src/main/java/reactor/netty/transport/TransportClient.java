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
package reactor.netty.transport;

import java.net.SocketAddress;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;

import io.netty.resolver.AddressResolverGroup;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.ConnectionObserver;

/**
 * A generic client {@link Transport} that will {@link #connect()} to a remote address and provide a {@link Connection}
 *
 * @param <T> TransportClient implementation
 * @param <CONN> Connection implementation
 * @param <CONF> Client Configuration implementation
 * @author Stephane Maldini
 * @author Violeta Georgieva
 * @since 1.0.0
 */
public abstract class TransportClient<T extends TransportClient<T, CONN, CONF>,
		CONN extends Connection,
		CONF extends TransportClientConfig<CONF, CONN>>
		extends Transport<T, CONF> {

	/**
	 * Connect the {@link TransportClient} and return a {@link Mono} of {@link Connection}. If
	 * {@link Mono} is cancelled, the underlying connection will be aborted. Once the
	 * {@link Connection} has been emitted and is not necessary anymore, disposing must be
	 * done by the user via {@link Connection#dispose()}.
	 *
	 * @return a {@link Mono} of {@link Connection}
	 */
	public Mono<CONN> connect() {
		T dup = duplicate();

		CONF config = dup.configuration();
		ConnectionObserver lifecycleObserver = config.lifecycleObserver();
		if (lifecycleObserver != null) {
			ConnectionObserver current = config.observer;
			config.observer = current == null ? lifecycleObserver : current.then(lifecycleObserver);
		}

		Mono<CONN> mono = config.connectionProvider()
		                        .acquire(config);
		if (config.doOnConnect() != null) {
			mono = mono.doOnSubscribe(s -> config.doOnConnect().accept(config));
		}
		return mono;
	}

	/**
	 * Block the {@link TransportClient} and return a {@link Connection}. Disposing must be
	 * done by the user via {@link Connection#dispose()}. The max connection
	 * timeout is 45 seconds.
	 *
	 * @return a {@link Connection}
	 */
	public final CONN connectNow() {
		return connectNow(Duration.ofSeconds(45));
	}

	/**
	 * Block the {@link TransportClient} and return a {@link Connection}. Disposing must be
	 * done by the user via {@link Connection#dispose()}.
	 *
	 * @param timeout connect timeout
	 * @return a {@link Connection}
	 */
	public final CONN connectNow(Duration timeout) {
		Objects.requireNonNull(timeout, "timeout");
		try {
			return Objects.requireNonNull(connect().block(timeout), "aborted");
		}
		catch (IllegalStateException e) {
			if (e.getMessage().contains("blocking read")) {
				throw new IllegalStateException(getClass().getSimpleName() + " couldn't be started within " + timeout.toMillis() + "ms");
			}
			throw e;
		}
	}

	/**
	 * Set or add a callback called when {@link TransportClient} is about to connect to the remote endpoint.
	 *
	 * @param doOnConnect a consumer observing connect events
	 * @return a new {@link TransportClient} reference
	 */
	public final T doOnConnect(Consumer<? super CONF> doOnConnect) {
		Objects.requireNonNull(doOnConnect, "doOnConnect");
		T dup = duplicate();
		@SuppressWarnings("unchecked")
		Consumer<CONF> current = (Consumer<CONF>) configuration().doOnConnect;
		dup.configuration().doOnConnect = current == null ? doOnConnect : current.andThen(doOnConnect);
		return dup;
	}

	/**
	 * Set or add a callback called after {@link Connection} has been connected.
	 *
	 * @param doOnConnected a consumer observing connected events
	 * @return a new {@link TransportClient} reference
	 */
	public final T doOnConnected(Consumer<? super CONN> doOnConnected) {
		Objects.requireNonNull(doOnConnected, "doOnConnected");
		T dup = duplicate();
		@SuppressWarnings("unchecked")
		Consumer<CONN> current = (Consumer<CONN>) configuration().doOnConnected;
		dup.configuration().doOnConnected = current == null ? doOnConnected : current.andThen(doOnConnected);
		return dup;
	}

	/**
	 * Set or add a callback called after {@link Connection} has been disconnected.
	 *
	 * @param doOnDisconnected a consumer observing disconnected events
	 * @return a new {@link TransportClient} reference
	 */
	public final T doOnDisconnected(Consumer<? super CONN> doOnDisconnected) {
		Objects.requireNonNull(doOnDisconnected, "doOnDisconnected");
		T dup = duplicate();
		@SuppressWarnings("unchecked")
		Consumer<CONN> current = (Consumer<CONN>) configuration().doOnDisconnected;
		dup.configuration().doOnDisconnected = current == null ? doOnDisconnected : current.andThen(doOnDisconnected);
		return dup;
	}

	/**
	 * The host to which this client should connect.
	 *
	 * @param host the host to connect to
	 * @return a new {@link TransportClient} reference
	 */
	public final T host(String host) {
		Objects.requireNonNull(host, "host");
		return remoteAddress(AddressUtils.updateHost(configuration().remoteAddress(), host));
	}

	/**
	 * The port to which this client should connect.
	 *
	 * @param port the port to connect to
	 * @return a new {@link TransportClient} reference
	 */
	public final T port(int port) {
		return remoteAddress(AddressUtils.updatePort(configuration().remoteAddress(), port));
	}

	/**
	 * The address to which this client should connect for each subscribe.
	 *
	 * @param remoteAddress the address to connect to.
	 * @return a new {@link TransportClient} reference
	 */
	public final T remoteAddress(SocketAddress remoteAddress) {
		Objects.requireNonNull(remoteAddress, "remoteAddress");
		T dup = duplicate();
		dup.configuration().remoteAddress = remoteAddress;
		return dup;
	}

	/**
	 * Assign an {@link AddressResolverGroup}.
	 *
	 * @param resolver the new {@link AddressResolverGroup}
	 * @return a new {@link TransportClient} reference
	 */
	public final T resolver(AddressResolverGroup<?> resolver) {
		Objects.requireNonNull(resolver, "resolver");
		T dup = duplicate();
		dup.configuration().resolver = resolver;
		return dup;
	}
}