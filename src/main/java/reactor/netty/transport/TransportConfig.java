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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFactory;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.socket.InternetProtocolFamily;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.AttributeKey;
import reactor.netty.ConnectionObserver;
import reactor.netty.channel.ChannelMetricsRecorder;
import reactor.netty.channel.ChannelOperations;
import reactor.netty.resources.LoopResources;

import javax.annotation.Nullable;

/**
 * A basic configuration holder.
 *
 * @author Stephane Maldini
 * @author Violeta Georgieva
 * @since 1.0.0
 */
public abstract class TransportConfig {

	/**
	 * Return the read-only default channel attributes
	 *
	 * @return the read-only default channel attributes
	 */
	public final Map<AttributeKey<?>, ?> attributes() {
		if (attrs.isEmpty()) {
			return Collections.emptyMap();
		}
		return Collections.unmodifiableMap(attrs);
	}

	/**
	 * Return the configured {@link ChannelGroup} or null
	 *
	 * @return the configured {@link ChannelGroup} or null
	 */
	@Nullable
	public final ChannelGroup channelGroup() {
		return channelGroup;
	}

	/**
	 * Return the associated {@link ChannelOperations.OnSetup}, config implementations might override this
	 *
	 * @return the associated {@link ChannelOperations.OnSetup}
	 */
	public ChannelOperations.OnSetup channelOperationsProvider() {
		return DEFAULT_OPS;
	}

	/**
	 * Return the configured {@link ConnectionObserver} if any or {@link ConnectionObserver#emptyListener()}
	 *
	 * @return the configured {@link ConnectionObserver} if any or {@link ConnectionObserver#emptyListener()}
	 */
	public final ConnectionObserver connectionObserver() {
		return observer;
	}

	/**
	 * Return the configured {@link InternetProtocolFamily} to run with or null
	 *
	 * @return the configured {@link InternetProtocolFamily} to run with or null
	 */
	@Nullable
	public final InternetProtocolFamily family() {
		return family;
	}

	/**
	 * Return {@code true} if prefer native event loop and channel factory (e.g. epoll or kqueue)
	 *
	 * @return {@code true} if prefer native event loop and channel factory (e.g. epoll or kqueue)
	 */
	public final boolean isPreferNative() {
		return this.preferNative;
	}

	/**
	 * Return the local {@link SocketAddress} that will be bound or null
	 *
	 * @return the {@link SocketAddress}
	 */
	@Nullable
	public final SocketAddress localAddress() {
		return this.localAddress;
	}

	/**
	 * Return the configured {@link LoggingHandler} or null
	 *
	 * @return the configured {@link LoggingHandler} or null
	 */
	@Nullable
	public final LoggingHandler loggingHandler() {
		return loggingHandler;
	}

	/**
	 * Return the configured {@link LoopResources} or null
	 *
	 * @return the configured  {@link LoopResources} or null
	 */
	@Nullable
	public final LoopResources loopResources() {
		return this.loopResources;
	}

	/**
	 * Return the configured metrics recorder {@link ChannelMetricsRecorder} or null
	 *
	 * @return the configured metrics recorder {@link ChannelMetricsRecorder} or null
	 */
	@Nullable
	public final ChannelMetricsRecorder metricsRecorder() {
		return  this.metricsRecorder;
	}

	/**
	 * Return the read-only {@link ChannelOption} map
	 *
	 * @return the read-only {@link ChannelOption} map
	 */
	public final Map<ChannelOption<?>, ?> options() {
		if (options.isEmpty()) {
			return Collections.emptyMap();
		}
		return Collections.unmodifiableMap(options);
	}


	// Protected/Package private write API

	Map<AttributeKey<?>, ?>  attrs;
	ChannelGroup             channelGroup;
	InternetProtocolFamily   family;
	SocketAddress            localAddress;
	LoggingHandler           loggingHandler;
	LoopResources            loopResources;
	ChannelMetricsRecorder   metricsRecorder;
	ConnectionObserver       observer;
	Map<ChannelOption<?>, ?> options;
	boolean                  preferNative;

	/**
	 * Default TransportConfig with options
	 */
	protected TransportConfig(Map<ChannelOption<?>, ?> options) {
		this.attrs = Collections.emptyMap();
		this.observer = ConnectionObserver.emptyListener();
		this.options = options;
		this.preferNative = LoopResources.DEFAULT_NATIVE;
	}

	/**
	 * Default TransportConfig with options
	 */
	protected TransportConfig(Map<ChannelOption<?>, ?> options, SocketAddress localAddress) {
		this.attrs = Collections.emptyMap();
		this.localAddress = localAddress;
		this.observer = ConnectionObserver.emptyListener();
		this.options = options;
		this.preferNative = LoopResources.DEFAULT_NATIVE;
	}

	/**
	 * Create TransportConfig from an existing one
	 */
	protected TransportConfig(TransportConfig parent) {
		this.attrs = parent.attrs;
		this.channelGroup = parent.channelGroup;
		this.family = parent.family;
		this.localAddress = parent.localAddress;
		this.loggingHandler = parent.loggingHandler;
		this.loopResources = parent.loopResources;
		this.metricsRecorder = parent.metricsRecorder;
		this.observer = parent.observer;
		this.options = parent.options;
		this.preferNative = parent.preferNative;
	}

	public abstract ChannelInitializer<Channel> channelInitializer(ConnectionObserver connectionObserver, SocketAddress remoteAddress);

	/**
	 * Return the {@link ChannelFactory} which is used to create {@link Channel} instances.
	 *
	 * @return the {@link ChannelFactory} which is used to create {@link Channel} instances.
	 */
	protected abstract ChannelFactory<? extends Channel> connectionFactory(EventLoopGroup elg);

	/**
	 * Return the default {@link LoggingHandler} to wiretap this transport
	 *
	 * @return the default {@link LoggingHandler} to wiretap this transport
	 */
	protected abstract LoggingHandler defaultLoggingHandler();

	/**
	 * Return the default {@link LoopResources} for this transport
	 *
	 * @return the default {@link LoopResources} for this transport
	 */
	protected abstract LoopResources defaultLoopResources();

	/**
	 * Return the configured metrics recorder {@link String}.
	 *
	 * @return the configured metrics category {@link String}
	 */
	protected abstract ChannelMetricsRecorder defaultMetricsRecorder();

	/**
	 * Return the configured {@link EventLoopGroup}.
	 *
	 * @return the configured {@link EventLoopGroup}
	 */
	protected abstract EventLoopGroup group();

	/**
	 * Add or remove values to a map in an immutable way by returning a new map instance.
	 *
	 * @param parentMap the container map to update
	 * @param key the key to update
	 * @param value the new value or null to remove an existing key
	 * @param <K> key type to add
	 * @param <V> value to add
	 * @return a new instance of the map
	 */
	@SuppressWarnings("unchecked")
	protected static <K, V> Map<K, V> updateMap(Map<K, V> parentMap, Object key, @Nullable Object value) {
		if (parentMap.isEmpty()) {
			return value == null ? parentMap : Collections.singletonMap((K) key, (V) value);
		}
		else {
			Map<K, V> attrs = new HashMap<>(parentMap.size() + 1);
			attrs.putAll(parentMap);
			if (value == null) {
				attrs.remove(key);
			}
			else {
				attrs.put((K) key, (V) value);
			}
			return attrs;
		}
	}

	static final ChannelOperations.OnSetup DEFAULT_OPS = (ch, c, msg) -> new ChannelOperations<>(ch, c);
}