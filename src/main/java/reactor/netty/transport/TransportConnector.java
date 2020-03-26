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

import io.netty.channel.Channel;
import io.netty.channel.ChannelFactory;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.resolver.AddressResolver;
import io.netty.resolver.AddressResolverGroup;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.GenericFutureListener;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.Logger;
import reactor.util.Loggers;

import javax.annotation.Nullable;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static reactor.netty.ReactorNetty.format;

/**
 * {@link TransportConnector} is a helper class that creates, initializes and registers the channel.
 * It performs the actual connect operation to the remote peer or binds the channel.
 *
 * @author Stephane Maldini
 * @author Violeta Georgieva
 * @since 1.0.0
 */
public final class TransportConnector {

	/**
	 * Connect a {@link Channel} to the remote peer.
	 *
	 * @param config the transport configuration
	 * @param remoteAddress the {@link SocketAddress} to connect to
	 * @param resolverGroup the resolver which will resolve the address of the unresolved named address
	 * @param channelInitializer the {@link ChannelInitializer} to initialize the {@link Channel} once it is registered
	 */
	public static Mono<Channel> connect(TransportConfig config, SocketAddress remoteAddress,
			AddressResolverGroup<?> resolverGroup, ChannelInitializer<Channel> channelInitializer) {
		return initAndRegister(config, channelInitializer)
		        .flatMap(channel -> doResolveAndConnect0(channel, config, remoteAddress, resolverGroup));
	}

	static void doConnect(SocketAddress remoteAddress, @Nullable SocketAddress localAddress, ChannelPromise connectPromise) {
		Channel channel = connectPromise.channel();
		channel.eventLoop().execute(() -> {
			if (localAddress == null) {
				channel.connect(remoteAddress, connectPromise);
			}
			else {
				channel.connect(remoteAddress, localAddress, connectPromise);
			}
		});
	}

	@SuppressWarnings("unchecked")
	static Mono<Channel> doResolveAndConnect0(Channel channel, TransportConfig config,
	                                          SocketAddress remoteAddress, AddressResolverGroup<?> resolverGroup) {
		try {
			AddressResolver<SocketAddress> resolver;
			try {
				resolver = (AddressResolver<SocketAddress>) resolverGroup.getResolver(channel.eventLoop());
			}
			catch (Throwable t) {
				channel.close();
				return Mono.error(t);
			}

			SocketAddress localAddress = config.localAddress();
			if (!resolver.isSupported(remoteAddress) || resolver.isResolved(remoteAddress)) {
				MonoChannelPromise monoChannelPromise = new MonoChannelPromise(channel);
				doConnect(remoteAddress, localAddress, monoChannelPromise);
				return monoChannelPromise;
			}

			Future<SocketAddress> resolveFuture = resolver.resolve(remoteAddress);
			if (resolveFuture.isDone()) {
				Throwable cause = resolveFuture.cause();
				if (cause != null) {
					channel.close();
					return Mono.error(cause);
				}
				else {
					MonoChannelPromise monoChannelPromise = new MonoChannelPromise(channel);
					doConnect(resolveFuture.getNow(), localAddress, monoChannelPromise);
					return monoChannelPromise;
				}
			}

			MonoChannelPromise monoChannelPromise = new MonoChannelPromise(channel);
			resolveFuture.addListener((FutureListener<SocketAddress>) future -> {
				if (future.cause() != null) {
					channel.close();
					monoChannelPromise.tryFailure(future.cause());
				}
				else {
					doConnect(future.getNow(), localAddress, monoChannelPromise);
				}
			});
			return monoChannelPromise;
		}
		catch (Throwable t) {
			return Mono.error(t);
		}
	}

	static Mono<Channel> initAndRegister(TransportConfig config, ChannelInitializer<Channel> channelInitializer) {
		EventLoopGroup elg = config.group();

		ChannelFactory<? extends Channel> channelFactory = config.connectionFactory(elg);

		Channel channel = null;
		try {
			channel = channelFactory.newChannel();
			channel.pipeline().addLast(channelInitializer);
			setChannelOptions(channel, config.options());
			setAttributes(channel, config.attributes());
		}
		catch (Throwable t) {
			if (channel != null) {
				channel.unsafe().closeForcibly();
			}
			return Mono.error(t);
		}

		MonoChannelPromise monoChannelPromise = new MonoChannelPromise(channel);
		channel.unsafe().register(elg.next(), monoChannelPromise);
		Throwable cause = monoChannelPromise.cause();
		if (cause != null) {
			if (channel.isRegistered()) {
				channel.close();
			}
			else {
				channel.unsafe().closeForcibly();
			}
		}

		return monoChannelPromise;
	}

	@SuppressWarnings("unchecked")
	static void setAttributes(Channel channel, Map<AttributeKey<?>, ?> attrs) {
		for (Map.Entry<AttributeKey<?>, ?> e : attrs.entrySet()) {
			channel.attr((AttributeKey<Object>) e.getKey()).set(e.getValue());
		}
	}

	@SuppressWarnings("unchecked")
	static void setChannelOptions(Channel channel, Map<ChannelOption<?>, ?> options) {
		for (Map.Entry<ChannelOption<?>, ?> e : options.entrySet()) {
			try {
				if (!channel.config().setOption((ChannelOption<Object>) e.getKey(), e.getValue())) {
					log.warn(format(channel, "Unknown channel option '{}' for channel '{}'"), e.getKey(), channel);
				}
			}
			catch (Throwable t) {
				log.warn(format(channel, "Failed to set channel option '{}' with value '{}' for channel '{}'"),
						e.getKey(), e.getValue(), channel, t);
			}
		}
	}


	static final class MonoChannelPromise extends Mono<Channel> implements ChannelPromise {

		final Channel channel;

		CoreSubscriber<? super Channel> actual;

		MonoChannelPromise(Channel channel) {
			this.channel = channel;
		}

		@Override
		public Channel channel() {
			return channel;
		}

		@Override
		public ChannelPromise setSuccess(Void result) {
			trySuccess(null);
			return this;
		}

		@Override
		public ChannelPromise setSuccess() {
			trySuccess(null);
			return this;
		}

		@Override
		public boolean trySuccess() {
			return trySuccess(null);
		}

		@Override
		public ChannelPromise setFailure(Throwable cause) {
			tryFailure(cause);
			return this;
		}

		@Override
		public ChannelPromise addListener(GenericFutureListener<? extends Future<? super Void>> listener) {
			throw new UnsupportedOperationException();
		}

		@Override
		public ChannelPromise addListeners(GenericFutureListener<? extends Future<? super Void>>... listeners) {
			throw new UnsupportedOperationException();
		}

		@Override
		public ChannelPromise removeListener(GenericFutureListener<? extends Future<? super Void>> listener) {
			return this;
		}

		@Override
		public ChannelPromise removeListeners(GenericFutureListener<? extends Future<? super Void>>... listeners) {
			return this;
		}

		@Override
		public ChannelPromise sync() {
			throw new UnsupportedOperationException();
		}

		@Override
		public ChannelPromise syncUninterruptibly() {
			throw new UnsupportedOperationException();
		}

		@Override
		public ChannelPromise await() {
			throw new UnsupportedOperationException();
		}

		@Override
		public ChannelPromise awaitUninterruptibly() {
			throw new UnsupportedOperationException();
		}

		@Override
		public ChannelPromise unvoid() {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean isVoid() {
			return false;
		}

		@Override
		public boolean trySuccess(Void result) {
			if (RESULT_UPDATER.compareAndSet(this, null, SUCCESS)) {
				if (actual != null) {
					actual.onNext(channel);
					actual.onComplete();
				}
				return true;
			}
			if (actual != null) {
				Operators.onNextDropped(channel, actual.currentContext());
			}
			return false;
		}

		@Override
		public boolean tryFailure(Throwable cause) {
			if (RESULT_UPDATER.compareAndSet(this, null, cause)) {
				channel().close();
				if (actual != null) {
					actual.onError(cause);
				}
				return true;
			}
			if (actual != null) {
				Operators.onErrorDropped(cause, actual.currentContext());
			}
			return false;
		}

		@Override
		public boolean setUncancellable() {
			return true;
		}

		@Override
		public boolean isSuccess() {
			Object result = this.result;
			return result == SUCCESS;
		}

		@Override
		public boolean isCancellable() {
			return false;
		}

		@Override
		public Throwable cause() {
			Object result = this.result;
			return result == SUCCESS ? null : (Throwable) result;
		}

		@Override
		public boolean await(long timeout, TimeUnit unit) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean await(long timeoutMillis) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean awaitUninterruptibly(long timeout, TimeUnit unit) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean awaitUninterruptibly(long timeoutMillis) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Void getNow() {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			return false;
		}

		@Override
		public boolean isCancelled() {
			return false;
		}

		@Override
		public boolean isDone() {
			Object result = this.result;
			return result != null;
		}

		@Override
		public Void get() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Void get(long timeout, TimeUnit unit) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void subscribe(CoreSubscriber<? super Channel> actual) {
			EventLoop eventLoop = channel.eventLoop();
			if (eventLoop.inEventLoop()) {
				_subscribe(actual);
			}
			else {
				eventLoop.execute(() -> _subscribe(actual));
			}
		}

		void _subscribe(CoreSubscriber<? super Channel> actual) {
			this.actual = actual;
			MonoChannelPromiseSubscription s = new MonoChannelPromiseSubscription();
			actual.onSubscribe(s);

			if (isDone()) {
				if (isSuccess()) {
					actual.onNext(channel);
					actual.onComplete();
				}
				else {
					actual.onError(cause());
				}
			}
		}

		static final Object SUCCESS = new Object();
		static final AtomicReferenceFieldUpdater<MonoChannelPromise, Object> RESULT_UPDATER =
				AtomicReferenceFieldUpdater.newUpdater(MonoChannelPromise.class, Object.class, "result");
		volatile Object result;

		static final class MonoChannelPromiseSubscription implements Subscription {

			@Override
			public void request(long n) {
				// noop
			}

			@Override
			public void cancel() {
				// noop
			}
		}
	}

	static final Logger log = Loggers.getLogger(TransportConnector.class);
}
