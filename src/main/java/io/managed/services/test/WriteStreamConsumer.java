package io.managed.services.test;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.streams.WriteStream;
import org.apache.commons.lang3.NotImplementedException;

import java.util.function.Function;

public class WriteStreamConsumer<T> implements WriteStream<T> {

    private final Function<T, Future<Object>> consumer;

    public WriteStreamConsumer(Function<T, Future<Object>> consumer) {
        this.consumer = consumer;
    }

    public static <T> WriteStream<T> create(Function<T, Future<Object>> consumer) {
        return new WriteStreamConsumer<>(consumer);
    }

    @Override
    public WriteStream<T> exceptionHandler(Handler<Throwable> handler) {
        return null;
    }

    @Override
    public Future<Void> write(T data) {
        return consumer.apply(data).map(__ -> null);
    }

    @Override
    public void write(T data, Handler<AsyncResult<Void>> handler) {
        handler.handle(consumer.apply(data).map(__ -> null));
    }

    @Override
    public void end(Handler<AsyncResult<Void>> handler) {
        handler.handle(Future.succeededFuture());
    }

    @Override
    public WriteStream<T> setWriteQueueMaxSize(int maxSize) {
        throw new NotImplementedException();
    }

    @Override
    public boolean writeQueueFull() {
        return false;
    }

    @Override
    public WriteStream<T> drainHandler(Handler<Void> handler) {
        throw new NotImplementedException();
    }
}
