package io.managed.services.test;

import io.vertx.core.Future;
import org.javatuples.Pair;

import java.util.function.Function;

public interface IsReady<T> extends Function<Boolean, Future<Pair<Boolean, T>>> {
}
