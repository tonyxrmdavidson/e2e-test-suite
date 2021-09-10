package io.managed.services.test.wait;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

public interface ReadyFunction<A> extends BiFunction<Boolean, AtomicReference<A>, Boolean> {
}
