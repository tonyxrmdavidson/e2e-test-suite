package io.managed.services.test.wait;

import io.managed.services.test.ThrowingBiFunction;

import java.util.concurrent.atomic.AtomicReference;

public interface TReadyFunction<A, T extends Throwable>
    extends ThrowingBiFunction<Boolean, AtomicReference<A>, Boolean, T> {
}
