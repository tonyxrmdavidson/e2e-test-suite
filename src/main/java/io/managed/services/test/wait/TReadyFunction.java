package io.managed.services.test.wait;

import io.managed.services.test.ThrowableBiFunction;

import java.util.concurrent.atomic.AtomicReference;

public interface TReadyFunction<A, T extends Throwable>
    extends ThrowableBiFunction<Boolean, AtomicReference<A>, Boolean, T> {
}
