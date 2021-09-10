package io.managed.services.test;

@FunctionalInterface
public interface ThrowableBiFunction<A, B, C, T extends Throwable> {
    C apply(A var1, B var2) throws T;
}
