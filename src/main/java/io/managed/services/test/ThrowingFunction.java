package io.managed.services.test;

@FunctionalInterface
public interface ThrowingFunction<A, B, T extends Throwable> {
    B call(A a) throws T;
}
