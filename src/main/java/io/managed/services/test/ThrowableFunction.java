package io.managed.services.test;

@FunctionalInterface
public interface ThrowableFunction<A, B, T extends Throwable> {
    B call(A a) throws T;
}
