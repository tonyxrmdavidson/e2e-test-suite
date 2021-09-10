package io.managed.services.test;

@FunctionalInterface
public interface ThrowableSupplier<A, T extends Throwable> {
    A get() throws T;
}
