package io.managed.services.test;

@FunctionalInterface
public interface ThrowingSupplier<A, T extends Throwable> {
    A get() throws T;
}
