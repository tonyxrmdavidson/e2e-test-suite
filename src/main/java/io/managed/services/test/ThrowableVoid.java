package io.managed.services.test;

@FunctionalInterface
public interface ThrowableVoid<T extends Throwable> {
    void call() throws T;
}
