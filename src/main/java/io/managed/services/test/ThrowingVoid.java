package io.managed.services.test;

@FunctionalInterface
public interface ThrowingVoid<T extends Throwable> {
    void call() throws T;

    default ThrowingSupplier<Void, T> toSupplier() {
        return () -> {
            call();
            return null;
        };
    }
}
