package io.managed.services.test;

@FunctionalInterface
public interface BooleanFunction {
    boolean call(Boolean b) throws Exception;
}
