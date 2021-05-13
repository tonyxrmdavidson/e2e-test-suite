package io.managed.services.test;

import io.managed.services.test.framework.TestListener;
import org.testng.annotations.Listeners;

@Listeners(TestListener.class)
public abstract class TestBase {
    protected static final long MINUTES = 60 * 1000;
    protected static final long DEFAULT_TIMEOUT = 3 * MINUTES;
}
