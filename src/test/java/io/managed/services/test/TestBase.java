package io.managed.services.test;

import io.managed.services.test.framework.ExtensionContextParameterResolver;
import io.managed.services.test.framework.IndicativeSentences;
import io.managed.services.test.framework.TestCallbackListener;
import io.managed.services.test.framework.TestExceptionCallbackListener;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestCallbackListener.class)
@ExtendWith(TestExceptionCallbackListener.class)
@ExtendWith(ExtensionContextParameterResolver.class)
@DisplayNameGeneration(IndicativeSentences.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestBase {
    protected static final long MINUTES = 60 * 1000;
    protected static final long DEFAULT_TIMEOUT = 3 * MINUTES;
}
