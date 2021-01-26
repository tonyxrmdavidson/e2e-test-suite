package io.managed.services.test;

import com.epam.reportportal.junit5.ReportPortalExtension;
import io.managed.services.test.framework.ExtensionContextParameterResolver;
import io.managed.services.test.framework.IndicativeSentences;
import io.managed.services.test.framework.TestCallbackListener;
import io.managed.services.test.framework.TestExceptionCallbackListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestCallbackListener.class)
@ExtendWith(TestExceptionCallbackListener.class)
@ExtendWith(ExtensionContextParameterResolver.class)
@ExtendWith(ReportPortalExtension.class)
@DisplayNameGeneration(IndicativeSentences.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestBase {
    private static final Logger LOGGER = LogManager.getLogger(TestBase.class);
}
