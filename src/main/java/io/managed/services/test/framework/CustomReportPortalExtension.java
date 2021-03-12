package io.managed.services.test.framework;

import com.epam.reportportal.junit5.ItemType;
import com.epam.reportportal.junit5.ReportPortalExtension;
import com.epam.reportportal.listeners.ItemStatus;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.opentest4j.IncompleteExecutionException;

import javax.annotation.Nonnull;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class CustomReportPortalExtension extends ReportPortalExtension {
    private static final Logger LOGGER = LogManager.getLogger(CustomReportPortalExtension.class);

    @Override
    protected void startTestItem(@Nonnull final ExtensionContext context, @Nonnull final List<Object> arguments,
                                 @Nonnull final ItemType itemType, @Nonnull final String description, @Nonnull final Date startTime) {

        var cleanedArguments = arguments.stream().map(a -> {
            // remove the Vertx and VertxTestContext from the parameters otherwise ReportPortal will not
            // display the test history because it will think that is a different test
            if (a instanceof Vertx || a instanceof VertxTestContext) {
                return null;
            }
            return a;
        }).collect(Collectors.toList());

        super.startTestItem(context, cleanedArguments, itemType, description, startTime);
    }

    @Override
    protected ItemStatus getExecutionStatus(@Nonnull final ExtensionContext context) {
        var exception = context.getExecutionException();
        if (exception.isPresent()) {
            var e = exception.get();
            if (e instanceof IncompleteExecutionException) {
                sendStackTraceToRP(e);
                return ItemStatus.SKIPPED;
            }
        }
        return super.getExecutionStatus(context);
    }
}
