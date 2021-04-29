package io.managed.services.test.framework;

import com.epam.reportportal.junit5.ReportPortalExtension;
import com.epam.reportportal.listeners.ItemStatus;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.opentest4j.IncompleteExecutionException;

import javax.annotation.Nonnull;

public class CustomReportPortalExtension extends ReportPortalExtension {

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
