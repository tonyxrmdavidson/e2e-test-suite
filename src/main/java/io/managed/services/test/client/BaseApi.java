package io.managed.services.test.client;

import io.managed.services.test.RetryUtils;
import io.managed.services.test.ThrowingSupplier;
import io.managed.services.test.ThrowingVoid;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.exception.ApiUnknownException;

public abstract class BaseApi<E extends Exception> {

    private final Class<E> eClass;

    protected BaseApi(Class<E> eClass) {
        this.eClass = eClass;
    }

    protected abstract ApiUnknownException toApiException(E e);

    private <A> A handle(ThrowingSupplier<A, E> f) throws ApiGenericException {
        try {
            return f.get();
        } catch (Exception e) {
            if (eClass.isInstance(e)) {
                throw ApiGenericException.apiException(toApiException(eClass.cast(e)));
            }
            throw new RuntimeException(e);
        }
    }

    protected <A> A retry(ThrowingSupplier<A, E> f) throws ApiGenericException {
        return RetryUtils.retry(1, () -> handle(f), BaseApi::retryCondition);
    }

    protected void retry(ThrowingVoid<E> f) throws ApiGenericException {
        RetryUtils.retry(1, () -> handle(f.toSupplier()), BaseApi::retryCondition);
    }

    private static boolean retryCondition(Throwable t) {
        if (t instanceof ApiGenericException) {
            var code = ((ApiGenericException) t).getCode();
            return (code >= 500 && code < 600) // Server Errors
                || code == 408;  // Request Timeout
        }
        return false;
    }
}
