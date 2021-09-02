package io.managed.services.test.client;

import io.managed.services.test.ThrowableSupplier;
import io.managed.services.test.ThrowableVoid;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.exception.ApiUnknownException;

public abstract class BaseApi<E extends Exception> {

    private final Class<E> eClass;

    protected BaseApi(Class<E> eClass) {
        this.eClass = eClass;
    }

    protected abstract ApiUnknownException toApiException(E e);

    protected <T> T handle(ThrowableSupplier<T, E> f) throws ApiGenericException {
        try {
            return f.call();
        } catch (Exception e) {
            if (eClass.isInstance(e)) {
                throw ApiGenericException.apiException(toApiException(eClass.cast(e)));
            }
            throw new RuntimeException(e);
        }
    }

    protected void vhandle(ThrowableVoid<E> f) throws ApiGenericException {
        try {
            f.call();
        } catch (Exception e) {
            if (eClass.isInstance(e)) {
                throw ApiGenericException.apiException(toApiException(eClass.cast(e)));
            }
            throw new RuntimeException(e);
        }
    }
}
