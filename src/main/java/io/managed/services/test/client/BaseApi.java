package io.managed.services.test.client;

import io.managed.services.test.RetryUtils;
import io.managed.services.test.ThrowingSupplier;
import io.managed.services.test.ThrowingVoid;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.exception.ApiUnauthorizedException;
import io.managed.services.test.client.exception.ApiUnknownException;
import io.managed.services.test.client.oauth.KeycloakUser;
import lombok.extern.log4j.Log4j2;

import java.util.Objects;

@Log4j2
public abstract class BaseApi {

    private final KeycloakUser user;

    protected BaseApi(KeycloakUser user) {
        this.user = Objects.requireNonNull(user);
    }

    /**
     * @param e Exception
     * @return ApiUnknownException | null if the passed Exception can't be converted
     */
    protected abstract ApiUnknownException toApiException(Exception e);

    protected abstract void setAccessToken(String t);

    private <A> A handleException(ThrowingSupplier<A, Exception> f) throws ApiGenericException {

        try {
            return f.get();
        } catch (Exception e) {
            var ex = toApiException(e);
            if (ex != null) {
                throw ApiGenericException.apiException(ex);
            }
            throw new RuntimeException(e);
        }
    }

    private <A> A handle(ThrowingSupplier<A, Exception> f) throws ApiGenericException {

        // Set the access token before each call because another API could
        // have renewed it
        setAccessToken(user.getAccessToken());

        try {
            return handleException(f);
        } catch (ApiUnauthorizedException e) {
            log.debug("renew access token");
            // Try to renew the access token
            setAccessToken(user.renewToken().getAccessToken());
            // and retry
            return handleException(f);
        }
    }

    protected <A> A retry(ThrowingSupplier<A, Exception> f) throws ApiGenericException {
        return RetryUtils.retry(1, () -> handle(f), BaseApi::retryCondition);
    }

    protected void retry(ThrowingVoid<Exception> f) throws ApiGenericException {
        RetryUtils.retry(1, () -> handle(f.toSupplier()), BaseApi::retryCondition);
    }

    private static boolean retryCondition(Throwable t) {
        if (t instanceof ApiGenericException) {
            var code = ((ApiGenericException) t).getCode();
            return code >= 500 && code < 600 // Server Errors
                || code == 408;  // Request Timeout
        }
        if (t instanceof RuntimeException) {
            // retry generic runtime exception
            return true;
        }
        log.warn("not going to retry exception:", t);
        return false;
    }
}
