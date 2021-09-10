package io.managed.services.test.client.oauth;

import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.impl.HttpResponseImpl;


public class VertxHttpResponse extends HttpResponseImpl<Buffer> implements HttpResponse<Buffer> {

    private final VertxHttpRequest request;

    public VertxHttpResponse(HttpResponse<Buffer> response, VertxHttpRequest request) {
        super(
            response.version(),
            response.statusCode(),
            response.statusMessage(),
            response.headers(),
            response.trailers(),
            response.cookies(),
            response.body(),
            response.followedRedirects());

        this.request = request;
    }

    public VertxHttpRequest getRequest() {
        return request;
    }
}
