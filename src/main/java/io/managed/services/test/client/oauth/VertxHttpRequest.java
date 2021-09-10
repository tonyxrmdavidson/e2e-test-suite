package io.managed.services.test.client.oauth;

import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpRequest;

public class VertxHttpRequest {

    private final HttpRequest<Buffer> request;
    private final String absoluteURI;

    public VertxHttpRequest(HttpRequest<Buffer> request, String absoluteURI) {
        this.request = request;
        this.absoluteURI = absoluteURI;
    }

    public Future<VertxHttpResponse> send() {
        return this.request.send().map(r -> new VertxHttpResponse(r, this));
    }

    public Future<VertxHttpResponse> sendForm(MultiMap body) {
        return this.request.sendForm(body).map(r -> new VertxHttpResponse(r, this));
    }

    public String getAbsoluteURI() {
        return absoluteURI;
    }
}
