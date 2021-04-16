package io.managed.services.test.client.sample;

import io.managed.services.test.client.BaseVertxClient;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.codec.BodyCodec;

import java.net.HttpURLConnection;

public class QuarkusSample extends BaseVertxClient {

    public QuarkusSample(Vertx vertx, String uri) {
        super(vertx, uri);
    }

    public Future<HttpResponse<Void>> streamPrices(WriteStream<Buffer> stream) {
        return client.get("/prices/stream")
                .as(BodyCodec.pipe(stream))
                .send()
                .compose(r -> assertResponse(r, HttpURLConnection.HTTP_OK));
    }
}
