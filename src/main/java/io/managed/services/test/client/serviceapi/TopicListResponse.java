package io.managed.services.test.client.serviceapi;

import java.util.List;

public class TopicListResponse {
    public int page;
    public List<TopicResponse> items;
    public int size;
    public int offset;
    public int limit;
    public int total;
}
