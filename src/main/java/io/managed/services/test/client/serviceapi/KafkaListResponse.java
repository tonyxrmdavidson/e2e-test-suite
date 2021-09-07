package io.managed.services.test.client.serviceapi;

import java.util.List;

@Deprecated
public class KafkaListResponse {
    public List<KafkaResponse> items;
    public String kind;
    public Integer page;
    public Integer size;
    public Integer total;
}
