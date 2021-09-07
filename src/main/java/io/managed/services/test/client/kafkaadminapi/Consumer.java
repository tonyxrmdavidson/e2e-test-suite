package io.managed.services.test.client.kafkaadminapi;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@Deprecated
@JsonIgnoreProperties(ignoreUnknown = true)
public class Consumer {
    public String groupId;
    public String topic;
    public int partition;
    public long offset;
    public long logEndOffset;
    public long lag;
    public String memberId;
}
