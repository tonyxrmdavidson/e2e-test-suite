package io.managed.services.test.client.kafkaadminapi;

import java.util.List;

public class GroupResponse {
    public String id;
    public boolean simple;
    public Coordinator coordinator;
    public List<Member> members;
    public String state;
}
