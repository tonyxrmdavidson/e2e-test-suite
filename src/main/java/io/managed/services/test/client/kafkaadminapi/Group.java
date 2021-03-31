package io.managed.services.test.client.kafkaadminapi;

import java.util.List;

public class Group {
    public String id;
    public boolean simple;
    public GroupCoordinator coordinator;
    public List<GroupMember> members;
    public String state;
}
