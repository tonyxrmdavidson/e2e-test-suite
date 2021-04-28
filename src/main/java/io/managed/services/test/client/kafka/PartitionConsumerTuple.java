package io.managed.services.test.client.kafka;

public class PartitionConsumerTuple {
    private int partitionId;
    private int consumerId;

    public PartitionConsumerTuple(int partitionId, int consumerId) {
        this.partitionId = partitionId;
        this.consumerId = consumerId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public int getConsumerId() {
        return consumerId;
    }

}

