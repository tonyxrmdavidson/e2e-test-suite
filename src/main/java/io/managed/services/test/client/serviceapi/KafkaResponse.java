package io.managed.services.test.client.serviceapi;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openshift.cloud.api.kas.models.KafkaRequest;
import org.joda.time.Duration;
import org.joda.time.Instant;

@Deprecated
@JsonIgnoreProperties(ignoreUnknown = true)
public class KafkaResponse {
    public String id;
    public String kind;
    public String href;
    public String status;
    @JsonProperty("cloud_provider")
    public String cloudProvider;
    @JsonProperty("multi_az")
    public Boolean multiAZ;
    public String region;
    public String owner;
    public String name;
    @JsonProperty("bootstrap_server_host")
    @JsonAlias({"bootstrapServerHost"})
    public String bootstrapServerHost;
    @JsonProperty("created_at")
    public String createdAt;
    @JsonProperty("updated_at")
    public String updatedAt;
    @JsonProperty("failed_reason")
    public String failedReason;
    public String version;


    public long getUpTimeInHours() {
        Instant creationTime = new Instant(this.createdAt);
        Duration upTime = new Duration(creationTime, new Instant());
        return upTime.getStandardHours();
    }

    public KafkaRequest toKafkaRequest() {
        return new KafkaRequest()
            .id(id)
            .kind(kind)
            .href(href)
            .status(status)
            .cloudProvider(cloudProvider)
            .multiAz(multiAZ)
            .region(region)
            .owner(owner)
            .name(name)
            .bootstrapServerHost(bootstrapServerHost)
            //.createdAt(createdAt)
            //.updatedAt(updatedAt)
            .failedReason(failedReason)
            .version(version);
    }
}
