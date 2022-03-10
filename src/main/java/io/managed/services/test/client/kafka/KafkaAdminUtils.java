package io.managed.services.test.client.kafka;

import lombok.SneakyThrows;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.common.network.KafkaChannel;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.security.authenticator.SaslClientAuthenticator;

import java.util.HashMap;
import java.util.Objects;

public class KafkaAdminUtils {

    static private <T> T assertType(Object o, Class<T> c) {
        Objects.requireNonNull(o);
        Objects.requireNonNull(c);

        if (c.isInstance(o)) {
            return c.cast(o);
        } else {
            throw new AssertionError(String.format("object of type '%s' can not be cast to '%s'", o.getClass().getName(), c.getName()));
        }
    }

    static private KafkaAdminClient getKafkaAdminClient(KafkaAdmin admin) {
        return assertType(admin.getAdmin(), KafkaAdminClient.class);
    }

    static private NetworkClient getKafkaNetworkClient(KafkaAdminClient kafkaAdminClient)
        throws NoSuchFieldException, IllegalAccessException {

        var clientField = KafkaAdminClient.class.getDeclaredField("client");
        clientField.setAccessible(true);
        var client = clientField.get(kafkaAdminClient);

        return assertType(client, NetworkClient.class);
    }

    static private Selector getKafkaSelector(NetworkClient kafkaNetworkClient)
        throws NoSuchFieldException, IllegalAccessException {

        var selectorField = NetworkClient.class.getDeclaredField("selector");
        selectorField.setAccessible(true);
        var selector = selectorField.get(kafkaNetworkClient);

        return assertType(selector, Selector.class);
    }

    static private KafkaChannel getAnyKafkaChannel(Selector kafkaSelector)
        throws NoSuchFieldException, IllegalAccessException {

        var channelsField = Selector.class.getDeclaredField("channels");
        channelsField.setAccessible(true);
        var channels = channelsField.get(kafkaSelector);

        var kafkaChannels = assertType(channels, HashMap.class);

        var channel = kafkaChannels.values().stream().findAny().orElseThrow();

        return assertType(channel, KafkaChannel.class);
    }

    static private SaslClientAuthenticator getKafkaSaslClientAuthenticator(KafkaChannel kafkaChannel)
        throws NoSuchFieldException, IllegalAccessException {

        var authenticatorField = KafkaChannel.class.getDeclaredField("authenticator");
        authenticatorField.setAccessible(true);
        var authenticator = authenticatorField.get(kafkaChannel);

        return assertType(authenticator, SaslClientAuthenticator.class);
    }

    static private Long getPositiveSessionLifetimeMs(SaslClientAuthenticator kafkaAuthenticator)
        throws NoSuchFieldException, IllegalAccessException {

        var reauthInfoField = SaslClientAuthenticator.class.getDeclaredField("reauthInfo");
        reauthInfoField.setAccessible(true);
        var authenticator = reauthInfoField.get(kafkaAuthenticator);

        Objects.requireNonNull(authenticator);

        // SaslClientAuthenticator.ReauthInfo ins private so we will need to assume
        // the reauthInfo field is of type ReauthInfo and retrieve the positiveSessionLifetimeMs

        var positiveSessionLifetimeMsField = authenticator.getClass().getDeclaredField("positiveSessionLifetimeMs");
        positiveSessionLifetimeMsField.setAccessible(true);
        var positiveSessionLifetimeMs = positiveSessionLifetimeMsField.get(authenticator);

        if (positiveSessionLifetimeMs == null) {
            // if the reauthentication is disabled positiveSessionLifetimeMs is null
            return null;
        }

        return assertType(positiveSessionLifetimeMs, Long.class);
    }

    @SneakyThrows
    static public Long getPositiveSessionLifetimeMs(KafkaAdmin admin) {

        // admin
        var kafkaAdminClient = getKafkaAdminClient(admin);

        // admin.client
        var kafkaNetworkClient = getKafkaNetworkClient(kafkaAdminClient);

        // admin.client.selector
        var kafkaSelector = getKafkaSelector(kafkaNetworkClient);

        // admin.client.selector.channels[any]
        var kafkaChannel = getAnyKafkaChannel(kafkaSelector);

        // admin.client.selector.channels[any].authenticator
        var kafkaAuthenticator = getKafkaSaslClientAuthenticator(kafkaChannel);

        // admin.client.selector.channels[any].authenticator.reauthInfo.positiveSessionLifetimeMs
        return getPositiveSessionLifetimeMs(kafkaAuthenticator);
    }

    static public Long getAuthenticatorPositiveSessionLifetimeMs(
        String bootstrapHost,
        String clientID,
        String clientSecret) {

        var admin = new KafkaAdmin(bootstrapHost, clientID, clientSecret);

        // initialize the Kafka Admin by making any requests
        admin.getClusterId();

        return getPositiveSessionLifetimeMs(admin);
    }
}


