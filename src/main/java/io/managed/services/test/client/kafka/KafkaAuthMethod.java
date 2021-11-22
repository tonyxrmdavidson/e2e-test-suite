package io.managed.services.test.client.kafka;

import io.managed.services.test.Environment;
import io.managed.services.test.TestUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import java.util.HashMap;
import java.util.Map;

public enum KafkaAuthMethod {
    PLAIN,
    OAUTH;

    public Map<String, String> configs(String bootstrapHost, String clientID, String clientSecret) {
        switch (this) {
            case OAUTH:
                return oAuthConfigs(bootstrapHost, clientID, clientSecret);
            case PLAIN:
                return plainConfigs(bootstrapHost, clientID, clientSecret);
            default:
                throw new EnumConstantNotPresentException(KafkaAuthMethod.class, this.name());
        }
    }

    static public Map<String, String> plainConfigs(String bootstrapHost, String clientID, String clientSecret) {
        Map<String, String> config = new HashMap<>();
        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapHost);
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        config.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        config.put(SaslConfigs.SASL_JAAS_CONFIG, String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";", clientID, clientSecret));
        setTrustConfigs(bootstrapHost, config);
        return config;
    }

    static public Map<String, String> oAuthConfigs(String bootstrapHost, String jaasConfig) {
        Map<String, String> config = new HashMap<>();
        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapHost);
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        config.put(SaslConfigs.SASL_MECHANISM, "OAUTHBEARER");
        config.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);
        config.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler");
        setTrustConfigs(bootstrapHost, config);
        return config;
    }

    static public void setTrustConfigs(String bootstrapHost, Map<String, String> config) {
        if (Environment.KAFKA_INSECURE_TLS) {
            // Allow the Kafka client to trust an insecure self-signed certificate
            config.put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, TestUtils.getCertificateChain(bootstrapHost));
            config.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
        }
    }

    static public Map<String, String> oAuthConfigs(String bootstrapHost, String clientID, String clientSecret) {
        String jaas = String.format("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required oauth.client.id=\"%s\" oauth.client.secret=\"%s\" " +
            "oauth.token.endpoint.uri=\"%s/auth/realms/%s/protocol/openid-connect/token\";", clientID, clientSecret, Environment.OPENSHIFT_IDENTITY_URI, Environment.OPENSHIFT_IDENTITY_REALM);
        return oAuthConfigs(bootstrapHost, jaas);
    }

    static public Map<String, String> oAuthTokenConfigs(String bootstrapHost, String token) {
        String jaas = String.format("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required oauth.access.token=\"%s\";", token);
        return oAuthConfigs(bootstrapHost, jaas);
    }
}
