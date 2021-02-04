package io.managed.services.test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Class which holds environment variables for system tests.
 */
public class Environment {

    private static final Logger LOGGER = LogManager.getLogger(Environment.class);
    private static final Map<String, String> VALUES = new HashMap<>();
    private static final JsonNode JSON_DATA = loadConfigurationFile();
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH:mm");
    private static String config;

    /*
     * Definition of env vars
     */
    private static final String LOG_DIR_ENV = "LOG_DIR";
    private static final String CONFIG_FILE_PATH_ENV = "CONFIG_PATH";

    public static final String SSO_USERNAME_ENV = "SSO_USERNAME";
    public static final String SSO_PASSWORD_ENV = "SSO_PASSWORD";
    public static final String SSO_SECONDARY_USERNAME_ENV = "SSO_SECONDARY_USERNAME";
    public static final String SSSO_SECONDARY_PASSWORD_ENV = "SSO_SECONDARY_PASSWORD";
    public static final String SSO_ALIEN_USERNAME_ENV = "SSO_ALIEN_USERNAME";
    public static final String SSO_ALIEN_PASSWORD_ENV = "SSO_ALIEN_PASSWORD";

    public static final String SSO_REDHAT_KEYCLOAK_URI_ENV = "SSO_REDHAT_KEYCLOAK_URI";
    public static final String SSO_REDHAT_REALM_ENV = "SSO_REDHAT_REALM";
    public static final String SSO_REDHAT_CLIENT_ID_ENV = "SSO_REDHAT_CLIENT_ID";
    public static final String SSO_REDHAT_REDIRECT_URI_ENV = "SSO_REDHAT_REDIRECT_URI";

    public static final String SERVICE_API_URI_ENV = "SERVICE_API_URI";

    public static final String KAFKA_POSTFIX_NAME_ENV = "KAFKA_POSTFIX_NAME";
    public static final String LONG_LIVED_KAFKA_NAME_ENV = "LONG_LIVED_KAFKA_NAME";
    public static final String LONG_LIVED_KAFKA_TOPIC_NAME_ENV = "LONG_LIVED_KAFKA_TOPIC_NAME";

    public static final String DEV_CLUSTER_SERVER_ENV = "DEV_CLUSTER_SERVER";
    public static final String DEV_CLUSTER_NAMESPACE_ENV = "DEV_CLUSTER_NAMESPACE";
    public static final String DEV_CLUSTER_TOKEN_ENV = "DEV_CLUSTER_TOKEN";


    /*
     * Setup constants from env variables or set default
     */
    public static final String SUITE_ROOT = System.getProperty("user.dir");
    public static final Path LOG_DIR = getOrDefault(LOG_DIR_ENV, Paths::get, Paths.get(SUITE_ROOT, "target", "logs")).resolve("test-run-" + DATE_FORMAT.format(LocalDateTime.now()));

    // main sso.redhat.com user
    public static final String SSO_USERNAME = getOrDefault(SSO_USERNAME_ENV, null);
    public static final String SSO_PASSWORD = getOrDefault(SSO_PASSWORD_ENV, null);

    // test sso.redhat.com secondary user
    public static final String SSO_SECONDARY_USERNAME = getOrDefault(SSO_SECONDARY_USERNAME_ENV, null);
    public static final String SSO_SECONDARY_PASSWORD = getOrDefault(SSSO_SECONDARY_PASSWORD_ENV, null);

    public static final String SSO_ALIEN_USERNAME = getOrDefault(SSO_ALIEN_USERNAME_ENV, null);
    public static final String SSO_ALIEN_PASSWORD = getOrDefault(SSO_ALIEN_PASSWORD_ENV, null);

    // sso.redhat.com OAuth ENVs
    public static final String SSO_REDHAT_KEYCLOAK_URI = getOrDefault(SSO_REDHAT_KEYCLOAK_URI_ENV, "https://sso.redhat.com");
    public static final String SSO_REDHAT_REALM = getOrDefault(SSO_REDHAT_REALM_ENV, "redhat-external");
    public static final String SSO_REDHAT_CLIENT_ID = getOrDefault(SSO_REDHAT_CLIENT_ID_ENV, "cloud-services");
    public static final String SSO_REDHAT_REDIRECT_URI = getOrDefault(SSO_REDHAT_REDIRECT_URI_ENV, "https://qaprodauth.cloud.redhat.com");

    public static final String SERVICE_API_URI = getOrDefault(SERVICE_API_URI_ENV, "https://api.stage.openshift.com");

    public static final String KAFKA_POSTFIX_NAME = getOrDefault(KAFKA_POSTFIX_NAME_ENV, "auto-test");
    public static final String LONG_LIVED_KAFKA_NAME = getOrDefault(LONG_LIVED_KAFKA_NAME_ENV, "mk-e2e-longlive-test");
    public static final String LONG_LIVED_KAFKA_TOPIC_NAME = getOrDefault(LONG_LIVED_KAFKA_TOPIC_NAME_ENV, "long-live-test-topic");

    public static final String DEV_CLUSTER_SERVER = getOrDefault(DEV_CLUSTER_SERVER_ENV, "https://api.devexp.imkr.s1.devshift.org:6443");
    public static final String DEV_CLUSTER_NAMESPACE = getOrDefault(DEV_CLUSTER_NAMESPACE_ENV, "mk-e2e-tests");
    public static final String DEV_CLUSTER_TOKEN = getOrDefault(DEV_CLUSTER_TOKEN_ENV, null);

    private Environment() {
    }

    static {
        String debugFormat = "{}: {}";
        LOGGER.info("=======================================================================");
        LOGGER.info("Used environment variables:");
        LOGGER.info(debugFormat, "CONFIG", config);
        VALUES.forEach((key, value) -> LOGGER.info(debugFormat, key, value));
        LOGGER.info("=======================================================================");
    }

    /**
     * Get value from env or  from config or default and parse it to String data type
     *
     * @param varName      variable name
     * @param defaultValue default string value
     * @return value of variable
     */
    private static String getOrDefault(String varName, String defaultValue) {
        return getOrDefault(varName, String::toString, defaultValue);
    }

    /**
     * Get value from env or  from config or default and parse it to defined type
     *
     * @param var          env variable name
     * @param converter    converter from string to defined type
     * @param defaultValue default value if variable is not set in env or config
     * @return value of variable fin defined data type
     */
    private static <T> T getOrDefault(String var, Function<String, T> converter, T defaultValue) {
        String value = System.getenv(var) != null ?
                System.getenv(var) :
                (Objects.requireNonNull(JSON_DATA).get(var) != null ?
                        JSON_DATA.get(var).asText() :
                        null);
        T returnValue = defaultValue;
        if (value != null && !value.isEmpty()) {
            returnValue = converter.apply(value);
        }
        VALUES.put(var, String.valueOf(returnValue));
        return returnValue;
    }

    /**
     * Load configuration fom config file
     *
     * @return json object with loaded variables
     */
    private static JsonNode loadConfigurationFile() {
        config = System.getenv().getOrDefault(CONFIG_FILE_PATH_ENV,
                Paths.get(System.getProperty("user.dir"), "config.json").toAbsolutePath().toString());
        ObjectMapper mapper = new ObjectMapper();
        try {
            File jsonFile = new File(config).getAbsoluteFile();
            return mapper.readTree(jsonFile);
        } catch (IOException ex) {
            LOGGER.info("Json configuration not provider or not exists");
            return mapper.createObjectNode();
        }
    }
}
