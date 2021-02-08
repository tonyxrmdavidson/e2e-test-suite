package io.managed.services.test;

import io.managed.services.test.client.github.GitHub;
import io.managed.services.test.executor.ExecBuilder;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Vertx;
import io.vertx.core.file.OpenOptions;
import io.vertx.junit5.VertxExtension;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;

import static io.managed.services.test.CLIUtils.extractCLI;
import static io.managed.services.test.TestUtils.await;

@Tag(TestTag.CI)
@Tag(TestTag.CLI)
@ExtendWith(VertxExtension.class)
public class CLITest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(CLITest.class);

    static final String DOWNLOAD_ORG = "bf2fc6cc711aee1a0c2a";
    static final String DOWNLOAD_REPO = "cli";
    static final String CLI_NAME = "rhoas";
    static final String DOWNLOAD_ASSET_TEMPLATE = "%s_%s_%s.tar.gz";
    static final String ARCHIVE_ENTRY_TEMPLATE = "%s_%s_%s/bin/%s";

    String cli;

    @Test
    @Order(1)
    void testDownloadCLI(Vertx vertx) throws IOException {

        final var workdir = await(vertx.fileSystem().createTempDirectory("cli"));
        LOGGER.info("workdir: {}", workdir);

        var client = new GitHub(vertx, Environment.BF2_GITHUB_TOKEN);

        LOGGER.info("retrieve release by tag '{}' in repository: '{}/{}'", Environment.CLI_VERSION, DOWNLOAD_ORG, DOWNLOAD_REPO);
        var release = await(client.getReleaseByTagName(DOWNLOAD_ORG, DOWNLOAD_REPO, Environment.CLI_VERSION));

        final var downloadAsset = String.format(DOWNLOAD_ASSET_TEMPLATE, CLI_NAME, Environment.CLI_VERSION, Environment.CLI_ARCH);
        LOGGER.info("search for asset '{}' in release: '{}'", downloadAsset, release.toString());
        var asset = release.assets.stream()
            .filter(a -> a.name.equals(downloadAsset))
            .findFirst().orElseThrow();


        final var archive = workdir + "/cli.tar.gz";
        LOGGER.info("download asset '{}' to '{}'", asset.toString(), archive);
        var archiveFile = await(vertx.fileSystem().open(archive, new OpenOptions()
            .setCreate(true)
            .setAppend(false)));
        await(client.downloadAsset(DOWNLOAD_ORG, DOWNLOAD_REPO, asset.id, archiveFile));

        final var cli = workdir + "/" + CLI_NAME;
        final var entry = String.format(ARCHIVE_ENTRY_TEMPLATE, CLI_NAME, Environment.CLI_VERSION, Environment.CLI_ARCH, CLI_NAME);
        LOGGER.info("extract {} from archive {} to: {}", entry, archive, cli);
        extractCLI(archive, entry, cli);

        // make the cli executable
        await(vertx.fileSystem().chmod(cli, "rwxr-xr-x"));

        this.cli = cli;

        LOGGER.info("validate cli");
        new ExecBuilder()
            .withCommand(cli, "--help")
            .logToOutput(true)
            .throwErrors(true)
            .exec();
    }


    @Test
    @Order(2)
    void testLogin(Vertx vertx) throws IOException {
        // TODO
    }
}
