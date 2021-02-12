package io.managed.services.test;

import io.managed.services.test.cli.CLI;
import io.managed.services.test.cli.CLIUtils;
import io.managed.services.test.cli.ProcessException;
import io.managed.services.test.client.github.GitHub;
import io.managed.services.test.executor.ExecBuilder;
import io.managed.services.test.framework.TestTag;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.file.OpenOptions;
import io.vertx.junit5.VertxExtension;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;

import static io.managed.services.test.TestUtils.await;
import static io.managed.services.test.cli.CLIUtils.extractCLI;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

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

    String workdir;
    CLI cli;

    @AfterAll
    void clean(Vertx vertx) {
        if (cli != null) {
            LOGGER.info("log-out from the CLI");
            await(cli.logout().recover(t -> {
                LOGGER.error("logout failed with error:", t);
                return Future.succeededFuture();
            }));
        }

        if (workdir != null) {
            LOGGER.info("delete workdir: {}", workdir);
            await(vertx.fileSystem().deleteRecursive(workdir, true).recover(t -> {
                LOGGER.error("failed to delete workdir {} with error:", workdir, t);
                return Future.succeededFuture();
            }));
        }
    }

    @Test
    @Order(1)
    void testDownloadCLI(Vertx vertx) throws IOException {
        assumeTrue(Environment.BF2_GITHUB_TOKEN != null, "the BF2_GITHUB_TOKEN env is null");

        workdir = await(vertx.fileSystem().createTempDirectory("cli"));
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

        this.cli = new CLI(workdir, CLI_NAME);

        LOGGER.info("validate cli");
        new ExecBuilder()
            .withCommand(cli, "--help")
            .logToOutput(true)
            .throwErrors(true)
            .exec();
    }


    @Test
    @Order(2)
    void testLogin(Vertx vertx) {
        assumeTrue(cli != null, "the global cli is null because testDownloadCLI did not complete");
        assumeTrue(Environment.SSO_USERNAME != null, "the SSO_USERNAME env is null");
        assumeTrue(Environment.SSO_PASSWORD != null, "the SSO_PASSWORD env is null");

        LOGGER.info("verify that we aren't logged-in");
        await(cli.listKafkas()
            .compose(r -> Future.failedFuture("cli kafka list should fail because we haven't log-in yet"))
            .recover(t -> {
                if (t instanceof ProcessException) {
                    if (((ProcessException) t).process.exitValue() == 1) {
                        LOGGER.info("we haven't log-in yet");
                        return Future.succeededFuture();
                    }
                }
                return Future.failedFuture(t);
            }));

        LOGGER.info("login the CLI");
        await(CLIUtils.login(vertx, cli, Environment.SSO_USERNAME, Environment.SSO_PASSWORD));

        LOGGER.info("verify that we are logged-in");
        await(cli.listKafkas());
    }
}
