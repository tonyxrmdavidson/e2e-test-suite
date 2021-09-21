package io.managed.services.test.cli;

import io.managed.services.test.Environment;
import io.managed.services.test.client.github.Asset;
import io.managed.services.test.client.github.GitHub;
import io.managed.services.test.client.github.Release;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.file.OpenOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

import static io.managed.services.test.cli.CLIUtils.extractCLI;
import static org.slf4j.helpers.MessageFormatter.format;

public class CLIDownloader {
    private static final Logger LOGGER = LogManager.getLogger(CLIDownloader.class);

    private static final String TMPDIR = "cli";
    private static final String NAME = "rhoas";

    private static final String DOWNLOAD_ASSET_TEMPLATE = "^%s_\\S+_%s_%s.%s$"; // rhoas_{version}_linux_amd64.tar.gz
    private static final String ARCHIVE_TEMPLATE = "%s.%s"; // rhoas.tar.gz
    private static final String ARCHIVE_ENTRY_TEMPLATE = "^%s_\\S+_%s_%s/bin/%s$"; // rhoas_{version}_linux_amd64/bin/rhoas

    private final Vertx vertx;
    private final GitHub github;
    private final String organization;
    private final String repository;
    private final String version;
    private final String platform;
    private final String arch;
    private final String archiveExt;

    public CLIDownloader(
        Vertx vertx,
        String organization,
        String repository,
        String version,
        String platform,
        String arch) {

        this.vertx = vertx;
        this.github = new GitHub(vertx);
        this.organization = organization;
        this.repository = repository;
        this.version = version;
        this.platform = platform;
        this.arch = arch;
        this.archiveExt = platformToArchive(platform);
    }

    public static CLIDownloader defaultDownloader(Vertx vertx) {
        return new CLIDownloader(
            vertx,
            Environment.CLI_DOWNLOAD_ORG,
            Environment.CLI_DOWNLOAD_REPO,
            Environment.CLI_VERSION,
            Environment.CLI_PLATFORM,
            Environment.CLI_ARCH);
    }

    private String platformToArchive(String platform) {
        if (Platform.WINDOWS.toString().equals(platform)) {
            return "zip";
        }

        return "tar.gz";
    }

    public Future<CLIBinary> downloadCLIInTempDir() {
        LOGGER.info("download CLI in temporary directory");
        return vertx.fileSystem().createTempDirectory(TMPDIR)
            .compose(workspace -> downloadCLIInWorkspace(workspace)
                .map(__ -> new CLIBinary(workspace, NAME)));
    }

    private Future<Void> downloadCLIInWorkspace(String workspace) {
        LOGGER.info("download CLI in workspace: {}", workspace);
        return github.getReleaseByTagName(organization, repository, version)
            .compose(r -> downloadCLIReleaseInWorkspace(r, workspace));
    }

    private Future<Void> downloadCLIReleaseInWorkspace(Release release, String workspace) {
        return getDownloadAssetFromRelease(release)
            .compose(asset -> downloadCLIAssetInWorkspace(asset, workspace))
            .compose(archive -> extractCLIBinaryInWorkspace(archive, workspace))
            .compose(binary -> makeCLIBinaryExecutable(binary));
    }

    private Future<String> downloadCLIAssetInWorkspace(Asset asset, String workspace) {
        var archive = workspace + "/" + String.format(ARCHIVE_TEMPLATE, NAME, archiveExt);
        LOGGER.info("download asset '{}' to '{}'", asset.toString(), archive);
        return vertx.fileSystem().open(archive, new OpenOptions().setCreate(true).setAppend(false))
            .compose(file -> github.downloadAsset(organization, repository, asset.getId(), file))
            .map(__ -> archive);
    }

    private Future<String> extractCLIBinaryInWorkspace(String archive, String workspace) {
        var entry = String.format(ARCHIVE_ENTRY_TEMPLATE, NAME, platform, arch, NAME);
        var binary = workspace + "/" + NAME;

        LOGGER.info("extract {} from archive {} to: {}", entry, archive, binary);
        try {
            extractCLI(archive, entry, binary);
            return Future.succeededFuture(binary);
        } catch (IOException e) {
            return Future.failedFuture(e);
        }
    }

    private Future<Void> makeCLIBinaryExecutable(String binary) {
        if (Platform.WINDOWS.toString().equals(platform)) {
            // skip windows
            return Future.succeededFuture();
        }

        return vertx.fileSystem().chmod(binary, "rwxr-xr-x");
    }

    private Future<Asset> getDownloadAssetFromRelease(Release release) {
        var assetName = String.format(DOWNLOAD_ASSET_TEMPLATE, NAME, platform, arch, archiveExt);
        LOGGER.info("search for asset '{}' in release: '{}'", assetName, release.toString());
        return release.getAssets().stream()
            .filter(a -> a.getName().matches(assetName))
            .findFirst()
            .map(a -> Future.succeededFuture(a))
            .orElseGet(() -> Future.failedFuture(format("asset '{}' not found in release: '{}'",
                assetName, release.toString()).getMessage()));
    }

    public static class CLIBinary {
        public final String directory;
        public final String name;

        @SuppressWarnings("SameParameterValue")
        private CLIBinary(String directory, String name) {
            this.directory = directory;
            this.name = name;
        }
    }
}
