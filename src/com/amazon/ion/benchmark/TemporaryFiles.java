package com.amazon.ion.benchmark;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

final class TemporaryFiles {

    private static final Path tempDirectory;

    static {
        File tempDir = new File(System.getProperty("java.io.tmpdir"));
        if (!tempDir.isDirectory()) {
            throw new RuntimeException("Cannot access temp directory " + tempDir.toString());
        }
        File tempLogDir = new File(tempDir, "ion-java-benchmark");
        tempDirectory = tempLogDir.toPath();
    }

    private TemporaryFiles() {
        // Do not instantiate.
    }

    static void cleanUpTempDirectory() throws IOException {
        if (tempDirectory.toFile().isDirectory()) {
            List<Path> files = Files.list(tempDirectory).collect(Collectors.toList());
            for (Path file : files) {
                Files.deleteIfExists(file);
            }
        }
        Files.deleteIfExists(tempDirectory);
    }

    static void prepareTempDirectory() throws IOException {
        cleanUpTempDirectory();
        if (!tempDirectory.toFile().exists()) {
            Files.createDirectory(tempDirectory);
        }
    }

    static Path newTempFile(String prefix, String suffix) throws IOException {
        return Files.createTempFile(tempDirectory, prefix, suffix);
    }
}
