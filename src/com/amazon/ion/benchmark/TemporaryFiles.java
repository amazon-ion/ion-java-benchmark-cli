package com.amazon.ion.benchmark;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utilities for managing temporary files.
 */
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

    /**
     * Delete all files within the temporary directory, then delete the temporary directory itself.
     * @throws IOException if thrown while trying to delete any file or directory.
     */
    static void cleanUpTempDirectory() throws IOException {
        if (tempDirectory.toFile().isDirectory()) {
            List<Path> files = Files.list(tempDirectory).collect(Collectors.toList());
            for (Path file : files) {
                Files.deleteIfExists(file);
            }
        }
        Files.deleteIfExists(tempDirectory);
    }

    /**
     * Delete the existing temporary directory (if necessary), then re-create it.
     * @throws IOException if deletion or creation fails.
     */
    static void prepareTempDirectory() throws IOException {
        cleanUpTempDirectory();
        if (!tempDirectory.toFile().exists()) {
            Files.createDirectory(tempDirectory);
        }
    }

    /**
     * Create a new temporary file inside the temporary directory.
     * @param prefix name prefix for the new temporary file.
     * @param suffix file suffix for the new temporary file.
     * @return Path to the new temporary file.
     * @throws IOException if thrown while trying to create the file.
     */
    static Path newTempFile(String prefix, String suffix) throws IOException {
        return Files.createTempFile(tempDirectory, prefix, suffix);
    }
}
