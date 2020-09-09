package com.amazon.ion.benchmark;

import com.amazon.ion.IonException;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.Timestamp;
import com.amazon.ion.system.IonTextWriterBuilder;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * Collects version info from the JAR manifest and properties files.
 */
class VersionInfo {

    private static final String MANIFEST_FILE = "META-INF/MANIFEST.MF";
    private static final String ION_JAVA_PROPERTIES_FILE = "META-INF/maven/com.amazon.ion/ion-java/pom.properties";
    private static final String ION_JAVA_PROPERTIES_VERSION_KEY = "version";
    private static final String ION_JAVA_PROJECT_VERSION_ATTRIBUTE = "Ion-Java-Project-Version";
    private static final String CLI_BUILD_TIME_ATTRIBUTE = "Ion-Java-Benchmark-Build-Time";
    private static final String CLI_PROJECT_VERSION_ATTRIBUTE = "Ion-Java-Benchmark-Project-Version";

    private String ionJavaProjectVersion;

    private String cliProjectVersion;

    private Timestamp cliBuildTime;

    /**
     * Constructs a new instance that can provide build information about this library and its dependencies.
     *
     * @throws IonException if there's a problem loading the build info.
     */
    VersionInfo() throws IonException {
        Enumeration<URL> manifestUrls;
        try {
            manifestUrls = getClass().getClassLoader().getResources(MANIFEST_FILE);
        }
        catch (IOException e) {
            throw new IonException("Unable to load manifests.", e);
        }
        List<Manifest> manifests = new ArrayList<>();
        while (manifestUrls.hasMoreElements()) {
            try {
                manifests.add(new Manifest(manifestUrls.nextElement().openStream()));
            }
            catch (IOException e) {
                // try the next manifest
            }
        }
        loadBuildProperties(manifests);

        Enumeration<URL> ionJavaPropertiesUrls;
        try {
            ionJavaPropertiesUrls = getClass().getClassLoader().getResources(ION_JAVA_PROPERTIES_FILE);
        }
        catch (IOException e) {
            throw new IonException("Unable to load ion-java properties.", e);
        }
        while (ionJavaPropertiesUrls.hasMoreElements()) {
            try {
                Properties properties = new Properties();
                properties.load(ionJavaPropertiesUrls.nextElement().openStream());
                String version = properties.getProperty(ION_JAVA_PROPERTIES_VERSION_KEY);
                if (version != null && ionJavaProjectVersion != null) {
                    // In the event of conflicting properties, fail instead of risking returning incorrect version info.
                    throw new IonException("Found multiple properties with ion-java version info on the classpath.");
                }
                ionJavaProjectVersion = version;
            }
            catch (IOException e) {
                // try the next properties file
            }
        }
    }

    /**
     * Retrieves the build properties from the manifests, if present.
     * @param manifests the manifests to check for build properties.
     * @throws RuntimeException if the build properties are present in multiple manifests.
     */
    private void loadBuildProperties(List<Manifest> manifests) throws IonException {
        boolean cliPropertiesLoaded = false;
        for(Manifest manifest : manifests) {
            boolean success = tryToLoadCliBuildProperties(manifest);
            if(success && cliPropertiesLoaded) {
                // In the event of conflicting manifests, fail instead of risking returning incorrect version info.
                throw new RuntimeException("Found multiple manifests with CLI version info on the classpath.");
            }
            cliPropertiesLoaded |= success;
        }
    }

    /**
     * Returns true if the CLI properties were loaded, otherwise false.
     */
    private boolean tryToLoadCliBuildProperties(Manifest manifest) {
        Attributes mainAttributes = manifest.getMainAttributes();
        String projectVersion = mainAttributes.getValue(CLI_PROJECT_VERSION_ATTRIBUTE);
        String time = mainAttributes.getValue(CLI_BUILD_TIME_ATTRIBUTE);

        if (projectVersion == null || time == null) {
            return false;
        }

        cliProjectVersion = projectVersion;

        try {
            cliBuildTime = Timestamp.valueOf(time);
        }
        catch (IllegalArgumentException e) {
            // Badly formatted timestamp. Ignore it.
        }
        return true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        try {
            try (IonWriter writer = IonTextWriterBuilder.pretty().build(sb)) {
                writer.stepIn(IonType.STRUCT);
                writer.setFieldName(ION_JAVA_PROJECT_VERSION_ATTRIBUTE);
                writer.writeString(ionJavaProjectVersion);
                writer.setFieldName(CLI_BUILD_TIME_ATTRIBUTE);
                writer.writeTimestamp(cliBuildTime);
                writer.setFieldName(CLI_PROJECT_VERSION_ATTRIBUTE);
                writer.writeString(cliProjectVersion);
                writer.stepOut();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return sb.toString();
    }
}
