package com.amazon.ion.benchmark;

import com.amazon.ion.IonList;
import com.amazon.ion.IonLoader;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonString;
import com.amazon.ion.IonType;
import com.amazon.ion.system.IonReaderBuilder;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import java.util.Map;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import com.amazon.ion.system.IonSystemBuilder;
import org.w3c.dom.Document;

public class GenerateAndOrganizeBenchmarkResults {
    final private static String ION_JAVA_BENCHMARK = "ion-java-benchmark";
    final private static String MAKE_DIRECTORY = "mkdir -p ";
    final private static String PREVIOUS_FILE = "previous.ion";
    final private static String NEW_FILE = "new.ion";
    final private static String POM_FILE = "pom.xml";
    final private static String ION_JAVA_BENCHMARK_INVOKE_ELEMENT = "java -jar target/ion-java-benchmark-cli-";
    final private static String JAR_WITH_DEPENDENCIES = "-jar-with-dependencies.jar";
    final private static IonLoader LOADER = IonSystemBuilder.standard().build().newLoader();

    /**
     * Execute ion-java-benchmark command with different options combinations and save the benchmark results generated by the same ion-java-benchmark invoke into the same directory.
     * One final directory which contains all directories from different ion-java-benchmark invokes will be generated under the specific file path.
     * The schema of constructing directory name is "ion-java-benchmark" + (read | write) + "--api" + (dom | streaming) + "--format" + (ion-binary | ion-text | text) + file name of test Ion data.
     *
     * @param optionsMap is the hash map which generated by the command line parser which match the option name and its value appropriately.
     * @throws Exception if errors occur when executing command line or parsing data.
     */
    public static void generateAndSaveBenchmarkResults(Map<String, Object> optionsMap) throws Exception {
        String combinations = optionsMap.get("--benchmark-options-combinations").toString();
        String testDataDirectory = optionsMap.get("--test-ion-data").toString();
        String finalResultDirectory = optionsMap.get("<output_file>").toString();
        // Get the version of ion-java-benchmark-cli-jar-with-dependencies.jar
        String version = parseVersionFromPom(POM_FILE);
        String ionJavaBenchmarkInvoke = ION_JAVA_BENCHMARK_INVOKE_ELEMENT + version + JAR_WITH_DEPENDENCIES;
        String fileName = NEW_FILE;
        String makeFinalDirectoryCommand = MAKE_DIRECTORY + finalResultDirectory;
        IonList optionsCombinationsList;
        try (IonReader reader = IonReaderBuilder.standard().build(new BufferedInputStream(new FileInputStream(combinations)))) {
            reader.next();
            if (reader.getType().equals(IonType.LIST)) {
                optionsCombinationsList = (IonList) LOADER.load(reader).get(0);
            } else {
                throw new IllegalStateException("The data structure of options combinations file is not supported.");
            }
        }
        String subDirectoryName;
        // Create the directory that contains all results from different ion-java-benchmark invokes
        Process createFinalResultDirectory = Runtime.getRuntime().exec(makeFinalDirectoryCommand);
        File[] files = new File(testDataDirectory).listFiles();
        for (File testData : files) {
            for (int index = 0; index < optionsCombinationsList.size(); index++) {
                IonString combination = (IonString) optionsCombinationsList.get(index);
                String benchmarkOptionCombination = combination.stringValue();
                // Use ion-java-benchmark as a start point to construct directory name which gives user a better idea which benchmark results would be contained under this directory.
                subDirectoryName = File.separator + ION_JAVA_BENCHMARK + benchmarkOptionCombination.replace("--results-format ion ", "").replace("_", "-").replace(" ", "_") + testData.getName();
                Process makeSubDirectory = Runtime.getRuntime().exec(makeFinalDirectoryCommand + subDirectoryName);
                // When generating benchmark results for ion-java from the new commit, all benchmark results will be named as 'new.ion'.
                File fileCheck = new File(finalResultDirectory + subDirectoryName + File.separator + fileName);
                if (fileCheck.exists()) {
                    fileName = PREVIOUS_FILE;
                }
                // Execute ion-java-benchmark invoke.
                String commandLine = ionJavaBenchmarkInvoke + benchmarkOptionCombination + "--results-file " + finalResultDirectory + subDirectoryName + File.separator + fileName + " " + testData.getAbsolutePath();
                Process process = Runtime.getRuntime().exec(commandLine);
                printExecuteProcess(process);
            }
        }
    }

    /**
     * Print the executing process of the specific command line.
     *
     * @param process is the process that executing the specific command line.
     * @throws IOException if error occurs when reading executing process.
     */
    private static void printExecuteProcess(Process process) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
            System.out.println(System.lineSeparator());
        }
    }

    /**
     * Parse pom.xml and extract the version of ion-java-benchmark-cli.
     *
     * @param filePath the file path of pom.xml/
     * @return the version of ion-java-benchmark-cli in a string format.
     * @throws Exception if errors occur when parsing data.
     */
    private static String parseVersionFromPom(String filePath) throws Exception {
        DocumentBuilderFactory pomFileInstance = DocumentBuilderFactory.newInstance();
        pomFileInstance.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        DocumentBuilder db = pomFileInstance.newDocumentBuilder();
        Document pomFile = db.parse(new File(filePath));
        pomFile.getDocumentElement().normalize();
        String jarVersion = pomFile.getElementsByTagName("version").item(0).getTextContent();
        return jarVersion;
    }
}