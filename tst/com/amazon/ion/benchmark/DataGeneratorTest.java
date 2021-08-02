package com.amazon.ion.benchmark;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonDecimal;
import com.amazon.ion.IonFloat;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.Timestamp;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ion.util.IonStreamUtils;
import com.amazon.ionschema.IonSchemaSystem;
import com.amazon.ionschema.IonSchemaSystemBuilder;
import com.amazon.ionschema.Schema;
import com.amazon.ionschema.Type;
import com.amazon.ionschema.Violations;
import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.After;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class DataGeneratorTest {
    private static String outputFile = null;
    private final static IonSchemaSystem ISS = IonSchemaSystemBuilder.standard().build();
    private final static IonSystem SYSTEM = IonSystemBuilder.standard().build();
    private final static String INPUT_ION_STRUCT_FILE_PATH = "./tst/com/amazon/ion/benchmark/testStruct.isl";
    private final static String INPUT_ION_LIST_FILE_PATH = "./tst/com/amazon/ion/benchmark/testList.isl";
    private final static String INPUT_NESTED_ION_STRUCT_PATH = "./tst/com/amazon/ion/benchmark/testNestedStruct.isl";
    private final static String COMPARISON_REPORT = "./tst/com/amazon/ion/benchmark/testComparisonReport.ion";
    private final static String SCORE_DIFFERENCE = "scoreDifference";
    private final static String COMPARISON_REPORT_WITHOUT_REGRESSION = "./tst/com/amazon/ion/benchmark/testComparisonReportWithoutRegression.ion";
    private final static String THRESHOLD = "./tst/com/amazon/ion/benchmark/threshold.ion";

    /**
     * Construct IonReader for current output file in order to finish the following test process
     * @param optionsMap is the hash map which generated by the command line parser which match the option name and its value appropriately.
     * @return constructed IonReader
     * @throws Exception if errors occur during executing data generator process.
     */
    public static IonReader executeAndRead(Map<String, Object> optionsMap) throws Exception {
        outputFile = optionsMap.get("<output_file>").toString();
        GeneratorOptions.executeGenerator(optionsMap);
        return IonReaderBuilder.standard().build(new BufferedInputStream(new FileInputStream(outputFile)));
    }

    /**
     * Detect if violation occurs by comparing every single data in the generated file with Ion Schema constraints.
     * @param inputFile is the Ion Schema file.
     * @throws Exception if error occurs when checking if there is violation in the generated data.
     */
    public static void violationDetect(String inputFile) throws Exception {
        Map <String, Object> optionsMap = Main.parseArguments("generate", "--data-size", "5000", "--format", "ion_text", "--input-ion-schema", inputFile, "test8.ion");
        String inputFilePath = optionsMap.get("--input-ion-schema").toString();
        outputFile = optionsMap.get("<output_file>").toString();
        try (
                IonReader readerInput = IonReaderBuilder.standard().build(new BufferedInputStream(new FileInputStream(inputFilePath)));
                IonReader reader = DataGeneratorTest.executeAndRead(optionsMap);
        ) {
            // Get the name of Ion Schema.
            IonDatagram schema = ReadGeneralConstraints.LOADER.load(readerInput);
            String ionSchemaName = null;
            for (int i = 0; i < schema.size(); i++) {
                IonValue schemaValue = schema.get(i);
                if (schemaValue.getType().equals(IonType.STRUCT) && schemaValue.getTypeAnnotations()[0].equals(IonSchemaUtilities.KEYWORD_TYPE)) {
                    IonStruct constraintStruct = (IonStruct) schemaValue;
                    ionSchemaName = constraintStruct.get(IonSchemaUtilities.KEYWORD_NAME).toString();
                    break;
                }
            }
            // Construct new schema amd get the type of the Ion Schema.
            Schema newSchema = ISS.newSchema(schema.iterator());
            Type type = newSchema.getType(ionSchemaName);
            while (reader.next() != null) {
                IonValue value = SYSTEM.newValue(reader);
                Violations violations = type.validate(value);
                assertTrue("Violations " + violations + "found in value " + value, violations.isValid());
            }
        }
    }

    /**
     * Assert generated Ion data is the same type as expected.
     * @throws Exception if error occurs when executing Ion data generator.
     */
    @Test
    public void testGeneratedType() throws Exception {
        Map<String, Object> optionsMap = Main.parseArguments("generate", "--data-size", "500", "--data-type", "decimal", "test1.10n");
        try (IonReader reader = DataGeneratorTest.executeAndRead(optionsMap)) {
            while (reader.next() != null) {
                assertSame(reader.getType(), IonType.valueOf(optionsMap.get("--data-type").toString().toUpperCase()));
            }
        }
    }

    /**
     * Assert the exponent range of generated Ion decimals is conform with the expected range.
     * @throws Exception if error occurs when executing Ion data generator.
     */
    @Test
    public void testGeneratedDecimalExponentRange() throws Exception {
        Map<String, Object> optionsMap = Main.parseArguments("generate", "--data-size", "500", "--data-type", "decimal", "--decimal-exponent-range", "[0,10]", "test2.10n");
        try (IonReader reader = DataGeneratorTest.executeAndRead(optionsMap)) {
            List<Integer> range = WriteRandomIonValues.parseRange(optionsMap.get("--decimal-exponent-range").toString());
            while (reader.next() != null) {
                int exp = reader.decimalValue().scale();
                assertTrue(exp * (-1) >= range.get(0) && exp * (-1) <= range.get(1));
            }
        }
    }

    /**
     * Assert the range of coefficient digits number of generated Ion decimals is conform with the expected range.
     * @throws Exception if error occurs when executing Ion data generator.
     */
    @Test
    public void testGeneratedDecimalCoefficientRange() throws Exception {
        Map<String, Object> optionsMap = Main.parseArguments("generate", "--data-size", "500", "--data-type", "decimal", "--decimal-coefficient-digit-range", "[1,12]", "test3.10n");
        try (IonReader reader = DataGeneratorTest.executeAndRead(optionsMap)) {
            List<Integer> range = WriteRandomIonValues.parseRange(optionsMap.get("--decimal-coefficient-digit-range").toString());
            while (reader.next() != null) {
                BigInteger coefficient = reader.decimalValue().unscaledValue();
                double factor = Math.log(2) / Math.log(10);
                int digitCount = (int) (factor * coefficient.bitLength() + 1);
                if (BigInteger.TEN.pow(digitCount - 1).compareTo(coefficient) > 0) {
                    digitCount = digitCount - 1;
                }
                assertTrue(digitCount >= range.get(0) && digitCount <= range.get(1));
            }
        }
    }

    /**
     * Assert the format of generated file is conform with the expected format [ion_binary|ion_text].
     * @throws Exception if error occurs when executing Ion data generator.
     */
    @Test
    public void testGeneratedFormat() throws Exception {
        List<String> inputs = new ArrayList<>(Arrays.asList("ion_text","ion_binary"));
        for (int i = 0; i < 2; i++ ) {
            Map<String, Object> optionsMap = Main.parseArguments("generate", "--data-size", "500", "--data-type", "float", "--format", inputs.get(i), "test4.ion");
            GeneratorOptions.executeGenerator(optionsMap);
            String format = ((List<String>)optionsMap.get("--format")).get(0);
            outputFile = optionsMap.get("<output_file>").toString();
            Path path = Paths.get(outputFile);
            byte[] buffer = Files.readAllBytes(path);
            assertEquals(Format.valueOf(format.toUpperCase()) == Format.ION_BINARY, IonStreamUtils.isIonBinary(buffer));
        }
    }

    /**
     * Assert the unicode code point range of the character which constructed the generated Ion string is conform with the expect range.
     * @throws Exception if error occurs when executing Ion data generator.
     */
    @Test
    public void testGeneratedStringUniCodeRange() throws Exception {
        Map<String, Object> optionsMap = Main.parseArguments("generate", "--data-size", "500", "--data-type", "string", "--text-code-point-range", "[96,99]","test5.10n");
        try (IonReader reader = DataGeneratorTest.executeAndRead(optionsMap)) {
            List<Integer> range = WriteRandomIonValues.parseRange(optionsMap.get("--text-code-point-range").toString());
            while (reader.next() != null) {
                String str = reader.stringValue();
                for (int i = 0; i < str.length(); i++) {
                    int codePoint = Character.codePointAt(str, i);
                    int charCount = Character.charCount(codePoint);
                    //UTF characters may use more than 1 char to be represented
                    if (charCount == 2) {
                        i++;
                    }
                    assertTrue(codePoint >= range.get(0) && codePoint <= range.get(1));
                }
            }
        }
    }

    /**
     * Assert the generated timestamps is follow the precision and proportion of the given timestamp template.
     * @throws Exception if error occurs when executing Ion data generator.
     */
    @Test
    public void testGeneratedTimestampTemplateFormat() throws Exception{
        Map<String, Object> optionsMap = Main.parseArguments("generate", "--data-size", "500", "--data-type", "timestamp", "--timestamps-template", "[2021T]", "test6.10n");
        try (
                IonReader reader = DataGeneratorTest.executeAndRead(optionsMap);
                IonReader templateReader = IonReaderBuilder.standard().build(optionsMap.get("--timestamps-template").toString())
        ) {
            templateReader.next();
            templateReader.stepIn();
            reader.next();
            while (reader.isNullValue()) {
                while (templateReader.next() != null){
                    Timestamp templateTimestamp = templateReader.timestampValue();
                    Timestamp.Precision templatePrecision = templateTimestamp.getPrecision();
                    Timestamp.Precision currentPrecision = reader.timestampValue().getPrecision();
                    Integer templateOffset = templateTimestamp.getLocalOffset();
                    Integer currentOffset = reader.timestampValue().getLocalOffset();
                    assertSame(templatePrecision, currentPrecision);
                    if (currentOffset == null || currentOffset == 0) {
                        assertSame(currentOffset, templateOffset);
                    } else {
                        assertEquals(currentOffset >= -1439 && currentOffset <= 1439, templateOffset >= -1439 && templateOffset <= 1439);
                    }
                    if (currentPrecision == Timestamp.Precision.SECOND) {
                        assertEquals(reader.timestampValue().getDecimalSecond().scale(), templateTimestamp.getDecimalSecond().scale());
                    }
                    reader.next();
                }
            }
        }
    }

    /**
     * Assert the generated data size in bytes has an 10% difference with the expected size, this range is not available for Ion symbol, because the size of symbol is predicted.
     * @throws Exception if error occurs when executing Ion data generator
     */
    @Test
    public void testSizeOfGeneratedData() throws Exception {
        Map <String, Object> optionsMap = Main.parseArguments("generate", "--data-size", "5000", "--data-type", "timestamp", "--timestamps-template","[2021T]","test7.10n");
        GeneratorOptions.executeGenerator(optionsMap);
        int expectedSize = Integer.parseInt(optionsMap.get("--data-size").toString());
        outputFile = optionsMap.get("<output_file>").toString();
        Path filePath = Paths.get(outputFile);
        FileChannel fileChannel;
        fileChannel = FileChannel.open(filePath);
        int fileSize = (int)fileChannel.size();
        fileChannel.close();
        int difference = Math.abs(expectedSize - fileSize);
        assertTrue(difference <= 0.1 * expectedSize);
    }


    /**
     * Test if there's violation when generating Ion Struct based on Ion Schema.
     * @throws Exception if error occurs during the violation detecting process.
     */
    @Test
    public void testViolationOfIonStruct() throws Exception {
        DataGeneratorTest.violationDetect(INPUT_ION_STRUCT_FILE_PATH);
    }

    /**
     * Test if there's violation when generating Ion List based on Ion Schema.
     * @throws Exception if error occurs during the violation detecting process.
     */
    @Test
    public void testViolationOfIonList() throws Exception {
        DataGeneratorTest.violationDetect(INPUT_ION_LIST_FILE_PATH);
    }

    /**
     * Test if there's violation when generating nested Ion Struct based on Ion Schema.
     * @throws Exception if error occurs during the violation detecting process.
     */
    @Test
    public void testViolationOfNestedIonStruct() throws Exception {
        DataGeneratorTest.violationDetect(INPUT_NESTED_ION_STRUCT_PATH);
    }

    /**
     * Test the accuracy of the calculated results in the generated file.
     * @throws Exception if error occurs when reading the input file.
     */
    @Test
    public void testParseBenchmark() throws Exception {
        Map<String, Object> optionsMap = Main.parseArguments("compare", "--benchmark-result-previous", "./tst/com/amazon/ion/benchmark/IonLoaderBenchmarkResultPrevious.ion", "--benchmark-result-new", "./tst/com/amazon/ion/benchmark/IonLoaderBenchmarkResultNew.ion", "--threshold", "./tst/com/amazon/ion/benchmark/threshold.ion", "test11.ion");
        ParseAndCompareBenchmarkResults.compareResult(optionsMap);
        outputFile = optionsMap.get("<output_file>").toString();
        try (IonReader reader = IonReaderBuilder.standard().build(new BufferedInputStream(new FileInputStream(outputFile)))) {
            reader.next();
            reader.stepIn();
            while (reader.next() != null) {
                if (reader.getFieldName().equals(SCORE_DIFFERENCE)) {
                    reader.stepIn();
                    while (reader.next() != null) {
                        String benchmarkResultPrevious = optionsMap.get("--benchmark-result-previous").toString();
                        String benchmarkResultNew = optionsMap.get("--benchmark-result-new").toString();
                        BigDecimal previousScore = ParseAndCompareBenchmarkResults.getScore(benchmarkResultPrevious, reader.getFieldName());
                        BigDecimal newScore = ParseAndCompareBenchmarkResults.getScore(benchmarkResultNew, reader.getFieldName());
                        BigDecimal scoreDifference = newScore.subtract(previousScore);
                        BigDecimal relativeDifference = scoreDifference.divide(previousScore, RoundingMode.HALF_UP);
                        assertTrue(relativeDifference.equals(reader.decimalValue()));
                    }
                    reader.stepOut();
                }
            }
        }
    }

    /**
     * Test whether the detecting regression process can return the expected result when there is performance regression in the test file.
     * In this unit test we use an Ion file which contain regression on [·gc.alloc.rate] as input to test the detectRegression method.
     * @throws Exception if error occur when reading Ion data.
     */
    @Test
    public void testRegressionDetected() throws Exception {
        Map<String, BigDecimal> scoreMap = new HashMap<>();
        IonStruct scoresStruct;
        try (IonReader reader = IonReaderBuilder.standard().build(new BufferedInputStream(new FileInputStream(COMPARISON_REPORT)))) {
            reader.next();
            if (reader.getType().equals(IonType.STRUCT)) {
                IonStruct comparisonResult = (IonStruct) ReadGeneralConstraints.LOADER.load(reader).get(0);
                scoresStruct = (IonStruct) comparisonResult.get(ParseAndCompareBenchmarkResults.RELATIVE_DIFFERENCE_SCORE);
            } else {
                throw new IllegalStateException("The data structure of the comparison report is not supported.");
            }
        }
        for (String keyWord : ParseAndCompareBenchmarkResults.BENCHMARK_SCORE_KEYWORDS) {
            IonValue score = scoresStruct.get(keyWord);
            if (score.getType().equals(IonType.FLOAT)) {
                IonFloat scoreFloat = (IonFloat) score;
                scoreMap.put(keyWord, scoreFloat.bigDecimalValue());
            } else {
                IonDecimal scoreDecimal = (IonDecimal) score;
                scoreMap.put(keyWord, scoreDecimal.bigDecimalValue());
            }
        }
        String detectionResult = ParseAndCompareBenchmarkResults.detectRegression(THRESHOLD, scoreMap, COMPARISON_REPORT);
        assertTrue(detectionResult.equals("The performance regression detected when benchmark the ion-java from the new commit with the test data: testList.10n and parameters: read::{format:\"ION_BINARY\",type:\"FILE\",api:\"DOM\"}\n" +
                "The following aspects have regressions: {·gc.alloc.rate=-0.002851051607559}\n"));
    }

    /**
     * Test whether the detecting regression process can return the expected result when there is no performance regression in the test file.
     * In this unit test we use an Ion file which contain regression on [·gc.alloc.rate] as input to test the detectRegression method.
     * @throws Exception if error occur when reading Ion data.
     */
    @Test
    public void testRegressionNotDetected() throws Exception {
        Map<String, BigDecimal> scoreMap = new HashMap<>();
        IonStruct scoresStruct;
        try (IonReader reader = IonReaderBuilder.standard().build(new BufferedInputStream(new FileInputStream(COMPARISON_REPORT_WITHOUT_REGRESSION)))) {
            reader.next();
            if (reader.getType().equals(IonType.STRUCT)) {
                IonStruct comparisonResult = (IonStruct) ReadGeneralConstraints.LOADER.load(reader).get(0);
                scoresStruct = (IonStruct) comparisonResult.get(ParseAndCompareBenchmarkResults.RELATIVE_DIFFERENCE_SCORE);
            } else {
                throw new IllegalStateException("The data structure of the comparison report is not supported.");
            }
        }
        for (String keyWord : ParseAndCompareBenchmarkResults.BENCHMARK_SCORE_KEYWORDS) {
            IonValue score = scoresStruct.get(keyWord);
            if (score.getType().equals(IonType.FLOAT)) {
                IonFloat scoreFloat = (IonFloat) score;
                scoreMap.put(keyWord, scoreFloat.bigDecimalValue());
            } else {
                IonDecimal scoreDecimal = (IonDecimal) score;
                scoreMap.put(keyWord, scoreDecimal.bigDecimalValue());
            }
        }
        String detectionResult = ParseAndCompareBenchmarkResults.detectRegression(THRESHOLD, scoreMap, COMPARISON_REPORT_WITHOUT_REGRESSION);
        assertTrue(detectionResult == null);
    }

    /**
     * Delete all files generated in the test process.
     * @throws IOException if an error occur when deleting files.
     */
    @After
    public void deleteGeneratedFile() throws IOException {
        if (outputFile != null) {
            Path filePath = Paths.get(outputFile);
            if(Files.exists(filePath)) {
                Files.delete(filePath);
            }
        }
    }
}
