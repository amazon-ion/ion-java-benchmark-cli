package com.amazon.ion.benchmark;

/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import com.amazon.ion.IonList;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.Timestamp;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Generate specific scalar type of Ion data randomly, for some specific type, e.g. String, Decimal, Timestamp, users can put specifications on these types of Ion data.
 */
class WriteRandomIonValues {
    final static private IonSystem SYSTEM = IonSystemBuilder.standard().build();
    final static private List<Integer> DEFAULT_RANGE = WriteRandomIonValues.parseRange("[0, 1114111]");
    final static private Timestamp.Precision[] PRECISIONS = Timestamp.Precision.values();
    final static private int NO_CODE_POINT_LENGTH = 0;
    final static private List<Integer> NO_POINT_RANGE = null;
    final static private List<Integer> NO_EXPONENT_VALUE_RANGE = null;
    final static private List<Integer> NO_COEFFICIENT_DIGIT_RANGE = null;
    final static private String NO_TIMESTAMP_TEMPLATE = null;
    final static private IonStruct NO_CONSTRAINT_STRUCT = null;

    /**
     * Build up the writer based on the provided format (ion_text|ion_binary)
     * @param format the option to decide which writer to be constructed.
     * @param file the generated file which contains specified Ion data.
     * @return the writer which conforms with the required format.
     * @throws Exception if an error occurs while creating a file output stream.
     */
    private static IonWriter formatWriter(String format, File file) throws Exception {
        IonWriter writer;
        OutputStream out = new BufferedOutputStream(new FileOutputStream(file));
        Format formatName = Format.valueOf(format.toUpperCase());
        switch (formatName) {
            case ION_BINARY:
                writer = IonBinaryWriterBuilder.standard().withLocalSymbolTableAppendEnabled().build(out);
                break;
            case ION_TEXT:
                writer = IonTextWriterBuilder.standard().build(out);
                break;
            default:
                throw new IllegalStateException("Please input the format ion_text or ion_binary");
        }
        return writer;
    }

    /**
     * Use Ion-java parser to parse the data provided in the options which specify the range of data.
     * @param range the range needed to be parsed, normally in the format of "[Integer, Integer]"
     * @return a list of Integer which will be extracted in the following executions.
     */
    public static List<Integer> parseRange(String range) {
        IonReaderBuilder readerBuilder = IonReaderBuilder.standard();
        IonReader reader = readerBuilder.build(range);
        if (reader.next() != IonType.LIST) throw new IllegalStateException("Please provide a list type");
        reader.stepIn();
        List<Integer> result = new ArrayList<>();
        while (reader.next() != null) {
            if (reader.getType() != IonType.INT) throw new IllegalStateException("Please put integer elements inside of the list");
            int value = reader.intValue();
            result.add(value);
        }
        reader.stepOut();

        if (reader.next() != null) throw new IllegalStateException("Only one list is accepted");
        if (result.get(0) > result.get(1)) throw new IllegalStateException("The value of the lower bound should be smaller than the upper bound");
        if (result.size() != 2) throw new IllegalStateException("Please put two integers inside of the list");
        return result;
    }

    /**
     * Print the successfully generated data notification which includes the file path information.
     * @param path identifies the output file path.
     */
    private static void printInfo(String path) {
        String fileName = path.substring(path.lastIndexOf("/") + 1);
        System.out.println(fileName + " generated successfully ! ");
        if (fileName.equals(path)) {
            System.out.println("Generated data is under the current directory");
        } else {
            System.out.println("File path: " + path);
        }
    }

    /**
     * Write random Ion strings into target file, and all data conform with the specifications provided by the options if these are provided. Otherwise, this method will generate Ion string data randomly.
     * @param size specifies the size in bytes of the generated file.
     * @param type determines which type of data will be generated.
     * @param path the destination of the generated file.
     * @param codePointRange provides the range of Unicode code point of characters which construct the Ion string.
     * @param format the format of output file (ion_binary | ion_text).
     * @param codePointLength is the length of generated Ion String.
     * @throws Exception if an error occurs when building up the writer.
     */
    public static void writeRandomStrings(int size, IonType type, String path, String codePointRange, String format, int codePointLength) throws Exception {
        File file = new File(path);
        try (IonWriter writer = WriteRandomIonValues.formatWriter(format, file)) {
            List<Integer> pointRange = WriteRandomIonValues.parseRange(codePointRange);

            if (pointRange.get(0) < 0) throw new IllegalStateException("Please provide the valid range inside of [0, 1114111]");
            if (pointRange.get(1) > Character.MAX_CODE_POINT) throw new IllegalStateException("Please provide the valid range inside of [0, 1114111]");
            WriteRandomIonValues.writeRequestedSizeFile(size, writer, file, type, codePointLength, pointRange, NO_EXPONENT_VALUE_RANGE, NO_COEFFICIENT_DIGIT_RANGE, NO_TIMESTAMP_TEMPLATE, NO_CONSTRAINT_STRUCT);
        }
        WriteRandomIonValues.printInfo(path);
    }

    /**
     * Write random Ion decimals into target file. If the options which specify the range of exponent and digits number of coefficient are provided, the generated data will conform with the specifications.
     * Otherwise, this method will generate the random decimals based on the default range.
     * @param size specifies the size in bytes of the generated file.
     * @param type determines which type of data will be generated.
     * @param path the destination of the generated file.
     * @param format the format of output file (ion_binary | ion_text).
     * @param expRange the range of exponent when the decimal represented in coefficient * 10 ^ exponent.
     * @param coefficientDigit the range of digit number of coefficient when the decimal represented in coefficient * 10 ^ exponent.
     * @throws Exception if an error occurs when building up the writer.
     */
    public static void writeRandomDecimals(int size, IonType type, String path, String format, String expRange, String coefficientDigit) throws Exception {
        File file = new File(path);
        try (IonWriter writer = WriteRandomIonValues.formatWriter(format, file)) {
            List<Integer> expValRange = WriteRandomIonValues.parseRange(expRange);
            List<Integer> coefficientDigitRange = WriteRandomIonValues.parseRange(coefficientDigit);
            if (coefficientDigitRange.get(0) <= 0) throw new IllegalStateException ("The coefficient digits should be positive integer");
            WriteRandomIonValues.writeRequestedSizeFile(size, writer, file, type, NO_CODE_POINT_LENGTH, NO_POINT_RANGE, expValRange, coefficientDigitRange, NO_TIMESTAMP_TEMPLATE, NO_CONSTRAINT_STRUCT);
        }
        WriteRandomIonValues.printInfo(path);
    }

    /**
     * Write random Ion integers into target file, and all data conform with the specifications provided by the options, e.g. size, format and the output file path.
     * @param size specifies the size in bytes of the generated file.
     * @param type determines which type of data will be generated.
     * @param path the destination of the generated file.
     * @param format the format of output file (ion_binary | ion_text).
     * @throws Exception if an error occurs when building up the writer.
     */
    public static void writeRandomInts(int size, IonType type, String format, String path) throws Exception {
        File file = new File(path);
        try (IonWriter writer = WriteRandomIonValues.formatWriter(format, file)) {
           WriteRandomIonValues.writeRequestedSizeFile(size, writer, file, type, NO_CODE_POINT_LENGTH, NO_POINT_RANGE, NO_EXPONENT_VALUE_RANGE, NO_COEFFICIENT_DIGIT_RANGE, NO_TIMESTAMP_TEMPLATE, NO_CONSTRAINT_STRUCT);
        }
        WriteRandomIonValues.printInfo(path);
    }

    /**
     * Write random Ion floats into target file, and all data conform with the specifications provided by the options, e.g. size, format and the output file path.
     * @param size specifies the size in bytes of the generated file.
     * @param type determines which type of data will be generated.
     * @param format the format of output file (ion_binary | ion_text).
     * @param path the destination of the generated file.
     * @throws Exception if an error occurs when building up the writer.
     */
    public static void writeRandomFloats(int size, IonType type, String format, String path) throws Exception {
        File file = new File(path);
        try (IonWriter writer = WriteRandomIonValues.formatWriter(format, file)) {
            WriteRandomIonValues.writeRequestedSizeFile(size, writer, file, type, NO_CODE_POINT_LENGTH, NO_POINT_RANGE, NO_EXPONENT_VALUE_RANGE, NO_COEFFICIENT_DIGIT_RANGE, NO_TIMESTAMP_TEMPLATE, NO_CONSTRAINT_STRUCT);
        }
        WriteRandomIonValues.printInfo(path);
    }

    /**
     *This method is not available now
     */
    private static void writeListOfRandomFloats() throws Exception {
        File file = new File("manyLargeListsOfRandomFloats.10n");
        try (OutputStream out = new BufferedOutputStream(new FileOutputStream(file));
                IonWriter writer = IonBinaryWriterBuilder.standard().withFloatBinary32Enabled().build(out)) {
            Random random = new Random();
            for (int j = 0; j < 100; j++) {
                writer.stepIn(IonType.LIST);
                // Target about 1MB of data. Floats will average at 7 bytes, and we're writing 2
                // per iteration.
                for (int i = 0; i < (1_000_000 / 7 / 2); i++) {
                    writer.writeFloat(Double.longBitsToDouble(random.nextLong()));
                    writer.writeFloat(Float.intBitsToFloat(random.nextInt()));
                }
                writer.stepOut();
            }
        }
        System.out.println("Finished writing floats. Verifying.");
        try (IonReader reader = IonReaderBuilder.standard().build(new BufferedInputStream(new FileInputStream(file)))) {
            while (reader.next() != null) {
                reader.stepIn();
                while (reader.next() != null) {
                    if (reader.getType() != IonType.FLOAT) {
                        throw new IllegalStateException("Found non-float");
                    }
                    double value = reader.doubleValue();
                }
                reader.stepOut();
            }
        }
        System.out.println("Done. Size: " + file.length());
    }

    /**
     * Generate random offset without any specification
     * @param random is the random number generator.
     * @return random offset [Z(+00:00) | -00:00 | random offset].
     */
    private static Integer localOffset(Random random) {
        // Offsets are in minutes, [-23:59, 23:59], i.e. [-1439, 1439].
        // The most common offset is Z (00:00), while unknown (-00:00) may also be common.
        Integer offsetMinutes = random.nextInt(6000) - 2000;
        if (offsetMinutes > 1439) {
            // This means about 43% of timestamps will have offset Z (UTC).
            offsetMinutes = 0;
        } else if (offsetMinutes < -1439) {
            // This means about 9% of timestamps will have unknown offset (-00:00).
            offsetMinutes = null;
        }
        return offsetMinutes;
    }

    /**
     * Generate random fractional second which the precision conforms with the precision of the second in template timestamp.
     * @param random is the random number generator.
     * @param scale is the the scale of the decimal second in the current template timestamp.
     * @return a random timestamp second in a fractional format which conforms with the current template timestamp.
     */
    private static BigDecimal randomSecondWithFraction(Random random, int scale) {
        int second = random.nextInt(60);
        if (scale != 0) {
            StringBuilder coefficientStr = new StringBuilder();
            for (int digit = 0; digit < scale; digit++) {
                coefficientStr.append(random.nextInt(10));
            }
            BigDecimal fractional = new BigDecimal(coefficientStr.toString());
            BigDecimal fractionalSecond = fractional.scaleByPowerOfTen(scale * (-1));
            return fractionalSecond.add(BigDecimal.valueOf(second));
        } else {
            return BigDecimal.valueOf(second);
        }
    }

    /**
     * Write random Ion timestamps into target file, and all data conform with the specifications provided by the options.
     * If timestamps template provided, the generated timestamps will be conformed with the precision and portion of the template. Otherwise, the data will be generated randomly.
     * @param size specifies the size in bytes of the generated file.
     * @param type determines which type of data will be generated.
     * @param path the destination of the generated file.
     * @param timestampTemplate is a string which provides a series of template timestamps which data generating process will follow with.
     * @param format the format of output file (ion_binary | ion_text).
     * @throws Exception if an error occurs when building up the writer.
     */
    public static void writeRandomTimestamps(int size, IonType type, String path, String timestampTemplate, String format) throws Exception {
        File file = new File(path);
        try (IonWriter writer = WriteRandomIonValues.formatWriter(format, file)) {
            WriteRandomIonValues.writeRequestedSizeFile(size, writer, file, type, NO_CODE_POINT_LENGTH, NO_POINT_RANGE, NO_EXPONENT_VALUE_RANGE, NO_COEFFICIENT_DIGIT_RANGE, timestampTemplate, NO_CONSTRAINT_STRUCT);
        }
        WriteRandomIonValues.printInfo(path);
    }

    /**
     * Execute the writing timestamp data process based on the precision provided by the template timestamps.
     * @param precision is the precision of current template timestamp.
     * @param value is the current timestamp in the provided template.
     * @throws IOException if an error occurs when writing timestamp value.
     */
    public static Timestamp writeTimestamp(Timestamp.Precision precision,  Timestamp value) throws IOException {
        Timestamp timestamp;
        Random random = new Random();
        switch (precision) {
            case YEAR:
                timestamp = Timestamp.forYear(random.nextInt(9998) + 1);
                break;
            case MONTH:
                timestamp = Timestamp.forMonth(random.nextInt(9998) + 1, random.nextInt(12) + 1);
                break;
            case DAY:
                timestamp = Timestamp.forDay(
                        random.nextInt(9998) + 1,
                        random.nextInt(12) + 1,
                        random.nextInt(28) + 1 // Use max 28 for simplicity. Not including up to 31 is not going to affect the measurement.
                );
                break;
            case MINUTE:
                if (value == null) {
                    timestamp = Timestamp.forMinute(
                            random.nextInt(9998) + 1,
                            random.nextInt(12) + 1,
                            random.nextInt(28) + 1, // Use max 28 for simplicity. Not including up to 31 is not going to affect the measurement.
                            random.nextInt(24),
                            random.nextInt(60),
                            localOffset(random)
                    );
                } else {
                    Integer localOffSet;
                    if (value.getLocalOffset() != null && value.getLocalOffset() != 0) {
                        localOffSet = random.nextInt(2878) - 1439;
                    } else {
                        localOffSet = value.getLocalOffset();
                    }
                    timestamp = Timestamp.forMinute(random.nextInt(9998) + 1, random.nextInt(12) + 1,
                            random.nextInt(28) + 1, // Use max 28 for simplicity. Not including up to 31 is not going to
                            // affect the measurement.
                            random.nextInt(24), random.nextInt(60), localOffSet);
                }
                break;
            case SECOND:
                if (value == null) {
                    timestamp = Timestamp.forSecond(
                            random.nextInt(9998) + 1,
                            random.nextInt(12) + 1,
                            random.nextInt(28) + 1, // Use max 28 for simplicity. Not including up to 31 is not going to affect the measurement.
                            random.nextInt(24),
                            random.nextInt(60),
                            random.nextInt(60),
                            localOffset(random)
                    );
                } else {
                    Integer localOffSet;
                    if (value.getLocalOffset() != null && value.getLocalOffset() != 0) {
                        localOffSet = random.nextInt(2878) - 1439;
                    } else {
                        localOffSet = value.getLocalOffset();
                    }
                    timestamp = Timestamp.forSecond(random.nextInt(9998) + 1, random.nextInt(12) + 1,
                            random.nextInt(28) + 1, // Use max 28 for simplicity. Not including up to 31 is not going to
                            // affect the measurement.
                            random.nextInt(24), random.nextInt(60), randomSecondWithFraction(random,value.getDecimalSecond().scale()), localOffSet);
                }
                break;
            case FRACTION:
                int scale = random.nextInt(20);
                timestamp = Timestamp.forSecond(
                        random.nextInt(9998) + 1,
                        random.nextInt(12) + 1,
                        random.nextInt(28) + 1, // Use max 28 for simplicity. Not including up to 31 is not going to affect the measurement.
                        random.nextInt(24),
                        random.nextInt(60),
                        randomSecondWithFraction(random,scale),
                        localOffset(random)
                );
                break;
            default:
                throw new IllegalStateException();
        }
        return timestamp;
    }

    /**
     * Write random Ion blobs/clobs into target file, and all data conform with the specifications provided by the options, e.g. size, format, type(blob/clob) and the output file path.
     * @param size specifies the size in bytes of the generated file.
     * @param path the destination of the generated file.
     * @param format the format of output file (ion_binary | ion_text).
     * @param type determines which type of data will be generated [blob | clob].
     * @throws Exception if an error occurs when building up the writer.
    */
    public static void writeRandomLobs(int size, IonType type, String format, String path) throws Exception {
        File file = new File(path);
        try (IonWriter writer = WriteRandomIonValues.formatWriter(format, file)) {
            Random random = new Random();
            int currentSize = 0;
            while (currentSize <= size) {
                WriteRandomIonValues.constructLobs(random, type, writer);
                writer.flush();
                currentSize = (int) file.length();
            }
        }
        WriteRandomIonValues.printInfo(path);
    }

    /**
     * This method is not available now.
     * @throws Exception
     */
    private static void writeRandomAnnotatedFloats() throws Exception {
        File file = new File("randomAnnotatedFloats.10n");
        List<String> annotations = new ArrayList<>(500);
        Random random = new Random();
        for (int i = 0; i < 500; i++) {
            int length = random.nextInt(20);
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < length; j++) {
                int codePoint;
                int type;
                do {
                    codePoint = random.nextInt(Character.MAX_CODE_POINT);
                    type = Character.getType(codePoint);
                } while (type == Character.PRIVATE_USE || type == Character.SURROGATE || type == Character.UNASSIGNED);
                sb.appendCodePoint(codePoint);
            }
            annotations.add(sb.toString());
        }
        try (OutputStream out = new BufferedOutputStream(new FileOutputStream(file));
                IonWriter writer = IonBinaryWriterBuilder.standard().build(out)) {
            // Target about 100MB of data. Annotated floats will average around 14 bytes.
            for (int i = 0; i < (100_000_000 / 14); i++) {
                // 60% of values will have 1 annotation; 40% will have 2 or 3.
                int numberOfAnnotations = random.nextInt(5) + 1;
                if (numberOfAnnotations > 3) {
                    numberOfAnnotations = 1;
                }
                for (int j = 0; j < numberOfAnnotations; j++) {
                    writer.addTypeAnnotation(annotations.get(random.nextInt(500)));
                }
                writer.writeFloat(Double.longBitsToDouble(random.nextLong()));
            }
        }
    }

    /**
     * Write random Ion symbols into target file, and all data conform with the specifications provided by the options, e.g. size, format and the output file path.
     * @param size specifies the size in bytes of the generated file.
     * @param format the format of output file (ion_binary | ion_text).
     * @param path the destination of the generated file.
     * @throws Exception if an error occurs when building up the writer.
     */
    public static void writeRandomSymbolValues(int size, String format, String path) throws Exception {
        File file = new File(path);
        try (IonWriter writer = WriteRandomIonValues.formatWriter(format, file)) {
            List<String> symbols = new ArrayList<>(500);
            Random random = new Random();
            for (int i = 0; i < 500; i++) {
                int length = random.nextInt(20);
                StringBuilder sb = new StringBuilder();
                for (int j = 0; j < length; j++) {
                    int codePoint;
                    int charactereType;
                    do {
                        codePoint = random.nextInt(Character.MAX_CODE_POINT);
                        charactereType = Character.getType(codePoint);
                    } while (charactereType == Character.PRIVATE_USE || charactereType == Character.SURROGATE || charactereType == Character.UNASSIGNED);
                    sb.appendCodePoint(codePoint);
                }
                symbols.add(sb.toString());
            }
            for (int i = 0; i < size / 2; i++) {
                writer.writeSymbol(symbols.get(random.nextInt(500)));
            }
        }
        WriteRandomIonValues.printInfo(path);
    }

    /**
     * This method is used for generating requested size file by comparing the current file size and the target size.
     * @param size specifies the size in bytes of the generated file.
     * @param writer writer is IonWriter.
     * @param file the generated file which contains specified Ion data.
     * @param type determines which type of data will be generated.
     * @param codePointLength is the length of generated Ion String.
     * @param pointRange provides the range of Unicode code point of characters which construct the Ion string.
     * @param expValRange the range of exponent when the decimal represented in coefficient * 10 ^ exponent.
     * @param coefficientDigitRange the range of digit number of coefficient when the decimal represented in coefficient * 10 ^ exponent.
     * @param timestampTemplate  is a string which provides a series of template timestamps which data generating process will follow with.
     * @param constraintStruct is an IonStruct which contains the top-level constraints in Ion Schema.
     * @throws IOException if an error occur when writing generated data.
     */
    public static void writeRequestedSizeFile(int size, IonWriter writer, File file, IonType type, int codePointLength, List<Integer> pointRange, List<Integer> expValRange, List<Integer> coefficientDigitRange, String timestampTemplate, IonStruct constraintStruct) throws Exception {
        Random random = new Random();
        int currentSize = 0;
        int count = 0;
        // Determine how many values should be write before the writer.flush()
        while (currentSize <= 0.05 * size) {
            WriteRandomIonValues.writeDataToFile(type, writer, random, codePointLength, pointRange, expValRange, coefficientDigitRange, timestampTemplate, constraintStruct);
            count += 1;
            writer.flush();
            currentSize = (int) file.length();
        }
        while (currentSize <= size) {
            for (int i = 0; i < count; i++) {
                WriteRandomIonValues.writeDataToFile(type, writer, random, codePointLength, pointRange, expValRange, coefficientDigitRange, timestampTemplate, constraintStruct);
            }
            writer.flush();
            currentSize = (int) file.length();
        }
    }

    /**
     * This methods will be reused by different data generator
     * @param type determines which type of data will be generated.
     * @param writer writer is IonWriter.
     * @param random random is the random number generator.
     * @param codePointLength is the length of generated Ion String.
     * @param pointRange provides the range of Unicode code point of characters which construct the Ion string.
     * @param expValRange the range of exponent when the decimal represented in coefficient * 10 ^ exponent.
     * @param coefficientDigitRange the range of digit number of coefficient when the decimal represented in coefficient * 10 ^ exponent.
     * @param timestampTemplate  is a string which provides a series of template timestamps which data generating process will follow with.
     * @param constraintStruct is an IonStruct which contains the top-level constraints in Ion Schema.
     * @throws IOException if an error occur during the data writing process.
     */
    private static void writeDataToFile(IonType type, IonWriter writer, Random random, int codePointLength, List<Integer> pointRange, List<Integer> expValRange, List<Integer> coefficientDigitRange, String timestampTemplate, IonStruct constraintStruct) throws Exception {
        IonList annotationList;
        switch (type) {
            case FLOAT:
                WriteRandomIonValues.constructFloat(writer, random);
                break;
            case STRING:
                writer.writeString(WriteRandomIonValues.constructString(pointRange, codePointLength));
                break;
            case DECIMAL:
                writer.writeDecimal(WriteRandomIonValues.constructDecimal(expValRange, coefficientDigitRange));
                break;
            case INT:
                long intValue = WriteRandomIonValues.constructInt();
                writer.writeInt(intValue);
                break;
            case TIMESTAMP:
                Timestamp timestamp = WriteRandomIonValues.constructTimestamp(timestampTemplate);
                writer.writeTimestamp(timestamp);
                break;
            case STRUCT:
                annotationList = IonSchemaUtilities.getAnnotation(constraintStruct);
                WriteRandomIonValues.constructIonStruct(constraintStruct, annotationList, writer);
                break;
            case LIST:
                annotationList = IonSchemaUtilities.getAnnotation(constraintStruct);
                WriteRandomIonValues.constructIonList(writer, constraintStruct, annotationList);
                break;
        }
    }

    /**
     * Construct string with the characters which unicode code point is inside of the provided range.
     * @param pointRange is unicode code point range which is in a List format.
     * @param codePointsLengthBound is the length of generated Ion String.
     * @return constructed string.
     */
    public static String constructString(List<Integer> pointRange, int codePointsLengthBound) {
        Random random = new Random();
        StringBuilder sb = new StringBuilder();
        for (int j = 0; j < codePointsLengthBound; j++) {
            int codePoint;
            int type;
            do {
                codePoint = random.nextInt(pointRange.get(1) - pointRange.get(0) + 1) + pointRange.get(0);
                type = Character.getType(codePoint);
            } while (type == Character.PRIVATE_USE || type == Character.SURROGATE || type == Character.UNASSIGNED);
            sb.appendCodePoint(codePoint);
        }
        return sb.toString();
    }

    /**
     * Construct the decimal based on the provided exponent range and coefficient digit number range
     * @param expValRange the range of exponent when the decimal represented in coefficient * 10 ^ exponent.
     * @param coefficientDigitRange the range of number of digits in the coefficient when the decimal represented in coefficient * 10 ^ exponent.
     * @return the constructed decimal.
     */
    public static BigDecimal constructDecimal(List<Integer> expValRange, List<Integer> coefficientDigitRange) {
        Random random = new Random();
        int exp = random.nextInt((expValRange.get(1) - expValRange.get(0)) + 1) + expValRange.get(0);
        int randDigits = random.nextInt((coefficientDigitRange.get(1) - coefficientDigitRange.get(0)) + 1) + coefficientDigitRange.get(0);

        StringBuilder rs = new StringBuilder();
        for (int digit = 0; digit < randDigits; digit++) {
            rs.append(random.nextInt(9) + 1);
        }
        BigDecimal coefficient = new BigDecimal(rs.toString());
        return coefficient.scaleByPowerOfTen(exp);
    }

    /**
     * Generate random integers which composed by different length of data into the output file.
     */
    public static long constructInt() {
        Random random = new Random();
        long longValue = random.nextLong();
        return longValue;
    }

    /**
     * Write random Ion floats
     * @param writer is the Ionwriter which can write Ion data into the targer file.
     * @param random is the random number generator.
     * @throws IOException if error occurs during the writing process.
     */
    public static void constructFloat(IonWriter writer, Random random) throws IOException {
        writer.writeFloat(Double.longBitsToDouble(random.nextLong()));
        writer.writeFloat(Float.intBitsToFloat(random.nextInt()));
    }

    /**
     * Construct output timestamps that follow the timestamp template or generate data randomly
     * @param timestampTemplate is a string which provides a series of timestamps that are used as templates when generating data.
     * @throws IOException if an error occurs when writing timestamps.
     */
    public static Timestamp constructTimestamp(String timestampTemplate) throws IOException {
        Timestamp timestamp = null;
        if (timestampTemplate != null) {
            IonReader templateReader = IonReaderBuilder.standard().build(timestampTemplate);
            if (templateReader.next() != IonType.LIST) throw new IllegalStateException("Please provide a list type");
            templateReader.stepIn();
            while (templateReader.next() != null) {
                Timestamp value;
                if (templateReader.getType() == IonType.TIMESTAMP) {
                    value = templateReader.timestampValue();
                } else {
                    throw new IllegalStateException("Please provide timestamp in the template list");
                }
                Timestamp.Precision precision = value.getPrecision();
                timestamp = WriteRandomIonValues.writeTimestamp(precision, value);
            }
            templateReader.close();
            if (templateReader.next() != null) throw new IllegalStateException("Only one template list is needed");
        } else {
            Random random = new Random();
            Timestamp.Precision precision = PRECISIONS[random.nextInt(PRECISIONS.length)];
            timestamp = WriteRandomIonValues.writeTimestamp(precision, null);
        }
        return timestamp;
    }

    /**
     * Execute the process of writing clob / blob data into target file.
     * @param random is the random number generator.
     * @param type determines which type of data will be generated [blob | clob].
     * @param writer writes Ion clob/blob data.
     * @throws IOException if an error occurs during the writing process
     */
    public static void constructLobs(Random random, IonType type, IonWriter writer) throws IOException {
        byte[] randomBytes = new byte[random.nextInt(512)];
        random.nextBytes(randomBytes);
        switch (type) {
            case CLOB:
                writer.writeClob(randomBytes);
                break;
            case BLOB:
                writer.writeBlob(randomBytes);
                break;
            default:
                throw new IllegalStateException ("Please provide CLOB or BLOB");
        }
    }

    /**
     * Write Ion structs which conform with the constraints in Ion Schema.
     * @param format is the format of the generated file, select from set (ion_text | ion_binary).
     * @param size specifies the size in bytes of the generated file.
     * @param path the destination of the generated file.
     * @param constraintStruct is an IonStruct which contains the top-level constraints in Ion Schema.
     * @throws Exception if errors occur when writing data.
     */
    public static void writeRandomStructValues(int size, String format, String path, IonStruct constraintStruct) throws Exception {
        File file = new File(path);
        IonType type = IonType.STRUCT;
        try (IonWriter writer = WriteRandomIonValues.formatWriter(format, file)) {
            WriteRandomIonValues.writeRequestedSizeFile(size, writer, file, type, NO_CODE_POINT_LENGTH, NO_POINT_RANGE, NO_EXPONENT_VALUE_RANGE, NO_COEFFICIENT_DIGIT_RANGE, NO_TIMESTAMP_TEMPLATE, constraintStruct);
            WriteRandomIonValues.printInfo(path);
        }
    }

    /**
     * Construct Ion structs based on the provided constraints.
     * @param constraintStruct is an IonStruct which contains the top-level constraints in Ion Schema.
     * @param annotations is an IonList represents the value of annotations.
     * @param writer writes Ion struct data.
     * @throws IOException if errors occur when writing data.
     */
    public static void constructIonStruct(IonStruct constraintStruct, IonList annotations, IonWriter writer) throws Exception {
        Random random = new Random();
        IonStruct fields = (IonStruct) constraintStruct.get(IonSchemaUtilities.KEYWORD_FIELDS);
        try (IonReader reader = IonReaderBuilder.standard().build(fields)) {
            reader.next();
            reader.stepIn();
            for (int i = 0; i < annotations.size(); i++) {
                writer.addTypeAnnotation(annotations.get(i).toString());
            }
            writer.stepIn(IonType.STRUCT);
            while (reader.next() != null) {
                String fieldName = reader.getFieldName();
                IonValue struct = SYSTEM.newValue(reader);
                IonStruct value = (IonStruct) struct;
                // If the value of "occurs" is optional, the integer represents this value is 1 or 0.
                int occurTime = IonSchemaUtilities.parseConstraints(value, IonSchemaUtilities.KEYWORD_OCCURS);
                if (occurTime == 0) {
                    continue;
                }
                writer.setFieldName(fieldName);
                IonType type = IonType.valueOf(value.get(IonSchemaUtilities.KEYWORD_TYPE).toString().toUpperCase());
                switch (type) {
                    // If more types of Ion data are available, the logic should be added below.
                    case STRING:
                        int codePointsLengthBound = 20;
                        if (value.get(IonSchemaUtilities.KEYWORD_CODE_POINT_LENGTH) != null) {
                            codePointsLengthBound = IonSchemaUtilities.parseConstraints(value, IonSchemaUtilities.KEYWORD_CODE_POINT_LENGTH);
                        }
                        String stringValue = WriteRandomIonValues.constructString(DEFAULT_RANGE, codePointsLengthBound);
                        writer.writeString(stringValue);
                        break;
                    case TIMESTAMP:
                        // Call the function that extract the precision of the timestamp
                        Timestamp.Precision precision = PRECISIONS[random.nextInt(PRECISIONS.length)];
                        if (value.get(IonSchemaUtilities.KEYWORD_TIMESTAMP_PRECISION) != null) {
                            precision = IonSchemaUtilities.getTimestampPrecisionTemplate(value);
                        }
                        Timestamp timestampValue = WriteRandomIonValues.writeTimestamp(precision, null);
                        writer.writeTimestamp(timestampValue);
                        break;
                    case LIST:
                        IonList annotationList = IonSchemaUtilities.getAnnotation(value);
                        WriteRandomIonValues.constructIonList(writer, value, annotationList);
                        break;
                    default:
                        throw new IllegalStateException(type + " is not supported when generating Ion Struct based on Ion Schema.");
                }
            }
            writer.stepOut();
        }
    }

    /**
     * Write random Ion Lists which conform with the constraints in Ion Schema.
     * @param size specifies the size in bytes of the generated file.
     * @param format is the format of the generated file, select from set (ion_text | ion_binary).
     * @param path the destination of the generated file.
     * @param constraintStruct is an IonStruct which contains the top-level constraints in Ion Schema.
     * @throws Exception if errors occur when writing data.
     */
    public static void writeRandomListValues(int size, String format, String path, IonStruct constraintStruct) throws Exception {
        File file = new File(path);
        IonType type = IonType.LIST;
        try (IonWriter writer = WriteRandomIonValues.formatWriter(format, file)) {
            WriteRandomIonValues.writeRequestedSizeFile(size, writer, file, type, NO_CODE_POINT_LENGTH, NO_POINT_RANGE, NO_EXPONENT_VALUE_RANGE, NO_COEFFICIENT_DIGIT_RANGE, NO_TIMESTAMP_TEMPLATE, constraintStruct);
            WriteRandomIonValues.printInfo(path);
        }
    }

    /**
     * Construct Ion List based on the constraints provided by Ion Schema.
     * @param writer is Ion Writer.
     * @param constraints is an IonStruct which contains the top-level constraints in Ion Schema.
     * @param annotationList  is an IonList represents the value of annotations.
     * @throws Exception if errors occur when reading or writing data.
     */
    public static void constructIonList(IonWriter writer, IonStruct constraints, IonList annotationList) throws Exception {
        // When there's only one required element in Ion List and the length of generated Ion List is not specified, we set the default length as a integer smaller than 20.
        int containerLength = IonSchemaUtilities.parseConstraints(constraints, IonSchemaUtilities.KEYWORD_CONTAINER_LENGTH);
        int occurrences;
        try (IonReader reader = IonReaderBuilder.standard().build(constraints)) {
            reader.next();
            reader.stepIn();
            while (reader.next() != null) {
                if (annotationList != null) {
                    for (int i = 0; i < annotationList.size(); i++) {
                        writer.addTypeAnnotation(annotationList.get(i).toString());
                    }
                }
                writer.stepIn(IonType.LIST);
                // If constraint name is 'element', only one type of Ion Data is specified.
                if (constraints.get(IonSchemaUtilities.KEYWORD_ELEMENT) != null) {
                    IonType type = IonType.valueOf(constraints.get(IonSchemaUtilities.KEYWORD_ELEMENT).toString().toUpperCase());
                    for (int i = 0; i < containerLength; i++) {
                        occurrences = 1;
                        WriteRandomIonValues.constructScalarTypeData(type, writer, occurrences);
                    }
                    break;
                } else if (constraints.get(IonSchemaUtilities.KEYWORD_ORDERED_ELEMENTS) != null) {
                    IonList orderedElement = (IonList) constraints.get(IonSchemaUtilities.KEYWORD_ORDERED_ELEMENTS);
                    for (int index = 0; index < orderedElement.size(); index++) {
                        IonType elementType = orderedElement.get(index).getType();
                        IonType valueType;
                        switch (elementType) {
                            case SYMBOL:
                                occurrences = 1;
                                valueType = IonType.valueOf(orderedElement.get(index).toString().toUpperCase());
                                WriteRandomIonValues.constructScalarTypeData(valueType, writer, occurrences);
                                break;
                            case STRUCT:
                                IonStruct structConstraints = (IonStruct) orderedElement.get(index);
                                occurrences = IonSchemaUtilities.parseConstraints(structConstraints, IonSchemaUtilities.KEYWORD_OCCURS);
                                if(occurrences == 0) {
                                    break;
                                }
                                valueType = IonType.valueOf(structConstraints.get(IonSchemaUtilities.KEYWORD_TYPE).toString().toUpperCase());
                                WriteRandomIonValues.constructScalarTypeData(valueType, writer, occurrences);
                                break;
                        }
                    }
                    writer.stepOut();
                    return;
                }
            }
            writer.stepOut();
        }
    }

    /**
     * Construct scalar type Ion data based on the occurrence time. This method is mainly reused during the process of generating Ion List which will specify the occurrence time.
     * @param valueType is IonType of the data needed to be written in Ion List.
     * @param writer is IonWriter.
     * @param occurTime is the occurrence time of the element in Ion List.
     * @throws IOException if errors occur when writing data.
     */
    public static void constructScalarTypeData(IonType valueType, IonWriter writer, int occurTime) throws IOException {
        for (int i = 0; i < occurTime; i++) {
            switch (valueType) {
                // If more scalar types of Ion data are supported, this is the point to add more cases.
                case STRING:
                    writer.writeString(WriteRandomIonValues.constructString(DEFAULT_RANGE, 20));
                    break;
                case INT:
                    writer.writeInt(WriteRandomIonValues.constructInt());
                    break;
                default:
                    throw new IllegalStateException(valueType + " is not supported when generating Ion List based on Ion Schema.");
            }
        }
    }
}
