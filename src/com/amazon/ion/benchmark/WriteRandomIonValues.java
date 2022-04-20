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
import com.github.curiousoddman.rgxgen.RgxGen;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Generate specific scalar type of Ion data randomly, for some specific type, e.g. String, Decimal, Timestamp, users can put specifications on these types of Ion data.
 */
class WriteRandomIonValues {
    // The constant defined below are used as placeholder in the method WriteRandomIonValues.writeRequestedSizeFile.
    final static private IonSystem SYSTEM = IonSystemBuilder.standard().build();
    final static private List<Integer> DEFAULT_RANGE = WriteRandomIonValues.parseRange("[0, 1114111]");
    final static private Timestamp.Precision[] PRECISIONS = Timestamp.Precision.values();
    final static public IonStruct NO_CONSTRAINT_STRUCT = null;
    final static private int DEFAULT_PRECISION = 20;
    final static private int DEFAULT_SCALE_LOWER_BOUND = -20;
    final static private int DEFAULT_SCALE_UPPER_BOUND = 20;

    /**
     * Build up the writer based on the provided format (ion_text|ion_binary)
     * @param format the option to decide which writer to be constructed.
     * @param file the generated file which contains specified Ion data.
     * @return the writer which conforms with the required format.
     * @throws Exception if an error occurs while creating a file output stream.
     */
    public static IonWriter formatWriter(String format, File file) throws Exception {
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
    public static void printInfo(String path) {
        String fileName = path.substring(path.lastIndexOf("/") + 1);
        System.out.println(fileName + " generated successfully ! ");
        if (fileName.equals(path)) {
            System.out.println("Generated data is under the current directory");
        } else {
            System.out.println("File path: " + path);
        }
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
     * @param scale is the scale of the decimal second in the current template timestamp.
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
     * This method is not available now. Refactor required.
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
     * @param constraintStruct is an IonStruct which contains the top-level constraints in Ion Schema.
     * @throws IOException if an error occur when writing generated data.
     */
    public static void writeRequestedSizeFile(int size, IonWriter writer, File file, IonStruct constraintStruct) throws Exception {
        int currentSize = 0;
        int count = 0;
        // Determine how many values should be written before the writer.flush(), and this process aims to reduce the execution time of writer.flush().
        while (currentSize <= 0.05 * size) {
            WriteRandomIonValues.writeDataToFile(writer, constraintStruct);
            count += 1;
            writer.flush();
            currentSize = (int) file.length();
        }
        while (currentSize <= size) {
            for (int i = 0; i < count; i++) {
                WriteRandomIonValues.writeDataToFile(writer, constraintStruct);
            }
            writer.flush();
            currentSize = (int) file.length();
        }
    }

    /**
     * This method will be reused by different data generator
     * @param writer writer is IonWriter.
     * @param constraintStruct is an IonStruct which contains the top-level constraints in Ion Schema.
     * @throws IOException if an error occur during the data writing process.
     */
    private static void writeDataToFile(IonWriter writer, IonStruct constraintStruct) throws Exception {
        IonType type = IonType.valueOf(constraintStruct.get(IonSchemaUtilities.KEYWORD_TYPE).toString().toUpperCase());
        switch (type) {
            case FLOAT:
            case SYMBOL:
                //Not available.
                break;
            case INT:
                writer.writeInt(WriteRandomIonValues.constructInt());
                break;
            case STRING:
                writer.writeString(WriteRandomIonValues.constructString(constraintStruct));
                break;
            case DECIMAL:
                writer.writeDecimal(WriteRandomIonValues.constructDecimal(constraintStruct));
                break;
            case TIMESTAMP:
                writer.writeTimestamp(WriteRandomIonValues.constructTimestamp(constraintStruct));
                break;
            case STRUCT:
                WriteRandomIonValues.constructAndWriteIonStruct(constraintStruct, writer);
                break;
            case LIST:
                WriteRandomIonValues.constructAndWriteIonList(writer, constraintStruct);
                break;
            case BLOB:
                writer.writeBlob(WriteRandomIonValues.constructLobs(constraintStruct));
                break;
            case CLOB:
                writer.writeClob(WriteRandomIonValues.constructLobs(constraintStruct));
                break;
            default:
                throw new IllegalStateException(type + " is not supported.");
        }
    }

    /**
     * Construct string which is conformed with the constraints provided in ISL.
     * @param constraintStruct is an IonStruct which contains the top-level constraints in Ion Schema.
     * @return constructed string.
     * @throws Exception if error occurs when parsing the constraints.
     */
    public static String constructString(IonStruct constraintStruct) throws Exception {
        String constructedString;
        String regexPattern = IonSchemaUtilities.parseTextConstraints(constraintStruct, IonSchemaUtilities.KEYWORD_REGEX);
        Integer codePointsLengthBound = IonSchemaUtilities.parseConstraints(constraintStruct, IonSchemaUtilities.KEYWORD_CODE_POINT_LENGTH);
        if (regexPattern != null) {
            RgxGen rgxGen = new RgxGen(regexPattern);
            constructedString = rgxGen.generate();
        } else {
            Random random = new Random();
            // The condition of no codepoint_length constraint in ion schema.
            if (codePointsLengthBound == null) {
                // Preset the bound as average number 20;
                codePointsLengthBound = random.nextInt(20);
            }
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < codePointsLengthBound; j++) {
                int codePoint;
                int type;
                do {
                    codePoint = random.nextInt(DEFAULT_RANGE.get(1) - DEFAULT_RANGE.get(0) + 1) + DEFAULT_RANGE.get(0);
                    type = Character.getType(codePoint);
                } while (type == Character.PRIVATE_USE || type == Character.SURROGATE || type == Character.UNASSIGNED);
                sb.appendCodePoint(codePoint);
            }
            constructedString = sb.toString();
        }
        return constructedString;
    }

    /**
     * Construct the decimal which is conformed with the constraints provided in ISL.
     * @return the constructed decimal.
     */
    public static BigDecimal constructDecimal(IonStruct constraintStruct) throws Exception {
        Random random = new Random();
        // precision represents the minimum/maximum range indicating the number of digits in the unscaled value of a decimal. The minimum precision must be greater than or equal to 1.
        Integer precision = IonSchemaUtilities.parseConstraints(constraintStruct, IonSchemaUtilities.KEYWORD_PRECISION);
        if (precision == null) {
            precision = random.nextInt(DEFAULT_PRECISION);
        }
        // scale represents the minimum/maximum range indicating the number of digits to the right of the decimal point. The minimum scale must be greater than or equal to 0.
        Integer scale = IonSchemaUtilities.parseConstraints(constraintStruct, IonSchemaUtilities.KEYWORD_SCALE);
        if (scale == null) {
            scale = random.nextInt(DEFAULT_SCALE_UPPER_BOUND - DEFAULT_SCALE_LOWER_BOUND + 1) + DEFAULT_SCALE_LOWER_BOUND;
        }
        StringBuilder rs = new StringBuilder();
        rs.append(random.nextInt(9) + 1);
        for (int digit = 1; digit < precision; digit++) {
            rs.append(random.nextInt(10));
        }
        BigInteger unscaledValue = new BigInteger(rs.toString());
        BigDecimal bigDecimal = new BigDecimal(unscaledValue, scale);
        return bigDecimal;
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
     * Construct timestamp which is conformed with the constraints provided in ISL.
     * @param constraintStruct is an IonStruct which contains the top-level constraints in Ion Schema.
     * @return Constructed timestamp
     * @throws Exception if error occurs when parsing the constraints.
     */
    public static Timestamp constructTimestamp(IonStruct constraintStruct) throws Exception {
        Timestamp timestamp;
        Random random = new Random();
        int randomIndex = IonSchemaUtilities.parseConstraints(constraintStruct, IonSchemaUtilities.KEYWORD_TIMESTAMP_PRECISION);
        Timestamp.Precision precision = PRECISIONS[randomIndex];
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
                timestamp = Timestamp.forMinute(
                        random.nextInt(9998) + 1,
                        random.nextInt(12) + 1,
                        random.nextInt(28) + 1, // Use max 28 for simplicity. Not including up to 31 is not going to affect the measurement.
                        random.nextInt(24),
                        random.nextInt(60),
                        localOffset(random)
                );
                break;
            case SECOND:
                timestamp = Timestamp.forSecond(
                        random.nextInt(9998) + 1,
                        random.nextInt(12) + 1,
                        random.nextInt(28) + 1, // Use max 28 for simplicity. Not including up to 31 is not going to affect the measurement.
                        random.nextInt(24),
                        random.nextInt(60),
                        random.nextInt(60),
                        localOffset(random)
                );
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
     * Construct clob/blob which is conformed with the constraints provided in ISL.
     * @param constraintStruct is an IonStruct which contains the top-level constraints in Ion Schema.
     * @throws Exception if an error occurs when parsing constraints.
     */
    public static byte[] constructLobs(IonStruct constraintStruct) throws Exception {
        int byte_length;
        Random random = new Random();
        if (constraintStruct != null) {
            byte_length = IonSchemaUtilities.parseConstraints(constraintStruct, IonSchemaUtilities.KEYWORD_BYTE_LENGTH);
        } else {
            byte_length = random.nextInt(512);
        }
        byte[] randomBytes = new byte[byte_length];
        random.nextBytes(randomBytes);
        return randomBytes;
    }
    /**
     * Construct and write Ion structs based on the provided constraints.
     * @param constraintStruct is an IonStruct which contains the top-level constraints in Ion Schema.
     * @param writer writes Ion struct data.
     * @throws IOException if errors occur when writing data.
     */
    public static void constructAndWriteIonStruct(IonStruct constraintStruct, IonWriter writer) throws Exception {
        Random random = new Random();
        IonList annotations = IonSchemaUtilities.getAnnotation(constraintStruct);
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
                        writer.writeString(WriteRandomIonValues.constructString(value));
                        break;
                    case TIMESTAMP:
                        writer.writeTimestamp(WriteRandomIonValues.constructTimestamp(value));
                        break;
                    case LIST:
                        WriteRandomIonValues.constructAndWriteIonList(writer, value);
                        break;
                    default:
                        throw new IllegalStateException(type + " is not supported when generating Ion Struct based on Ion Schema.");
                }
            }
            writer.stepOut();
        }
    }

    /**
     * Construct Ion List based on the constraints provided by Ion Schema.
     * @param writer is Ion Writer.
     * @param constraintStruct is an IonStruct which contains the top-level constraints in Ion Schema.
     * @throws Exception if errors occur when reading or writing data.
     */
    public static void constructAndWriteIonList(IonWriter writer, IonStruct constraintStruct) throws Exception {
        // When there's only one required element in Ion List and the length of generated Ion List is not specified, we set the default length as a integer smaller than 20.
        Integer containerLength = IonSchemaUtilities.parseConstraints(constraintStruct, IonSchemaUtilities.KEYWORD_CONTAINER_LENGTH);
        IonList annotations = IonSchemaUtilities.getAnnotation(constraintStruct);
        int occurrences;
        try (IonReader reader = IonReaderBuilder.standard().build(constraintStruct)) {
            reader.next();
            reader.stepIn();
            while (reader.next() != null) {
                if (annotations != null) {
                    for (int i = 0; i < annotations.size(); i++) {
                        writer.addTypeAnnotation(annotations.get(i).toString());
                    }
                }
                writer.stepIn(IonType.LIST);
                // If constraint name is 'element', only one type of Ion Data is specified.
                if (constraintStruct.get(IonSchemaUtilities.KEYWORD_ELEMENT) != null && containerLength != null) {
                    IonType type = IonType.valueOf(constraintStruct.get(IonSchemaUtilities.KEYWORD_ELEMENT).toString().toUpperCase());
                    for (int i = 0; i < containerLength; i++) {
                        occurrences = 1;
                        WriteRandomIonValues.constructScalarTypeData(type, writer, occurrences, constraintStruct);
                    }
                    break;
                } else if (constraintStruct.get(IonSchemaUtilities.KEYWORD_ORDERED_ELEMENTS) != null) {
                    IonList orderedElement = (IonList) constraintStruct.get(IonSchemaUtilities.KEYWORD_ORDERED_ELEMENTS);
                    for (int index = 0; index < orderedElement.size(); index++) {
                        IonType elementType = orderedElement.get(index).getType();
                        IonType valueType;
                        switch (elementType) {
                            case SYMBOL:
                                occurrences = 1;
                                valueType = IonType.valueOf(orderedElement.get(index).toString().toUpperCase());
                                WriteRandomIonValues.constructScalarTypeData(valueType, writer, occurrences, NO_CONSTRAINT_STRUCT);
                                break;
                            case STRUCT:
                                IonStruct constraintsStruct = (IonStruct) orderedElement.get(index);
                                occurrences = IonSchemaUtilities.parseConstraints(constraintsStruct, IonSchemaUtilities.KEYWORD_OCCURS);
                                if(occurrences == 0) {
                                    break;
                                }
                                valueType = IonType.valueOf(constraintsStruct.get(IonSchemaUtilities.KEYWORD_TYPE).toString().toUpperCase());
                                WriteRandomIonValues.constructScalarTypeData(valueType, writer, occurrences, constraintsStruct);
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
    public static void constructScalarTypeData(IonType valueType, IonWriter writer, int occurTime, IonStruct constraintStruct) throws Exception {
        for (int i = 0; i < occurTime; i++) {
            switch (valueType) {
                // If more scalar types of Ion data are supported, this is the point to add more cases.
                case STRING:
                    writer.writeString(WriteRandomIonValues.constructString(constraintStruct));
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
