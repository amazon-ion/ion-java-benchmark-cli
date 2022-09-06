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
import com.amazon.ion.IonSequence;
import com.amazon.ion.IonSexp;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonTimestamp;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.Timestamp;
import com.amazon.ion.benchmark.schema.ReparsedType;
import com.amazon.ion.benchmark.schema.constraints.Annotations;
import com.amazon.ion.benchmark.schema.constraints.Contains;
import com.amazon.ion.benchmark.schema.constraints.Element;
import com.amazon.ion.benchmark.schema.constraints.Fields;
import com.amazon.ion.benchmark.schema.constraints.OrderedElements;
import com.amazon.ion.benchmark.schema.constraints.QuantifiableConstraints;
import com.amazon.ion.benchmark.schema.constraints.Range;
import com.amazon.ion.benchmark.schema.constraints.Regex;
import com.amazon.ion.benchmark.schema.constraints.ReparsedConstraint;
import com.amazon.ion.benchmark.schema.constraints.TimestampPrecision;
import com.amazon.ion.benchmark.schema.constraints.TypeName;
import com.amazon.ion.benchmark.schema.constraints.ValidValues;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.IonSystemBuilder;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generate specific scalar type of Ion data randomly, for some specific type, e.g. String, Decimal, Timestamp, users can put specifications on these types of Ion data.
 */
class DataConstructor {
    // The constant defined below are used as placeholder in the method WriteRandomIonValues.writeRequestedSizeFile.
    final static private IonSystem SYSTEM = IonSystemBuilder.standard().build();
    final static private List<Integer> DEFAULT_RANGE = Arrays.asList(0, 0x10FFFF);
    final static public IonStruct NO_CONSTRAINT_STRUCT = null;
    final static private int DEFAULT_PRECISION = 20;
    // The ASCII_CODE_LOWERCASE_A represents the ASCII code of character "a".
    final static private int ASCII_CODE_LOWERCASE_A = 97;
    // The ASCII_CODE_UPPERCASE_A represents the ASCII code of character "A".
    final static private int ASCII_CODE_UPPERCASE_A = 65;
    final static private int DEFAULT_SCALE_LOWER_BOUND = -20;
    final static private int DEFAULT_SCALE_UPPER_BOUND = 20;
    final static private int DEFAULT_CONTAINER_LENGTH = 20;
    final static private Set<String> VALID_STRING_SYMBOL_CONSTRAINTS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList("regex", "codepoint_length")));
    final static private Set<String> VALID_DECIMAL_CONSTRAINTS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList("precision", "scale")));
    final static private Set<String> VALID_TIMESTAMP_CONSTRAINTS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList("timestamp_offset", "timestamp_precision")));
    final static private Set<String> VALID_SEQUENCE_CONSTRAINTS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList("contains", "element", "ordered_elements")));
    final static private Set<String> VALID_STRUCT_CONSTRAINTS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList("element", "fields")));
    // Create a range which contains the default lower bound and upper bound values.
    final static private Range DEFAULT_TIMESTAMP_IN_MILLIS_DECIMAL_RANGE = new Range(SYSTEM.newList( SYSTEM.newDecimal(62135769600000L), SYSTEM.newDecimal(253402300800000L)));

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
     * Constructing data which is conformed with provided type definition.
     * @param parsedTypeDefinition is parsed from ion schema file as IonStruct format, it contains the top-level constraints.
     * @return constructed ion data.
     */
    public static IonValue constructIonData(ReparsedType parsedTypeDefinition) {
        IonValue result;
        // The first step is to check whether parsedTypeDefinition contains 'valid_values'. The reason we prioritize checking
        // 'valid_values' is that the constraint 'type' might not be contained in the type definition, in that case we cannot trigger
        // the following data constructing process.
        // Assume if 'valid_values' provided in ISL file, constraint 'type' is optional, else constraint 'type' is required.
        Map<String, ReparsedConstraint> constraintMap = parsedTypeDefinition.getConstraintMap();
        Map<String, ReparsedConstraint> constraintMapClone = new HashMap<>();
        constraintMapClone.putAll(constraintMap);
        ValidValues validValues = (ValidValues) constraintMap.get("valid_values");
        Annotations annotations = (Annotations)constraintMapClone.remove("annotations");
        TypeName type = (TypeName)constraintMapClone.remove("type");
        if (validValues != null && !validValues.isRange()) {
            result = getRandomValueFromList(validValues.getValidValues());
        } else if (type == null) {
            throw new IllegalStateException("Constraint 'type' is required.");
        } else {
            switch (type.getTypeName()) {
                case FLOAT:
                    result = SYSTEM.newFloat(constructFloat(constraintMapClone));
                    break;
                case SYMBOL:
                    result = SYSTEM.newSymbol(constructString(constraintMapClone));
                    break;
                case INT:
                    result = SYSTEM.newInt(constructInt(constraintMapClone));
                    break;
                case STRING:
                    result = SYSTEM.newString(constructString(constraintMapClone));
                    break;
                case DECIMAL:
                    result = SYSTEM.newDecimal(constructDecimal(constraintMapClone));
                    break;
                case TIMESTAMP:
                    result = SYSTEM.newTimestamp(constructTimestamp(constraintMapClone));
                    break;
                case BLOB:
                    result = SYSTEM.newBlob(constructLobs(constraintMapClone));
                    break;
                case CLOB:
                    result = SYSTEM.newClob(constructLobs(constraintMapClone));
                    break;
                case STRUCT:
                    result = constructIonStruct(constraintMapClone);
                    break;
                case LIST:
                    IonList listContainer = SYSTEM.newEmptyList();
                    result = constructSequenceTypeData(constraintMapClone, listContainer);
                    break;
                case SEXP:
                    IonSexp sexpContainer = SYSTEM.newEmptySexp();
                    result = constructSequenceTypeData(constraintMapClone, sexpContainer);
                    break;
                default:
                    throw new IllegalStateException(type + " is not supported.");
            }
        }
        if (annotations != null && annotations.getAnnotations() != null) {
            IonList annotationsList = annotations.getAnnotations();
            for (int i = 0; i < annotationsList.size(); i++) {
                result.addTypeAnnotation(annotationsList.get(i).toString());
            }
        }
        return result;
    }

    /**
     * Constructing IonStruct which is aligned with the constraints provided in the constraintMap.
     * @param constraintMapClone collects the constraints from ISL file, the key represents the name of constraints,
     * and the value is constraint value in ReparsedConstraint format.
     * @return the constructed IonStruct value.
     */
    private static IonStruct constructIonStruct(Map<String, ReparsedConstraint> constraintMapClone) {
        Fields fields = (Fields)constraintMapClone.remove("fields");
        Element element = (Element)constraintMapClone.remove("element");
        QuantifiableConstraints container_length = (QuantifiableConstraints)constraintMapClone.remove("container_length");
        Random random = new Random();
        IonStruct constructedIonStruct = SYSTEM.newEmptyStruct();
        // Check if there is unhandled constraint provided.
        if (!constraintMapClone.isEmpty()) {
            throw new IllegalStateException ("Found unhandled constraints : " + constraintMapClone.values());
        }
        if (element != null && fields != null) {
            throw new IllegalStateException("Can only handle one of : " + VALID_STRUCT_CONSTRAINTS);
        } else if (element != null) {
            int length = container_length == null ? DEFAULT_CONTAINER_LENGTH : container_length.getRange().getRandomQuantifiableValueFromRange().intValue();
            for (int i = 0; i < length; i++) {
                constructedIonStruct.add(constructStringFromCodepointLength(random.nextInt(20)), constructIonData(element.getElement()));
            }
        } else {
            Map<String, ReparsedType> fieldMap = fields.getFieldMap();
            // Writing field value to IonStruct based on the relevant constraint.
            for (Map.Entry<String, ReparsedType> entry : fieldMap.entrySet()) {
                // Get the type definition for each field.
                ReparsedType fieldTypeDefinition = entry.getValue();
                // 'occurs' included in the field constraint determines the occurrences of the specified field.
                int occurTime = ReparsedType.getOccurs(fieldTypeDefinition.getConstraintStruct());
                for (int i = 0; i < occurTime; i++) {
                    constructedIonStruct.add(entry.getKey(), constructIonData(fieldTypeDefinition));
                }
            }
        }
        return constructedIonStruct;
    }

    /**
     * Constructing IonSequence value which is aligned with the constraints provided in the constraintMap.
     * @param constraintMapClone collects the constraints from ISL file, the key represents the name of constraints,
     * and the value is constraint value in ReparsedConstraint format.
     * @param container represents one of the element from set (IonList | IonSexp).
     * @return the constructed IonSequence value.
     */
    private static IonSequence constructSequenceTypeData(Map<String, ReparsedConstraint> constraintMapClone, IonSequence container) {
        Contains contains = (Contains)constraintMapClone.remove("contains");
        OrderedElements elementsConstraints = (OrderedElements)constraintMapClone.remove("ordered_elements");
        Element element = (Element)constraintMapClone.remove("element");
        QuantifiableConstraints container_length = (QuantifiableConstraints)constraintMapClone.remove("container_length");
        if (!constraintMapClone.isEmpty()) {
            throw new IllegalStateException ("Found unhandled constraints : " + constraintMapClone.values());
        }
        // TODO: Consider to construct composable data-generator for each constraint to avoid the multiple conditions in one 'if/else' statement.
        if ((contains != null && elementsConstraints != null) || (elementsConstraints != null && element != null) || (contains != null && element != null)) {
            throw new IllegalStateException("Can only handle one of : " + VALID_SEQUENCE_CONSTRAINTS);
        } else if (element != null) {
            int length = container_length == null ? DEFAULT_CONTAINER_LENGTH : container_length.getRange().getRandomQuantifiableValueFromRange().intValue();
            for (int i = 0 ; i < length; i++) {
                container.add(constructIonData(element.getElement()));
            }
            return container;
        } else if (contains != null) {
            // TODO: The return IonList should also include other random values except the values provided by 'contains'.
            return contains.getExpectedContainedValues();
        } else {
            ArrayList<ReparsedType> orderedElementsConstraints = elementsConstraints.getOrderedElementsConstraints();
            for (ReparsedType constraint : orderedElementsConstraints) {
                // 'occurs' included in the constraint of 'ordered_element' indicates the occurrences of the specified element.
                int occurTime = ReparsedType.getOccurs(constraint.getConstraintStruct());
                for (int i = 0; i < occurTime; i++) {
                    container.add(constructIonData(constraint));
                }
            }
            return container;
        }
    }

    /**
     * Get a random IonValue from IonList.
     * @param values represents IonList.
     * @return the randomly chosen IonValue.
     */
    public static IonValue getRandomValueFromList(IonList values) {
        Random random = new Random();
        int randomIndex = random.nextInt(values.size());
        return values.get(randomIndex);
    }

    /**
     * Construct string which is conformed with the constraints provided in ISL.
     * @param constraintMapClone collects the constraints from ISL file, the key represents the name of constraints,
     * and the value is constraint value in ReparsedConstraint format.
     * @return constructed string.
     */
    public static String constructString(Map<String, ReparsedConstraint> constraintMapClone) {
        Random random = new Random();
        Regex regex = (Regex) constraintMapClone.remove("regex");
        QuantifiableConstraints codepoint_length = (QuantifiableConstraints) constraintMapClone.remove("codepoint_length");
        if (!constraintMapClone.isEmpty()) {
            throw new IllegalStateException ("Found unhandled constraints : " + constraintMapClone.values());
        }
        if (regex != null && codepoint_length != null) {
            throw new IllegalStateException ("Can only handle one of : " + VALID_STRING_SYMBOL_CONSTRAINTS);
        } else if (regex != null) {
            String pattern = regex.getPattern();
            RgxGen rgxGen = new RgxGen(pattern);
            return rgxGen.generate();
        } else if (codepoint_length != null) {
            int length = codepoint_length.getRange().getRandomQuantifiableValueFromRange().intValue();
            return constructStringFromCodepointLength(length);
        } else {
            // If there is no constraints provided, a randomly constructed string with
            // preset Unicode codepoints length will be generated.
            return constructStringFromCodepointLength(random.nextInt(20));
        }
    }

    /**
     * Generate unicode codepoint randomly which matches the character from [A-Z] and [a-z].
     * @return generated codepoint.
     */
    private static int getCodePoint() {
        int index = ThreadLocalRandom.current().nextInt(20);
        int randomIndex = ThreadLocalRandom.current().nextInt(26);
        if (index < 10) {
            // Randomly generate the unicode of character from [A-Z].
            return randomIndex + ASCII_CODE_UPPERCASE_A;
        } else {
            // Randomly generate the unicode of character from [a-z].
            return randomIndex + ASCII_CODE_LOWERCASE_A;
        }
    }

    /**
     * Construct string which is conformed with the provided codepoint_length.
     * @param codePointsLengthBound represents the exact number of Unicode codepoints in a string or symbol.
     * @return the constructed string.
     */
    private static String constructStringFromCodepointLength(int codePointsLengthBound) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < codePointsLengthBound; i++) {
            int codePoint = getCodePoint();
            sb.appendCodePoint(codePoint);
        }
        String constructedString = sb.toString();
        return constructedString;
    }

    /**
     * Construct the float which is conformed with the constraints provided in ISL.
     * @param constraintMapClone collects the constraints from ISL file, the key represents the name of constraints,
     * and the value is constraint value in ReparsedConstraint format.
     * @return the constructed double value.
     */
    public static Double constructFloat(Map<String, ReparsedConstraint> constraintMapClone) {
        // In the process of generating IonFloat, there is no type-specified constraints. For this step we
        // only consider the general constraint 'valid_values'.
        ValidValues validValues = (ValidValues) constraintMapClone.remove("valid_values");
        if (!constraintMapClone.isEmpty()) {
            throw new IllegalStateException ("Found unhandled constraints : " + constraintMapClone.values());
        }
        if (validValues != null) {
            return validValues.getRange().getRandomQuantifiableValueFromRange().doubleValue();
        } else {
            return ThreadLocalRandom.current().nextDouble();
        }
    }

    /**
     * Construct the decimal which is conformed with the constraints provided in ISL.
     * @param constraintMapClone collects the constraints from ISL file, the key represents the name of constraints,
     * and the value is constraint value in ReparsedConstraint format.
     * @return the constructed decimal.
     */
    public static BigDecimal constructDecimal(Map<String, ReparsedConstraint> constraintMapClone) {
        Random random = new Random();
        // If there is no constraints provided, assign scale and precision with default values.
        int scaleValue = random.nextInt(DEFAULT_SCALE_UPPER_BOUND - DEFAULT_SCALE_LOWER_BOUND + 1) + DEFAULT_SCALE_LOWER_BOUND;
        int precisionValue = random.nextInt(DEFAULT_PRECISION);
        QuantifiableConstraints scale = (QuantifiableConstraints) constraintMapClone.remove("scale");
        QuantifiableConstraints precision = (QuantifiableConstraints) constraintMapClone.remove("precision");
        ValidValues validValues = (ValidValues) constraintMapClone.remove("valid_values");
        StringBuilder rs = new StringBuilder();
        rs.append(random.nextInt(9) + 1);
        if (!constraintMapClone.isEmpty()) {
            throw new IllegalStateException ("Found unhandled constraints : " + constraintMapClone.values());
        }
        if (validValues == null) {
            if (scale != null) {
                scaleValue = scale.getRange().getRandomQuantifiableValueFromRange().intValue();
            }
            if (precision != null) {
                precisionValue = precision.getRange().getRandomQuantifiableValueFromRange().intValue();
            }
            for (int digit = 1; digit < precisionValue; digit++) {
                rs.append(random.nextInt(10));
            }
            BigInteger unscaledValue = new BigInteger(rs.toString());
            return new BigDecimal(unscaledValue, scaleValue);
        } else {
            if (scale != null || precision != null) {
                throw new IllegalStateException("Cannot handle 'valid_values' and constraint from " + VALID_DECIMAL_CONSTRAINTS + "at the same time.");
            } else {
                return validValues.getRange().getRandomQuantifiableValueFromRange();
            }
        }
    }

    /**
     * Generate random integers which is conformed with the constraints provided in ISL.
     * @param constraintMapClone collects the constraints from ISL file, the key represents the name of constraints,
     * and the value is in ReparsedConstraint format.
     * @return the constructed int.
     */
    public static long constructInt(Map<String, ReparsedConstraint> constraintMapClone) {
        // In the process of generating IonInt, there is no type-specified constraints. For this step we
        // only consider the general constraints 'valid_values'.
        ValidValues validValues = (ValidValues) constraintMapClone.remove("valid_values");
        if (!constraintMapClone.isEmpty()) {
            throw new IllegalStateException ("Found unhandled constraints : " + constraintMapClone.values());
        }
        if (validValues != null) {
            // The generated data is conformed with the provided 'valid_values' range.
            return validValues.getRange().getRandomQuantifiableValueFromRange().longValue();
        } else {
            // If there is no constraint provided, the generator will construct a random value.
            // Randomly generate integers in the distribution that more than 80% of integers would be smaller than 1024.
            // In this case, the generated integers would be more similar to the real world data.
            Random random = new Random();
            int index = random.nextInt(20);
            if (index < 16) {
                return ThreadLocalRandom.current().nextInt(1024);
            } else {
                return ThreadLocalRandom.current().nextLong();
            }
        }
    }

    /**
     * Construct timestamp which is conformed with the constraints provided in ISL.
     * @param constraintMapClone collects the constraints from ISL file, the key represents the name of constraints,
     * and the value is in ReparsedConstraint format.
     * @return the constructed timestamp.
     */
    public static Timestamp constructTimestamp(Map<String, ReparsedConstraint> constraintMapClone) {
        Random random = new Random();
        Range range = DEFAULT_TIMESTAMP_IN_MILLIS_DECIMAL_RANGE;
        // Preset the local offset.
        Integer localOffset = localOffset(random);
        // Preset the default precision as 'Day'.
        Timestamp.Precision precision = Timestamp.Precision.DAY;
        TimestampPrecision timestampPrecision = (TimestampPrecision) constraintMapClone.remove("timestamp_precision");
        ValidValues validValues = (ValidValues) constraintMapClone.remove("valid_values");
        if (!constraintMapClone.isEmpty()) {
            throw new IllegalStateException ("Found unhandled constraints : " + constraintMapClone.values());
        }
        if (validValues == null) {
            if (timestampPrecision != null) {
                precision = TimestampPrecision.getRandomTimestampPrecision(timestampPrecision.getRange());
            }
        } else {
            if (timestampPrecision != null) {
                throw new IllegalStateException("Cannot handle 'valid_values' and constraint from " + VALID_TIMESTAMP_CONSTRAINTS + "at the same time.");
            } else {
                range = validValues.getRange();
                IonTimestamp upperBound = range.upperBound(IonTimestamp.class);
                localOffset = upperBound.getLocalOffset();
                precision = upperBound.timestampValue().getPrecision();
            }
        }
        // Generate a random millisecond within the provided range.
        BigDecimal randomMillis = range.getRandomQuantifiableValueFromRange();
        // Generate timestamp based on the provided millisecond value and precision.
        Timestamp regeneratedTimestamp = Timestamp.forMillis(randomMillis, localOffset);

        int year = regeneratedTimestamp.getYear();
        int month = regeneratedTimestamp.getMonth();
        int day = regeneratedTimestamp.getDay();
        int minute = regeneratedTimestamp.getMinute();
        int hour = regeneratedTimestamp.getHour();
        int seconds = regeneratedTimestamp.getSecond();
        BigDecimal fracSecond = regeneratedTimestamp.getDecimalSecond().subtract(BigDecimal.valueOf(seconds));
        return Timestamp.createFromUtcFields(precision, year, month, day, hour, minute, seconds, fracSecond, localOffset);
    }

    /**
     * Construct clob/blob which is conformed with the constraints provided in ISL.
     * @param constraintMapClone collects the constraints from ISL file, the key represents the name of constraints,
     * and the value is in ReparsedConstraint format.
     * @return the constructed bytes.
     */
    public static byte[] constructLobs( Map<String, ReparsedConstraint> constraintMapClone) {
        int byte_length;
        Random random = new Random();
        QuantifiableConstraints byteLength = (QuantifiableConstraints) constraintMapClone.remove("byte_length");
        if (!constraintMapClone.isEmpty()) {
            throw new IllegalStateException ("Found unhandled constraints : " + constraintMapClone.values());
        }
        if (byteLength != null) {
            byte_length = byteLength.getRange().getRandomQuantifiableValueFromRange().intValue();
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
//    public static void constructAndWriteIonStruct(IonStruct constraintStruct, IonWriter writer) throws Exception {
//        Random random = new Random();
//        IonList annotations = IonSchemaUtilities.getAnnotation(constraintStruct);
//        IonStruct fields = (IonStruct) constraintStruct.get(IonSchemaUtilities.KEYWORD_FIELDS);
//        try (IonReader reader = IonReaderBuilder.standard().build(fields)) {
//            reader.next();
//            reader.stepIn();
//            for (int i = 0; i < annotations.size(); i++) {
//                writer.addTypeAnnotation(annotations.get(i).toString());
//            }
//            writer.stepIn(IonType.STRUCT);
//            while (reader.next() != null) {
//                String fieldName = reader.getFieldName();
//                IonValue struct = SYSTEM.newValue(reader);
//                IonStruct value = (IonStruct) struct;
//                // If the value of "occurs" is optional, the integer represents this value is 1 or 0.
//                int occurTime = IonSchemaUtilities.parseConstraints(value, IonSchemaUtilities.KEYWORD_OCCURS);
//                if (occurTime == 0) {
//                    continue;
//                }
//                writer.setFieldName(fieldName);
//                IonType type = IonType.valueOf(value.get(IonSchemaUtilities.KEYWORD_TYPE).toString().toUpperCase());
//                switch (type) {
//                    // If more types of Ion data are available, the logic should be added below.
//                    case STRING:
//                        writer.writeString(WriteRandomIonValues.constructString(value));
//                        break;
//                    case TIMESTAMP:
//                        writer.writeTimestamp(WriteRandomIonValues.constructTimestamp(value));
//                        break;
//                    case LIST:
//                        WriteRandomIonValues.constructAndWriteIonList(writer, value);
//                        break;
//                    default:
//                        throw new IllegalStateException(type + " is not supported when generating Ion Struct based on Ion Schema.");
//                }
//            }
//            writer.stepOut();
//        }
//    }

    /**
     * Construct Ion List based on the constraints provided by Ion Schema.
     * @param writer is Ion Writer.
     * @param constraintStruct is an IonStruct which contains the top-level constraints in Ion Schema.
     * @throws Exception if errors occur when reading or writing data.
     */
//    public static void constructAndWriteIonList(IonWriter writer, IonStruct constraintStruct) throws Exception {
//        // When there's only one required element in Ion List and the length of generated Ion List is not specified, we set the default length as a integer smaller than 20.
//        Integer containerLength = IonSchemaUtilities.parseConstraints(constraintStruct, IonSchemaUtilities.KEYWORD_CONTAINER_LENGTH);
//        IonList annotations = IonSchemaUtilities.getAnnotation(constraintStruct);
//        int occurrences;
//        try (IonReader reader = IonReaderBuilder.standard().build(constraintStruct)) {
//            reader.next();
//            reader.stepIn();
//            while (reader.next() != null) {
//                if (annotations != null) {
//                    for (int i = 0; i < annotations.size(); i++) {
//                        writer.addTypeAnnotation(annotations.get(i).toString());
//                    }
//                }
//                writer.stepIn(IonType.LIST);
//                // If constraint name is 'element', only one type of Ion Data is specified.
//                if (constraintStruct.get(IonSchemaUtilities.KEYWORD_ELEMENT) != null && containerLength != null) {
//                    IonType type = IonType.valueOf(constraintStruct.get(IonSchemaUtilities.KEYWORD_ELEMENT).toString().toUpperCase());
//                    for (int i = 0; i < containerLength; i++) {
//                        occurrences = 1;
//                        WriteRandomIonValues.constructScalarTypeData(type, writer, occurrences, constraintStruct);
//                    }
//                    break;
//                } else if (constraintStruct.get(IonSchemaUtilities.KEYWORD_ORDERED_ELEMENTS) != null) {
//                    IonList orderedElement = (IonList) constraintStruct.get(IonSchemaUtilities.KEYWORD_ORDERED_ELEMENTS);
//                    for (int index = 0; index < orderedElement.size(); index++) {
//                        IonType elementType = orderedElement.get(index).getType();
//                        IonType valueType;
//                        switch (elementType) {
//                            case SYMBOL:
//                                occurrences = 1;
//                                valueType = IonType.valueOf(orderedElement.get(index).toString().toUpperCase());
//                                WriteRandomIonValues.constructScalarTypeData(valueType, writer, occurrences, NO_CONSTRAINT_STRUCT);
//                                break;
//                            case STRUCT:
//                                IonStruct constraintsStruct = (IonStruct) orderedElement.get(index);
//                                occurrences = IonSchemaUtilities.parseConstraints(constraintsStruct, IonSchemaUtilities.KEYWORD_OCCURS);
//                                if(occurrences == 0) {
//                                    break;
//                                }
//                                valueType = IonType.valueOf(constraintsStruct.get(IonSchemaUtilities.KEYWORD_TYPE).toString().toUpperCase());
//                                WriteRandomIonValues.constructScalarTypeData(valueType, writer, occurrences, constraintsStruct);
//                                break;
//                        }
//                    }
//                    writer.stepOut();
//                    return;
//                }
//            }
//            writer.stepOut();
//        }
//    }

    /**
     * Construct scalar type Ion data based on the occurrence time. This method is mainly reused during the process of generating Ion List which will specify the occurrence time.
     * @param valueType is IonType of the data needed to be written in Ion List.
     * @param writer is IonWriter.
     * @param occurTime is the occurrence time of the element in Ion List.
     * @throws IOException if errors occur when writing data.
     */
//    public static void constructScalarTypeData(IonType valueType, IonWriter writer, int occurTime, IonStruct constraintStruct) throws Exception {
//        for (int i = 0; i < occurTime; i++) {
//            switch (valueType) {
//                // If more scalar types of Ion data are supported, this is the point to add more cases.
//                case STRING:
//                    writer.writeString(WriteRandomIonValues.constructString(constraintStruct));
//                    break;
//                case INT:
//                    writer.writeInt(WriteRandomIonValues.constructInt(constraintStruct));
//                    break;
//                default:
//                    throw new IllegalStateException(valueType + " is not supported when generating Ion List based on Ion Schema.");
//            }
//        }
//    }
}
