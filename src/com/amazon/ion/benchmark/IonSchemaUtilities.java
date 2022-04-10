package com.amazon.ion.benchmark;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonList;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.Timestamp;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ionschema.InvalidSchemaException;
import com.amazon.ionschema.IonSchemaSystem;
import com.amazon.ionschema.IonSchemaSystemBuilder;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * Contain the methods which process the constraints provided by the Ion Schema file and define the constants relevant to the Ion Schema file.
 */
public class IonSchemaUtilities {
    public static final String KEYWORD_ANNOTATIONS = "annotations";
    public static final String KEYWORD_REQUIRED = "required";
    public static final String KEYWORD_OPTIONAL = "optional";
    public static final String KEYWORD_TIMESTAMP_PRECISION = "timestamp_precision";
    public static final String KEYWORD_TYPE = "type";
    public static final String KEYWORD_FIELDS = "fields";
    public static final String KEYWORD_CODE_POINT_LENGTH = "codepoint_length";
    public static final String KEYWORD_OCCURS = "occurs";
    public static final String KEYWORD_ELEMENT = "element";
    public static final String KEYWORD_ORDERED_ELEMENTS = "ordered_elements";
    public static final String KEYWORD_ORDERED = "ordered";
    public static final String KEYWORD_NAME = "name";
    public static final String KEYWORD_CONTAINER_LENGTH = "container_length";
    public static final String KEYWORD_BYTE_LENGTH = "byte_length";
    public static final String KEYWORD_MIN = "min";
    public static final String KEYWORD_MAX = "max";
    public static final String KEYWORD_SCALE = "scale";
    public static final String KEYWORD_PRECISION = "precision";

    /**
     * Check the validation of input ion schema file and will throw InvalidSchemaException message when an invalid schema definition is encountered.
     * @param inputFile represents the file path of the ion schema file.
     * @throws Exception if an error occur when creating FileInputStream.
     */
    public static void checkValidationOfSchema(String inputFile) throws Exception {
        IonSchemaSystem ISS = IonSchemaSystemBuilder.standard().build();
        try (IonReader readerInput = IonReaderBuilder.standard().build(new BufferedInputStream(new FileInputStream(inputFile)))) {
            IonDatagram schema = ReadGeneralConstraints.LOADER.load(readerInput);
            ISS.newSchema(schema.iterator());
        } catch (InvalidSchemaException e) {
            System.out.println(e.getMessage());
        } throw new Exception("The provided ion schema file is not valid");
    }

    /**
     * Extract the value of the constraints, select from the set (occurs | container_length | codepoint_length | timestamp_precision | precision | scale | byte_length).
     * @param value is the Ion struct which contain the current constraint field
     * @param keyWord is the field name of the constraint
     * @return the value of the current constraint.
     * @throws IOException if an error occur when constructing the IonReader.
     */
    public static int parseConstraints(IonStruct value, String keyWord) throws IOException {
        Random random = new Random();
        int result = 0;
        int min;
        int max;
        try (IonReader reader = IonReaderBuilder.standard().build(value)) {
            reader.next();
            reader.stepIn();
            while (reader.next() != null) {
                if (reader.getFieldName().equals(keyWord)) {
                    IonType type = reader.getType();
                    switch (type) {
                        case INT:
                            result = reader.intValue();
                            break;
                        case SYMBOL:
                            if (reader.stringValue().equals(KEYWORD_REQUIRED)) {
                                result = 1;
                            } else if (reader.stringValue().equals(KEYWORD_OPTIONAL)) {
                                result = random.nextInt(2);
                            } else {
                                throw new IllegalArgumentException ("The value of this option is not supported");
                            }
                            break;
                        case LIST:
                            reader.stepIn();
                            reader.next();
                            if (reader.getType() == IonType.SYMBOL) {
                                if (reader.symbolValue().equals(KEYWORD_MIN)) {
                                    min = 0;
                                } else if (keyWord.equals(IonSchemaUtilities.KEYWORD_TIMESTAMP_PRECISION)) {
                                    min = Timestamp.Precision.valueOf(reader.stringValue().toUpperCase()).ordinal();
                                } else {
                                    throw new IllegalStateException("The lower bound symbol value is not supported in Ion Schema");
                                }
                            } else {
                                min = reader.intValue();
                            }
                            reader.next();
                            if (reader.getType() == IonType.SYMBOL) {
                                if (reader.symbolValue().equals(KEYWORD_MAX)) {
                                    max = Integer.MAX_VALUE;
                                } else if (keyWord.equals(IonSchemaUtilities.KEYWORD_TIMESTAMP_PRECISION)) {
                                    max = Timestamp.Precision.valueOf(reader.stringValue().toUpperCase()).ordinal();
                                } else {
                                    throw new IllegalStateException("The upper bound symbol value is not supported in Ion Schema");
                                }
                            } else {
                                max = reader.intValue();
                            }
                            result = random.nextInt(max - min + 1) + min;
                            break;
                    }
                }
            }
            return result;
        }
    }

    /**
     * Parse the precision of the timestamp.
     * @param value is the Ion struct which contain the current constraint field.
     * @throws IOException if errors occur when reading the data.
     * @return requested timestamp precision
     */
    public static Timestamp.Precision getTimestampPrecisionTemplate(IonStruct value) throws IOException {
        Timestamp.Precision precision = null;
        try (IonReader reader = IonReaderBuilder.standard().build(value)) {
            reader.next();
            reader.stepIn();
            while (reader.next() != null) {
                if (reader.getFieldName().equals(KEYWORD_TIMESTAMP_PRECISION)) {
                    IonType type = reader.getType();
                    switch (type) {
                        case SYMBOL:
                            precision = Timestamp.Precision.valueOf(reader.stringValue().toUpperCase());
                            break;
                    }
                }
            }
        }
        return precision;
    }

    /**
     * Parse the field 'annotations' based on the provided constraints (required|ordered).
     * @param constraintStruct contains the top-level constraints of Ion Schema.
     * @return IonList which contains annotations
     * @throws IOException if error occur when reading constraints.
     */
    public static IonList getAnnotation(IonStruct constraintStruct) throws IOException {
        IonList annotationList = null;
        Random random = new Random();
        try (IonReader annotationInfo = IonReaderBuilder.standard().build(constraintStruct)){
            annotationInfo.next();
            annotationInfo.stepIn();
            while (annotationInfo.next() != null) {
                if (annotationInfo.getFieldName().equals(IonSchemaUtilities.KEYWORD_ANNOTATIONS)) {
                    IonValue annotationValue = ReadGeneralConstraints.SYSTEM.newValue(annotationInfo);
                    List<String> constraint = Arrays.asList(annotationValue.getTypeAnnotations());
                    IonList annotations = (IonList) annotationValue;
                    annotationList = checkOrdered(constraint, annotations, random);
                }
            }
        }
        return annotationList;
    }

    /**
     * This is a helper method of getAnnotation which processes the constraint 'Ordered'.
     * @param constraint is a List which contains all annotations of 'annotations' field.
     * @param annotationList is the original annotation List without any consideration about constraints.
     * @param random is a random integer generator.
     * @return a List of processed annotations.
     * @throws IOException if error occur when reading constraints.
     */
    private static IonList checkOrdered(List<String> constraint, IonList annotationList, Random random) throws IOException {
        IonList result = annotationList;
        if (!constraint.contains(IonSchemaUtilities.KEYWORD_ORDERED)) {
            List<IonValue> annotations = annotationList.stream().collect(Collectors.toList());
            Collections.shuffle(annotations);
            result = ReadGeneralConstraints.SYSTEM.newEmptyList();
            for (int index = 0; index < annotations.size(); index++) {
                result.add(index, annotations.get(index).clone());
            }
        }
        return checkRequired(constraint, result,random);
    }

    /**
     * This is a helper method of getAnnotation which processes the constraint 'required'.
     * @param constraint is a List which contains all annotations of 'annotations' field.
     * @param annotationList is the annotation List after processing with the constraint 'ordered'.
     * @param random is a random integer generator.
     * @returna a List of processed annotations.
     * @throws IOException if error occur when reading constraints.
     */
    private static IonList checkRequired(List<String> constraint, IonList annotationList, Random random) throws IOException {
        IonList result = annotationList;
        if (!constraint.contains(IonSchemaUtilities.KEYWORD_REQUIRED)) {
            int randomValueOne = random.nextInt(annotationList.size());
            int randomValueTwo = random.nextInt(annotationList.size());
            List<IonValue> subAnnotationList = annotationList.subList(Math.min(randomValueOne, randomValueTwo), Math.max(randomValueOne, randomValueTwo));
            if (subAnnotationList != null) {
                result = ReadGeneralConstraints.SYSTEM.newEmptyList();
                for (int index = 0; index < subAnnotationList.size(); index++) {
                    result.add(index, subAnnotationList.get(index).clone());
                }
            } else {
                result = null;
            }
        }
        return result;
    }
}
