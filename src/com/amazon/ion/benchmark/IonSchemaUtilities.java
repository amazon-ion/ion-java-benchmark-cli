package com.amazon.ion.benchmark;

import com.amazon.ion.IonList;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.Timestamp;
import com.amazon.ion.system.IonReaderBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Contain the methods which process the constraints provided by the Ion Schema file and define the constants relevant to the Ion Schema file.
 */
public class IonSchemaUtilities {
    public static final String KEYWORD_ANNOTATIONS = "Annotations";
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
    public static final String KEYWORD_CONSTRAINT = "constraint";
    public static final String KEYWORD_NAME = "name";

    /**
     * Extract the value of the constraints, select from the set (occurs | container_length | codepoint_length).
     * @param value is the Ion struct which contain the current constraint field
     * @param keyWord is the field name of the constraint
     * @return the value of the current constraint.
     * @throws IOException if an error occur when constructing the IonReader.
     */
    public static int parseConstraints(IonStruct value, String keyWord) throws IOException {
        Random random = new Random();
        int result = 0;
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
                            int min = reader.intValue();
                            reader.next();
                            int max = reader.intValue();
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
     * Return the information of annotations in a HashMap which includes the constraint and values of annotation. The constraint is the from set(optional | required |ordered)
     * @param value is the Ion struct which contain the current constraint field.
     * @return a HashMap contains the information of annotations, both the constraint of the annotation and the value of the annotation.
     */
    public static Map<String, Object> getAnnotation(IonStruct value) {
        Map<String, Object> annotationMap = new HashMap<>();
        IonValue annotations = value.get(KEYWORD_ANNOTATIONS);
        if (annotations != null) {
            IonList annotationList = (IonList) value.get(KEYWORD_ANNOTATIONS);
            if (annotations.getTypeAnnotations().length != 0) {
                String annotationConstraint = annotations.getTypeAnnotations()[0];
                annotationMap.put(KEYWORD_CONSTRAINT, annotationConstraint);
            } else {
                annotationMap.put(KEYWORD_CONSTRAINT, KEYWORD_OPTIONAL);
            }
            annotationMap.put(KEYWORD_ANNOTATIONS, annotationList);
            return annotationMap;
        } else {
            return null;
        }
    }
}
