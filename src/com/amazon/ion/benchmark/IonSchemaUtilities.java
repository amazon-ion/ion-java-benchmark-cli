package com.amazon.ion.benchmark;

import com.amazon.ion.IonList;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonType;
import com.amazon.ion.Timestamp;
import com.amazon.ion.system.IonReaderBuilder;

import java.io.IOException;
import java.util.Random;

/**
 * Contain the methods which process the constraints provided by the Ion Schema file and define the constants relevant to the Ion Schema file.
 */
public class IonSchemaUtilities {
    public static final String keyWordAnnotations = "Annotations";
    public static final String keyWordRequired = "required";
    public static final String keyWordOptional = "optional";
    public static final String keyWordTimestampPrecision = "timestamp_precision";
    public static final String keyWordType = "type";
    public static final String keyWordFields = "fields";
    public static final String keyWordCodePointLength = "codepoint_length";
    public static final String keyWordOccurs = "occurs";

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
                            if (reader.stringValue().equals(keyWordRequired)) {
                                result = 1;
                            } else if (reader.stringValue().equals(keyWordOptional)) {
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
                if (reader.getFieldName().equals(keyWordTimestampPrecision)) {
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
     * Return the value of annotations, this method is available when the annotations are required, if there are more constraints relative to the annotations, more logic needed to be added here.
     * @param value is the Ion struct which contain the current constraint field.
     * @return an Ion List contains all annotations.
     */
    public static IonList getAnnotation(IonStruct value) {
        IonList annotationList = null;
        String annotationConstraint = value.get(keyWordAnnotations).getTypeAnnotations()[0];
        if (annotationConstraint.equals(keyWordRequired)) {
            annotationList = (IonList) value.get(keyWordAnnotations);
        }
        return annotationList;
    }
}
