package com.amazon.ion.benchmark.schema;

import com.amazon.ion.IonStruct;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.benchmark.schema.constraints.Contains;
import com.amazon.ion.benchmark.schema.constraints.Fields;
import com.amazon.ion.benchmark.schema.constraints.OrderedElements;
import com.amazon.ion.benchmark.schema.constraints.QuantifiableConstraints;
import com.amazon.ion.benchmark.schema.constraints.Regex;
import com.amazon.ion.benchmark.schema.constraints.ReparsedConstraint;
import com.amazon.ion.benchmark.schema.constraints.TimestampPrecision;
import com.amazon.ion.benchmark.schema.constraints.ValidValues;

import java.util.HashMap;
import java.util.Map;

// Parsing the type definition in ISL file into ReparsedType format which allows getting constraints information directly.
public class ReparsedType {
    private static final String KEYWORD_TIMESTAMP_PRECISION = "timestamp_precision";
    private static final String KEYWORD_TYPE = "type";
    private static final String KEYWORD_OCCURS = "occurs";
    private static final String KEYWORD_CODE_POINT_LENGTH = "codepoint_length";
    private static final String KEYWORD_REGEX = "regex";
    private static final String KEYWORD_CONTAINER_LENGTH = "container_length";
    private static final String KEYWORD_BYTE_LENGTH = "byte_length";
    private static final String KEYWORD_SCALE = "scale";
    private static final String KEYWORD_PRECISION = "precision";
    private static final String KEYWORD_VALID_VALUES = "valid_values";
    private static final String KEYWORD_NAME = "name";
    private static final String KEYWORD_FIELDS = "fields";
    private static final String KEYWORD_CONTAINS = "contains";
    private static final String KEYWORD_ORDERED_ELEMENTS = "ordered_elements";
    // Using map to avoid processing the multiple repeat constraints situation.
    private final Map<String, ReparsedConstraint> constraintMap;
    private final IonStruct constraintStruct;

    /**
     * Initializing the newly created ReparsedType object.
     * @param typeDefinition represents type definition of ISL file.
     */
    public ReparsedType(IonStruct typeDefinition) {
        this.constraintStruct = typeDefinition;
        constraintMap = new HashMap<>();
        constraintStruct.forEach(this::handleField);
    }

    /**
     * Get the name of type definition.
     * @return the name of type definition.
     */
    public String getName() {
        return constraintStruct.get(KEYWORD_NAME).toString();
    }

    /**
     * Handling the fields which are not used for specifying generated data.
     * @param field represents the field contained by the type definition.
     */
    private void handleField(IonValue field) {
        switch (field.getFieldName()) {
            case KEYWORD_NAME:
            case KEYWORD_TYPE:
            case KEYWORD_OCCURS:
                return;
            default:
                constraintMap.put(field.getFieldName(), toConstraint(field));
        }
    }

    /**
     * Processing field 'occurs' and return the occurrence value.
     * @param constraintStruct represents the <VARIABLY_OCCURRING_TYPE_REFERENCE> in IonStruct format.
     * @return an integer value which represents the required occurrence of the field/element.
     */
    public static int getOccurs(IonStruct constraintStruct) {
        IonValue occursValue = constraintStruct.get(KEYWORD_OCCURS);
        // The default value for occurs is specific to each constraint;
        // For constraint Field, the default value of occurs is optional.
        // For constraint ordered_elements, the default value of occurs is required.
        if (occursValue == null) {
            return -1;
        } else {
            Occurs occurs = Occurs.of(occursValue);
            return occurs.getOccurRange().getRandomQuantifiableValueFromRange().intValue();
        }
    }

    /**
     * Helping access the private attribute constraintStruct.
     * @return an IonStruct which represents the value of constraintStruct.
     */
    public IonStruct getConstraintStruct() {
        return this.constraintStruct;
    }

    /**
     * Get the value of constraint 'type' in IonType format.
     * @return the value of 'type' in IonType format.
     */
    public IonType getIonType() {
        return IonType.valueOf(constraintStruct.get(KEYWORD_TYPE).toString().toUpperCase());
    }

    /**
     * Get the constraintMap.
     * The keys in constraintMap represent constraint name, and the values represents the ReparsedConstraint.
     * @return constraintMap.
     */
    public Map<String, ReparsedConstraint> getConstraintMap() {
        return constraintMap;
    }

    /**
     * This method helps to categorize constraints based on the data type that they represent.
     * @param field represents the field contained in type definition.
     * @return ReparsedConstraints which are processed based on the provided constraint 'type'.
     */
    private ReparsedConstraint toConstraint(IonValue field) {
        switch (field.getFieldName()) {
            //TODO: Add cases of constraints 'annotation'.
            //TODO: Add container type constraints: 'element', these might cover some of the implemented constraints.
            case KEYWORD_BYTE_LENGTH:
            case KEYWORD_PRECISION:
            case KEYWORD_SCALE:
            case KEYWORD_CODE_POINT_LENGTH:
            case KEYWORD_CONTAINER_LENGTH:
                return QuantifiableConstraints.of(field);
            case KEYWORD_VALID_VALUES:
                return ValidValues.of(field);
            case KEYWORD_REGEX:
                return Regex.of(field);
            case KEYWORD_TIMESTAMP_PRECISION:
                return TimestampPrecision.of(field);
            case KEYWORD_FIELDS:
                return Fields.of(field);
            case KEYWORD_ORDERED_ELEMENTS:
                return OrderedElements.of(field);
            case KEYWORD_CONTAINS:
                return Contains.of(field);
            default:
                // For now, Ion Data Generator doesn't support processing 'open' content.
                // If the constraint 'content' included in the ISL , the data generator will throw an exception.
                throw new IllegalArgumentException("This field is not understood: " + field);
        }
    }
}
