package com.amazon.ion.benchmark.schema.constraints;

import com.amazon.ion.IonList;
import com.amazon.ion.IonValue;

import java.util.Arrays;
import java.util.Random;

/**
 * This class aims to process the constraint 'annotations'. After parsing the constraint value to Annotations object, we
 * are able to get the processed annotations in an IonList format.
 */
public class Annotations implements ReparsedConstraint {
    private IonList annotations;

    /**
     * Initializing the newly created Annotations object.
     * @param field represents the value of constraint 'annotations'.
     */
    private Annotations(IonValue field) {
        this.annotations = parseAnnotations(field);
    }

    /**
     * Helping access the private attribute 'annotations'.
     * @return the attribute 'annotations'.
     */
    public IonList getAnnotations() {
        return this.annotations;
    }

    /**
     * Process the annotation of the constraint 'annotations'. By default, individual annotations
     * are optional and this default may be overridden by annotating the annotations list with 'required'.
     * If annotations must be applied to value in specified order, the list may be annotated with 'ordered'.
     * This method will process these annotations and return the value aligned with these specifications.
     * @param field represents the value of constraint 'annotations'.
     * @return the list of annotations that are used for annotating IonValue.
     */
    private IonList parseAnnotations(IonValue field) {
        String[] annotationsSpecificationList = field.getTypeAnnotations();
        // If the constraint 'annotation' is not annotated or annotated with "optional",
        if (annotationsSpecificationList.length == 0 || Arrays.asList(annotationsSpecificationList).contains("optional")) {
            return randomlyReturnAnnotations(field);
        } else {
            return (IonList)field;
        }
    }

    /**
     * Process the constraint 'annotations' without annotation or contains 'optional' annotation.
     * @param field represents the value of constraint 'annotations'.
     * @return a null value or a list of annotations randomly.
     */
    private IonList randomlyReturnAnnotations(IonValue field) {
        Random random = new Random();
        int value = random.nextInt(2);
        switch (value) {
            case 1:
                return (IonList)field;
            default:
                return null;
        }
    }

    /**
     * Parsing the value of constraint 'annotations' into Annotations.
     * @param field represents the value of constraint 'annotations'.
     * @return the parsed object Annotations.
     */
    public static Annotations of(IonValue field) {
        return new Annotations(field);
    }
}
