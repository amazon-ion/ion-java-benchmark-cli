package com.amazon.ion.benchmark.schema.constraints;

import com.amazon.ion.IonList;
import com.amazon.ion.IonSequence;
import com.amazon.ion.IonTimestamp;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.benchmark.GeneratorOptions;

import java.math.BigDecimal;
import java.util.Arrays;

// Processing the constraint value which contains 'range' annotation.
public class Range {
    private static final String KEYWORD_RANGE = "range";
    private final IonSequence sequence;

    /**
     * Initializing the newly created Range object.
     * @param sequence represents the range value in IonSequence format.
     */
    public Range(IonSequence sequence) {
       this.sequence = sequence;
    }

    /**
     * Helping to access the private variable sequence.
     * @return IonSequence which represents the range value.
     */
    public IonSequence getSequence() {
        return this.sequence;
    }

    /**
     * Getting the lower bound value from range.
     * @param klass represent the Class object of different data types.
     * @param <T> represents different ion data types.
     * @return parameterized type data which extends IonValue.
     */
    public <T extends IonValue> T lowerBound(Class<T> klass) {
        return klass.cast(sequence.get(0));
    }

    /**
     * Getting the upper bound value from range.
     * @param klass represent the Class object of different data types.
     * @param <T> represents different ion data types.
     * @return parameterized type data which extends IonValue.
     */
    public <T extends IonValue> T upperBound(Class<T> klass) {
        return klass.cast(sequence.get(1));
    }

    /**
     * Parsing the provided IonValue into Range.
     * @param value represents the value of provided constraint.
     * @return an object of Range.
     */
    public static Range of(IonValue value) {
        IonSequence sequence;
        if (!(value instanceof IonList)) {
            sequence = value.getSystem().newList(value.clone(), value.clone());
            sequence.addTypeAnnotation(KEYWORD_RANGE);
        } else {
            sequence = (IonSequence) value;
        }
        return new Range(sequence);
    }

    /**
     * Checking whether the value contains annotation 'range'.
     * @param value represents the constraint value.
     * @return the result in the boolean format.
     */
    public static boolean isRange(IonValue value) {
        return Arrays.stream(value.getTypeAnnotations()).anyMatch(KEYWORD_RANGE::equals);
    }

    /**
     * Getting a random quantifiable value within the range. This method will be used when the range value is in '<RANGE<NUMBER>>' format.
     * @return a BigDecimal which is within the provided range. This value would be cast into different data types as needed.
     */
    public BigDecimal getRandomQuantifiableValueFromRange() {
        IonValue lowerBound = sequence.get(0);
        IonValue upperBound = sequence.get(1);
        BigDecimal lowerBoundBigDecimal = lowerBound.getType().equals(IonType.TIMESTAMP) ? ((IonTimestamp)lowerBound).getDecimalMillis() : new BigDecimal(lowerBound.toString());
        BigDecimal upperBoundBigDecimal = upperBound.getType().equals(IonType.TIMESTAMP) ? ((IonTimestamp)upperBound).getDecimalMillis() : new BigDecimal(upperBound.toString());
        return lowerBoundBigDecimal.add(new BigDecimal(GeneratorOptions.random.nextDouble()).multiply(upperBoundBigDecimal.subtract(lowerBoundBigDecimal)));
    }
}
