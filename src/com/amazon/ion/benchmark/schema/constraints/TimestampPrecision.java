package com.amazon.ion.benchmark.schema.constraints;

import com.amazon.ion.IonSequence;
import com.amazon.ion.IonValue;
import com.amazon.ion.Timestamp;
import com.amazon.ion.benchmark.GeneratorOptions;
import com.amazon.ion.benchmark.IonSchemaUtilities;

public class TimestampPrecision extends QuantifiableConstraints{

    /**
     * Initializing the newly created TimestampPrecision object.
     * @param value represent the value of constraint 'timestamp_precision'.
     */
    private TimestampPrecision(IonValue value) {
        super(value);
    }

    /**
     * Parsing the constraint 'timestamp_precision' into TimestampPrecision.
     * @param field represent the value of constraint 'timestamp_precision'.
     * @return the object of TimestampPrecision.
     */
    public static TimestampPrecision of(IonValue field) {
        return new TimestampPrecision(field);
    }

    /**
     * Getting the timestamp precision randomly from the provided timestamp precision range.
     * @param range represents the range of timestamp precision.
     * @return randomly generated Timestamp.Precision.
     */
    public static Timestamp.Precision getRandomTimestampPrecision(Range range) {
        IonSequence constraintSequence = range.getSequence();
        Timestamp.Precision[] precisions = Timestamp.Precision.values();
        String lowerBound = constraintSequence.get(0).toString();
        String upperBound = constraintSequence.get(1).toString();
        int lowerBoundOrdinal = lowerBound.equals(IonSchemaUtilities.KEYWORD_MIN) ? 0 : Timestamp.Precision.valueOf(lowerBound.toUpperCase()).ordinal();
        int upperBoundOrdinal = upperBound.equals(IonSchemaUtilities.KEYWORD_MAX) ? precisions.length : Timestamp.Precision.valueOf(upperBound.toUpperCase()).ordinal();
        int randomIndex = GeneratorOptions.random.nextInt(upperBoundOrdinal - lowerBoundOrdinal + 1) + lowerBoundOrdinal;
        return precisions[randomIndex];
    }
}
