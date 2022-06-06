package com.amazon.ion.benchmark.schema.constraints;

import com.amazon.ion.*;
import com.amazon.ion.benchmark.IonSchemaUtilities;

import java.util.Random;

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
        Random random = new Random();
        IonSequence constraintSequence = range.sequence;
        Timestamp.Precision[] precisions = Timestamp.Precision.values();
        String lowerBound = constraintSequence.get(0).toString();
        String upperBound = constraintSequence.get(1).toString();
        int lowerBoundOrdinal = lowerBound.equals(IonSchemaUtilities.KEYWORD_MIN) ? 0 : Timestamp.Precision.valueOf(lowerBound.toUpperCase()).ordinal();
        int upperBoundOrdinal = upperBound.equals(IonSchemaUtilities.KEYWORD_MAX) ? precisions.length : Timestamp.Precision.valueOf(upperBound.toUpperCase()).ordinal();
        int randomIndex = random.nextInt(upperBoundOrdinal - lowerBoundOrdinal + 1) + lowerBoundOrdinal;
        return precisions[randomIndex];
    }
}
