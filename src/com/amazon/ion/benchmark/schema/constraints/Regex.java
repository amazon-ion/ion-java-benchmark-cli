package com.amazon.ion.benchmark.schema.constraints;

import com.amazon.ion.IonValue;

public class Regex implements ReparsedConstraint{
    private final String pattern;

    /**
     * Initializing the newly created object.
     * @param pattern represents the value of constraint 'regex'.
     */
    private Regex(IonValue pattern) {
        this.pattern = pattern.toString().replace("\"","");
    }

    /**
     * Getting the 'regex' value.
     * @return a String to represent the value of 'regex'.
     */
    public String getPattern() {
        return this.pattern;
    }

    /**
     * Parsing constraint field into Regex.
     * @param value represents the value of constraint 'regex'.
     * @return newly created Scale object.
     */
    public static Regex of(IonValue value) {
        return new Regex(value);
    }
}
