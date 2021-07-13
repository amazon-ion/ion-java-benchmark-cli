package com.amazon.ion.benchmark;

import com.amazon.ion.IonType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Check if the current combination of options is valid.
 * e.g. The specific options only follow the specific data type, "--text-code-point-range" cannot follow the type "decimal"
 */
public class GeneratorOptionsValidator {
    final public static Set<String> INVALID_FOR_TIMESTAMP = Collections.unmodifiableSet(new HashSet<>(Arrays.asList("-N", "--text-code-point-range", "-E", "--decimal-exponent-range <exp_range>", "-C", "--decimal-coefficient-digit-range", "-Q", "--input-ion-schema")));
    final public static Set<String> INVALID_FOR_STRING = Collections.unmodifiableSet(new HashSet<>(Arrays.asList("-M", "--timestamps-template", "-E", "--decimal-exponent-range <exp_range>", "-C", "--decimal-coefficient-digit-range", "-Q", "--input-ion-schema")));
    final public static Set<String> INVALID_FOR_DECIMAL = Collections.unmodifiableSet(new HashSet<>(Arrays.asList("-N", "--text-code-point-range", "-M", "--timestamps-template", "-Q", "--input-ion-schema")));
    final public static Set<String> INVALID_FOR_ION_SCHEMA = Collections.unmodifiableSet(new HashSet<>(Arrays.asList("-N", "--text-code-point-range", "-E", "--decimal-exponent-range <exp_range>", "-C", "--decimal-coefficient-digit-range","-M", "--timestamps-template", "-T", "--data-type")));

    /**
     * The method combine all options which are not available for the current data type, and check
     * if the current command line which aim to generate the current type of data contain those options or not.
     * @param invalidOptionsSet  is a set of options which is not valid for the current data type.
     * @param commandLine is a list of arguments which indicates options.
     */
    private static void throwException(Set<String> invalidOptionsSet, List<String> commandLine) {
        for (String option : commandLine) {
            if (invalidOptionsSet.contains(option)) throw new IllegalStateException("Please provide options supported by the current data type");
        }
    }

    /**
     * This method is used for checking if the combination of options is valid or not.
     * In other word, some options can be only assigned to a specific type of data, if these options provided
     * when generating other types, then this combination ia not valid.
     * @param args is the arguments provided in command line.
     * @param optionsMap is a hashmap which match the option name and its value.
     */
    public static void checkValid(String [] args, Map <String, Object> optionsMap) {
        List<String> commandLine = new ArrayList<>(Arrays.asList(args));
        if (optionsMap.get("--data-type") != null) {
            IonType type = IonType.valueOf(optionsMap.get("--data-type").toString().toUpperCase());
            switch (type) {
                case TIMESTAMP:
                    GeneratorOptionsValidator.throwException(INVALID_FOR_TIMESTAMP, commandLine);
                    break;
                case STRING:
                    GeneratorOptionsValidator.throwException(INVALID_FOR_STRING, commandLine);
                    break;
                case DECIMAL:
                    GeneratorOptionsValidator.throwException(INVALID_FOR_DECIMAL, commandLine);
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + type);
            }
        } else {
            GeneratorOptionsValidator.throwException(INVALID_FOR_ION_SCHEMA, commandLine);
        }
    }
}
