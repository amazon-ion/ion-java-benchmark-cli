package com.amazon.ion.benchmark;

/**
 * Represents amounts from "none" to "all".
 */
public enum Gradient {
    ALL,
    NONE
    // TODO options could be added to allow more manual configuration (e.g. via pointing to a file), or to allow
    //  preservation of characteristics from the input file. If more specific options are needed, a different enum
    //  more suited to the purpose may need to be used instead.
}
