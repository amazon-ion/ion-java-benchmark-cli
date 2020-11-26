package com.amazon.ion.benchmark;

/**
 * The underlying IonReader implementation.
 */
enum IonReaderType {

    /**
     * The default non-incremental reader for both text and binary.
     */
    NON_INCREMENTAL,

    /**
     * The opt-in incremental reader (binary only).
     */
    INCREMENTAL
}
