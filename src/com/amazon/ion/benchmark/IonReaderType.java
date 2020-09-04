package com.amazon.ion.benchmark;

/**
 * The underlying IonReader implementation.
 */
enum IonReaderType {

    /**
     * The default blocking reader for both text and binary.
     */
    BLOCKING,

    /**
     * The opt-in non-blocking reader (binary only).
     */
    NON_BLOCKING
}
