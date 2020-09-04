package com.amazon.ion.benchmark;

/**
 * I/O type.
 */
enum IoType {

    /**
     * Read from or write to a file on disk.
     */
    FILE,

    /**
     * Read from or write to a byte array in memory.
     */
    BUFFER
}
