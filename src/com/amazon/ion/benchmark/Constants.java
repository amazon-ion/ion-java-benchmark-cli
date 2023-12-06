package com.amazon.ion.benchmark;

import com.amazon.ion.IonSystem;
import com.amazon.ion.system.IonSystemBuilder;

class Constants {
    static final IonSystem ION_SYSTEM = IonSystemBuilder.standard().build();
    static final String LIMIT_NAME = "n";
    static final String PREALLOCATION_NAME = "L";
    static final String FLUSH_PERIOD_NAME = "d";
    static final String API_NAME = "a";
    static final String IO_TYPE_NAME = "t";
    static final String IO_BUFFER_SIZE_NAME = "z";
    static final String ION_IMPORTS_FOR_INPUT_NAME = "I";
    static final String ION_IMPORTS_FOR_BENCHMARK_NAME = "c";
    static final String FORMAT_NAME = "f";
    static final String ION_READER_NAME = "R";
    static final String ION_USE_SYMBOL_TOKENS_NAME = "k";
    static final String ION_FLOAT_WIDTH_NAME = "W";
    static final String ION_USE_LOB_CHUNKS_NAME = "e";
    static final String ION_USE_BIG_DECIMALS_NAME = "D";
    static final String ION_READER_BUFFER_SIZE_NAME = "Z";
    static final String PATHS_NAME = "s";
    static final String ION_WRITER_BLOCK_SIZE_NAME = "b";
    static final String JSON_USE_BIG_DECIMALS_NAME = "g";
    static final String AUTO_VALUE = "auto";
    static final String NONE_VALUE = "none";
    static final String AUTO_FLUSH_ENABLED = "m";

    private Constants() {
        // Do not instantiate.
    }
}
