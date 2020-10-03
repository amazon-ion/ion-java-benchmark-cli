package com.amazon.ion.benchmark;

import com.amazon.ion.IonSystem;
import com.amazon.ion.system.IonSystemBuilder;

class Constants {
    static final IonSystem ION_SYSTEM = IonSystemBuilder.standard().build();
    static final String LIMIT_NAME = "n";
    static final String PREALLOCATION_NAME = "L";
    static final String FLUSH_PERIOD_NAME = "d";
    static final String ION_API_NAME = "a";
    static final String IO_TYPE_NAME = "t";
    static final String ION_IMPORTS_FOR_INPUT_NAME = "I";
    static final String ION_IMPORTS_FOR_BENCHMARK_NAME = "c";
    static final String FORMAT_NAME = "f";
    static final String ION_READER_NAME = "R";
    static final String ION_USE_SYMBOL_TOKENS_NAME = "k";
    static final String ION_FLOAT_WIDTH_NAME = "W";
    static final String PATHS_NAME = "s";
    static final String ION_WRITER_BLOCK_SIZE_NAME = "b";
    static final String AUTO_VALUE = "auto";
    static final String NONE_VALUE = "none";

    private Constants() {
        // Do not instantiate.
    }
}
