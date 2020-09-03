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
    static final String ION_IMPORTS_NAME = "c";
    static final String FORMAT_NAME = "f";
    static final String ION_READER_NAME = "R";
    static final String PATHS_NAME = "s";
    static final String AUTO_VALUE = "auto";

    private Constants() {
        // Do not instantiate.
    }
}
