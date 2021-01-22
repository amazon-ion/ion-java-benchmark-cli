package com.amazon.ion.benchmark;

/**
 * Serialization/deserialization APIs available for comparison.
 */
enum API {

    /**
     * For Ion: the IonReader and IonWriter APIs. For JSON (Jackson): the JsonParser and JsonGenerator APIs.
     */
    STREAMING,

    /**
     * For Ion: the DOM APIs (IonLoader, IonValue, etc.). For JSON (Jackson): JsonNode via ObjectMapper.
     */
    DOM
}
