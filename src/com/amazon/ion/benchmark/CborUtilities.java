package com.amazon.ion.benchmark;

/**
 * Utility class for CBOR-related functions.
 */
class CborUtilities {

    // Per RFC 7049 (the CBOR specification), CBOR's optional self identification tag is 55799, encoded as 0xD9D9F7.
    static final byte[] CBOR_SELF_IDENTIFICATION_TAG = new byte[] {(byte) 0xD9, (byte) 0xD9, (byte) 0xF7};

    private CborUtilities() {
        // Do not instantiate.
    }

    /**
     * Determines whether the given data self-identifies as CBOR.
     * @param data the data to test.
     * @return true if the given data begins with the CBOR self-identification tag.
     */
    static boolean isCbor(byte[] data) {
        if (data.length < CBOR_SELF_IDENTIFICATION_TAG.length) {
            return false;
        }
        for (int i = 0; i < CBOR_SELF_IDENTIFICATION_TAG.length; i++) {
            if (data[i] != CBOR_SELF_IDENTIFICATION_TAG[i]) {
                return false;
            }
        }
        return true;
    }
}
