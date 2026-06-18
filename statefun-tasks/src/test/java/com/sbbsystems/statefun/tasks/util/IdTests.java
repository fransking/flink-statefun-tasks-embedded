/*
 * Copyright [2026] [Frans King]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sbbsystems.statefun.tasks.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Base64;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class IdTests {

    @ParameterizedTest
    @CsvSource({
        "00000000-0000-0000-0000-000000000000, AAAAAAAAAAAAAAAAAAAAAA",
        "ffffffff-ffff-ffff-ffff-ffffffffffff, _____________________w",
        "550e8400-e29b-41d4-a716-446655440000, VQ6EAOKbQdSnFkRmVUQAAA",
        "6ba7b810-9dad-11d1-80b4-00c04fd430c8, a6e4EJ2tEdGAtADAT9QwyA",
        "6ba7b811-9dad-11d1-80b4-00c04fd430c8, a6e4EZ2tEdGAtADAT9QwyA"
    })
    void generate_knownUuidProducesExpectedBase64(String uuidStr, String expectedBase64) {
        UUID uuid = UUID.fromString(uuidStr);
        java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(new byte[16]);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        String encoded = Base64.getUrlEncoder().withoutPadding().encodeToString(bb.array());
        assertEquals(expectedBase64, encoded);
    }

    @ParameterizedTest
    @CsvSource({
        "AAAAAAAAAAAAAAAAAAAAAA, 00000000-0000-0000-0000-000000000000",
        "_____________________w, ffffffff-ffff-ffff-ffff-ffffffffffff",
        "VQ6EAOKbQdSnFkRmVUQAAA, 550e8400-e29b-41d4-a716-446655440000",
        "a6e4EJ2tEdGAtADAT9QwyA, 6ba7b810-9dad-11d1-80b4-00c04fd430c8",
        "a6e4EZ2tEdGAtADAT9QwyA, 6ba7b811-9dad-11d1-80b4-00c04fd430c8"
    })
    void fromBase64_knownBase64ProducesExpectedUuid(String base64, String expectedUuidStr) {
        UUID decoded = Id.fromBase64(base64);
        assertEquals(UUID.fromString(expectedUuidStr), decoded);
    }

    @Test
    void generate_returnsBase64UrlEncodedString() {
        String id = Id.generate();
        assertNotNull(id);
        // base64url without padding for 16 bytes = 22 chars
        assertEquals(22, id.length());
        // must only contain base64url characters (no +, /, or =)
        assertTrue(id.matches("[A-Za-z0-9_-]+"), "ID should be base64url encoded: " + id);
    }

    @Test
    void generate_returnsDifferentValuesEachTime() {
        String id1 = Id.generate();
        String id2 = Id.generate();
        assertNotEquals(id1, id2);
    }

    @Test
    void fromBase64_reversesGenerate() {
        // Round-trip: encode a known UUID and decode it back
        UUID original = UUID.randomUUID();
        java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(new byte[16]);
        bb.putLong(original.getMostSignificantBits());
        bb.putLong(original.getLeastSignificantBits());
        String encoded = Base64.getUrlEncoder().withoutPadding().encodeToString(bb.array());

        UUID decoded = Id.fromBase64(encoded);

        assertEquals(original, decoded);
    }

    @Test
    void fromBase64_canDecodeGeneratedId() {
        String id = Id.generate();
        // Should not throw
        UUID uuid = Id.fromBase64(id);
        assertNotNull(uuid);
    }

    @Test
    void generateAndFromBase64_roundTrip() {
        // Encode → decode → re-encode must yield the same string
        String id1 = Id.generate();
        UUID uuid = Id.fromBase64(id1);

        java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(new byte[16]);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        String id2 = Base64.getUrlEncoder().withoutPadding().encodeToString(bb.array());

        assertEquals(id1, id2);
    }
}

