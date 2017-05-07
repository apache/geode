/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal;

import static org.junit.Assert.*;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.query.functional.IndexUsageInNestedQueryJUnitTest;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Inet4Address;
import java.security.SecureRandom;

/**
 * Tests the serialization and deserialization of strings made up of random Unicode code points.
 */
@Category(UnitTest.class)
public class InternalDataSerializerRandomizedJUnitTest {
  private static final int ITERATIONS = 1000;

  private static void testStringSerializedDeserializesToSameValue(String originalString)
      throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    DataSerializer.writeString(originalString, dataOutputStream);
    dataOutputStream.flush();

    byte[] stringBytes = byteArrayOutputStream.toByteArray();
    DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(stringBytes));
    String returnedString = DataSerializer.readString(dataInputStream);

    assertEquals("Deserialized string matches original", originalString, returnedString);
  }

  @Before
  public void setUp() {
    // this may be unnecessary, but who knows what tests run before us.
    InternalDataSerializer.reinitialize();
  }

  @Test
  public void testRandomStringsSerializeThenDeserializeToSameValues() throws Exception {
    RandomStringGenerator stringGenerator = new RandomStringGenerator();
    try {
      for (int i = 0; i < ITERATIONS; i++) {
        String str = stringGenerator.randomString();
        testStringSerializedDeserializesToSameValue(str);
      }
    } catch (Throwable throwable) {
      throw new Exception("Failed with seed " + stringGenerator.getSeedString(), throwable);
    }
  }

  @Test
  public void testEdgeCaseSerializationDeserialization() throws IOException {
    testStringSerializedDeserializesToSameValue("\0");
    testStringSerializedDeserializesToSameValue("");

    RandomStringGenerator stringGenerator = new RandomStringGenerator();
    testStringSerializedDeserializesToSameValue(stringGenerator.randomString(65534));
    testStringSerializedDeserializesToSameValue(stringGenerator.randomString(65535));
    testStringSerializedDeserializesToSameValue(stringGenerator.randomString(65536));
    testStringSerializedDeserializesToSameValue(stringGenerator.randomString(65537));

    testStringSerializedDeserializesToSameValue(stringGenerator.randomString());
  }

  @Test
  public void testABigString() throws IOException {
    RandomStringGenerator stringGenerator = new RandomStringGenerator();
    final int strlen = 1024 * 1024 * 5;

    testStringSerializedDeserializesToSameValue(stringGenerator.randomString(strlen));
  }

  private static class RandomStringGenerator {
    public static final int SEED_BYTES = 8;
    public static final int MAX_STRING_LENGTH = 65538;
    // UTF-16 can represent any codepoint with 2 chars.
    public static final int MAX_UTF16_CHARS = MAX_STRING_LENGTH * 2;
    public static final int UNICODE_MAX = Character.MAX_CODE_POINT;
    private final byte[] seed;
    private final SecureRandom randomNumberGenerator;

    public byte[] getSeed() {
      return seed.clone();
    }

    /**
     * Returns a string representation of the seed, in xsd:hexBinary. This can be passed directly to
     * the String constructor of this class to recreate with the same seed.
     */
    public String getSeedString() {
      return DatatypeConverter.printHexBinary(seed);
    }

    /**
     * Generate and return a random string made up of a series of Unicode codepoints (integers).
     * This can be any series of codepoints, even unreserved ones.
     */
    public RandomStringGenerator() {
      this(SecureRandom.getSeed(SEED_BYTES));
    }

    /**
     * Construct based on a provided seed. Mostly this should be useful for reproducing tests.
     * <p>
     * Technically we can take any size of seed, but we use one of size SEED_BYTES for tests.
     */
    public RandomStringGenerator(byte[] seed) {
      this.seed = seed.clone();
      randomNumberGenerator = new SecureRandom(this.seed);
    }

    /**
     * @param seedString an xsd:hexBinary string representing a seed. Normally acquired from
     *        `getSeedString();`
     */
    public RandomStringGenerator(String seedString) {
      this(DatatypeConverter.parseHexBinary(seedString));
    }

    public int randomCodepoint() {
      return randomNumberGenerator.nextInt(UNICODE_MAX);
    }

    /**
     * @return A random string made of Unicode codepoints in the range from 0 to UNICODE_MAX. These
     *         strings will not necessarily be valid, as some codepoints in that range may be
     *         unallocated in the Unicode spec.
     */
    public String randomString() {
      return randomString(randomNumberGenerator.nextInt(MAX_UTF16_CHARS));
    }

    public String randomString(int length) {
      StringBuilder stringBuilder = new StringBuilder(length * 2);

      for (int i = 0; i < length; i++) {
        int codepoint = randomCodepoint();
        try {
          stringBuilder.appendCodePoint(codepoint);
        } catch (IllegalArgumentException ex) {
          System.out.println("Generated illegal codepoint " + codepoint);
        }
      }
      return stringBuilder.toString();
    }
  }
}
