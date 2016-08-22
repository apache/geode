/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.compression;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.compression.Compressor;
import com.gemstone.gemfire.compression.SnappyCompressor;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Tests the Snappy {@link Compressor}.
 */
@Category(IntegrationTest.class)
public class SnappyCompressorJUnitTest {

  /**
   * Tests {@link Compressor#compress(byte[])} and {@link Compressor#decompress(byte[])} using the Snappy compressor.
   */
  @Test
  public void testCompressByteArray() throws Exception {
    String compressMe = "Hello, how are you?";
    byte[] compressMeData = new SnappyCompressor().compress(compressMe.getBytes());
    String uncompressedMe = new String(SnappyCompressor.getDefaultInstance().decompress(compressMeData));

    assertEquals(compressMe, uncompressedMe);
  }
}
