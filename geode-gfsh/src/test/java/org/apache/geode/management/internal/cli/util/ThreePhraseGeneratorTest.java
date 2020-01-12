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
package org.apache.geode.management.internal.cli.util;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;


public class ThreePhraseGeneratorTest {
  @Test
  public void testGenerate() {
    final ThreePhraseGenerator tpg = new ThreePhraseGenerator();

    final String phrase = tpg.generate('-');
    assertNotNull("Generated string is null", phrase);
    assertNotEquals("Generated string is empty", "", phrase);
  }

  @Test
  public void testGeneratesOnlyLetters() {
    final ThreePhraseGenerator tpg = new ThreePhraseGenerator();

    // Since there is a random choice of word, try a bunch of times to get coverage.
    for (int attempt = 1; attempt < 50000; ++attempt) {
      final String phrase = tpg.generate('x');
      assertTrue("Generated string does not have at least five characters", 5 <= phrase.length());
      for (int index = 0; index < phrase.length(); ++index) {
        final char c = phrase.charAt(index);
        assertTrue(
            "Character in \"" + phrase + "\" at index " + index + ", '" + c + "', is a not letter",
            Character.isLetter(c));
      }
    }
  }
}
