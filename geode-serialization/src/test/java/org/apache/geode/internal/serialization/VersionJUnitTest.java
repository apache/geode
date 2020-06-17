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
package org.apache.geode.internal.serialization;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class VersionJUnitTest {
  @Test
  public void testVersionClass() throws Exception {
    compare(Version.GFE_662, Version.GFE_66);
    compare(Version.GFE_6622, Version.GFE_662);
    compare(Version.GFE_71, Version.GFE_70);
    compare(Version.GFE_80, Version.GFE_70);
    compare(Version.GFE_80, Version.GFE_71);
    compare(Version.GFE_81, Version.GFE_70);
    compare(Version.GFE_81, Version.GFE_71);
    compare(Version.GFE_81, Version.GFE_80);
    compare(Version.GFE_82, Version.GFE_81);
    compare(Version.GEODE_1_1_0, Version.GFE_82);
    compare(Version.GEODE_1_2_0, Version.GEODE_1_1_1);
    compare(Version.GEODE_1_3_0, Version.GEODE_1_2_0);
    compare(Version.GEODE_1_4_0, Version.GEODE_1_3_0);
    compare(Version.GEODE_1_5_0, Version.GEODE_1_4_0);
    compare(Version.GEODE_1_6_0, Version.GEODE_1_5_0);
    compare(Version.GEODE_1_7_0, Version.GEODE_1_6_0);
    compare(Version.GEODE_1_8_0, Version.GEODE_1_7_0);
    compare(Version.GEODE_1_9_0, Version.GEODE_1_8_0);
    compare(Version.GEODE_1_10_0, Version.GEODE_1_9_0);
    compare(Version.GEODE_1_11_0, Version.GEODE_1_10_0);
    compare(Version.GEODE_1_12_0, Version.GEODE_1_11_0);
    compare(Version.GEODE_1_13_0, Version.GEODE_1_12_0);
    compare(Version.GEODE_1_14_0, Version.GEODE_1_13_0);
  }

  private void compare(Version later, Version earlier) {
    assertTrue(later.compareTo(earlier) > 0);
    assertTrue(later.equals(later));
    assertTrue(later.compareTo(later) == 0);
    assertTrue(earlier.compareTo(later) < 0);

    assertTrue(later.compareTo(earlier.ordinal()) > 0);
    assertTrue(later.compareTo(later.ordinal()) == 0);
    assertTrue(earlier.compareTo(later.ordinal()) < 0);

    compareNewerVsOlder(later, earlier);
  }

  private void compareNewerVsOlder(Version newer, Version older) {
    assertTrue(older.isOlderThan(newer));
    assertFalse(newer.isOlderThan(older));
    assertFalse(newer.isOlderThan(newer));
    assertFalse(older.isOlderThan(older));

    assertTrue(older.isNotOlderThan(older));
    assertFalse(older.isNotOlderThan(newer));
    assertTrue(newer.isNotOlderThan(newer));
    assertTrue(newer.isNotOlderThan(older));

    assertTrue(newer.isNewerThan(older));
    assertFalse(older.isNewerThan(newer));
    assertFalse(newer.isNewerThan(newer));
    assertFalse(older.isNewerThan(older));

    assertTrue(older.isNotNewerThan(older));
    assertTrue(older.isNotNewerThan(newer));
    assertTrue(newer.isNotNewerThan(newer));
    assertFalse(newer.isNotNewerThan(older));
  }

  @Test
  public void testIsPre65() {
    assertTrue(Version.GFE_61.isPre65());
    assertFalse(Version.GFE_65.isPre65());
    assertFalse(Version.GFE_70.isPre65());
    assertFalse(Version.GEODE_1_1_0.isPre65());
  }

  @Test
  public void testFromOrdinalForCurrentVersionSucceeds()
      throws UnsupportedSerializationVersionException {
    Version.fromOrdinal(Version.CURRENT_ORDINAL);
  }

}
