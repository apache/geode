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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class VersionJUnitTest {
  @Test
  public void testVersionClass() throws Exception {
    compare(KnownVersion.GFE_662, KnownVersion.GFE_66);
    compare(KnownVersion.GFE_6622, KnownVersion.GFE_662);
    compare(KnownVersion.GFE_71, KnownVersion.GFE_70);
    compare(KnownVersion.GFE_80, KnownVersion.GFE_70);
    compare(KnownVersion.GFE_80, KnownVersion.GFE_71);
    compare(KnownVersion.GFE_81, KnownVersion.GFE_70);
    compare(KnownVersion.GFE_81, KnownVersion.GFE_71);
    compare(KnownVersion.GFE_81, KnownVersion.GFE_80);
    compare(KnownVersion.GFE_82, KnownVersion.GFE_81);
    compare(KnownVersion.GEODE_1_1_0, KnownVersion.GFE_82);
    compare(KnownVersion.GEODE_1_2_0, KnownVersion.GEODE_1_1_1);
    compare(KnownVersion.GEODE_1_3_0, KnownVersion.GEODE_1_2_0);
    compare(KnownVersion.GEODE_1_4_0, KnownVersion.GEODE_1_3_0);
    compare(KnownVersion.GEODE_1_5_0, KnownVersion.GEODE_1_4_0);
    compare(KnownVersion.GEODE_1_6_0, KnownVersion.GEODE_1_5_0);
    compare(KnownVersion.GEODE_1_7_0, KnownVersion.GEODE_1_6_0);
    compare(KnownVersion.GEODE_1_8_0, KnownVersion.GEODE_1_7_0);
    compare(KnownVersion.GEODE_1_9_0, KnownVersion.GEODE_1_8_0);
    compare(KnownVersion.GEODE_1_10_0, KnownVersion.GEODE_1_9_0);
    compare(KnownVersion.GEODE_1_11_0, KnownVersion.GEODE_1_10_0);
    compare(KnownVersion.GEODE_1_12_0, KnownVersion.GEODE_1_11_0);
    compare(KnownVersion.GEODE_1_13_0, KnownVersion.GEODE_1_12_0);
    compare(KnownVersion.GEODE_1_14_0, KnownVersion.GEODE_1_13_0);
  }

  private void compare(KnownVersion later, KnownVersion earlier) {
    assertTrue(later.compareTo(earlier) > 0);
    assertTrue(later.equals(later));
    assertTrue(later.compareTo(later) == 0);
    assertTrue(earlier.compareTo(later) < 0);

    assertTrue(later.compareTo(Versioning.getVersionOrdinal(earlier.ordinal())) > 0);
    assertTrue(later.compareTo(Versioning.getVersionOrdinal(later.ordinal())) == 0);
    assertTrue(earlier.compareTo(Versioning.getVersionOrdinal(later.ordinal())) < 0);

    compareNewerVsOlder(later, earlier);
  }

  private void compareNewerVsOlder(KnownVersion newer, KnownVersion older) {
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
    assertTrue(KnownVersion.GFE_61.isOlderThan(KnownVersion.GFE_65));
    assertFalse(KnownVersion.GFE_65.isOlderThan(KnownVersion.GFE_65));
    assertFalse(KnownVersion.GFE_70.isOlderThan(KnownVersion.GFE_65));
    assertFalse(KnownVersion.GEODE_1_1_0.isOlderThan(KnownVersion.GFE_65));
  }

  @Test
  public void testFromOrdinalForCurrentVersionSucceeds() {
    final KnownVersion version = Versioning.getKnownVersionOrDefault(
        Versioning.getVersionOrdinal(KnownVersion.CURRENT_ORDINAL), null);
    assertThat(version).isNotNull();
    assertThat(version).isEqualTo(KnownVersion.CURRENT);
  }

  @Test
  public void ordinalImplMatchesVersion() {
    /*
     * We are not using the Version.getVersionOrdinal(short) factory method here
     * because we intend to test that Version and VersionOrdinal are cross-comparable.
     * The factory would return Version.GFE_82 which would foil our testing.
     */
    final UnknownVersion versionOrdinal = new UnknownVersion(KnownVersion.GFE_82.ordinal());
    assertThat(KnownVersion.GFE_82.equals(versionOrdinal))
        .as("GFE_82 Version equals VersionOrdinal").isTrue();
    assertThat(versionOrdinal.equals(KnownVersion.GFE_82))
        .as("GFE_82 VersionOrdinal equals Version").isTrue();
  }

}
