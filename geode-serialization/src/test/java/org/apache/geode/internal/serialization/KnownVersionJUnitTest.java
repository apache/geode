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

public class KnownVersionJUnitTest {

  @Test
  public void testKnownVersionClass() {
    compare(KnownVersion.GFE_90, KnownVersion.GFE_81);
    compare(KnownVersion.GEODE_1_1_0, KnownVersion.GFE_90);
    compare(KnownVersion.GEODE_1_1_1, KnownVersion.GEODE_1_1_0);
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
    compare(KnownVersion.GEODE_1_12_1, KnownVersion.GEODE_1_12_0);
    compare(KnownVersion.GEODE_1_13_0, KnownVersion.GEODE_1_12_1);
    compare(KnownVersion.GEODE_1_13_2, KnownVersion.GEODE_1_13_0);
    compare(KnownVersion.GEODE_1_14_0, KnownVersion.GEODE_1_13_2);
  }

  @Test
  public void testClientServerProtocolVersion() {
    assertThat(KnownVersion.GEODE_1_7_0.getClientServerProtocolVersion())
        .isEqualTo(KnownVersion.GEODE_1_7_0);
    assertThat(KnownVersion.GEODE_1_8_0.getClientServerProtocolVersion())
        .isEqualTo(KnownVersion.GEODE_1_8_0);
    assertThat(KnownVersion.GEODE_1_9_0.getClientServerProtocolVersion())
        .isEqualTo(KnownVersion.GEODE_1_9_0);
    assertThat(KnownVersion.GEODE_1_10_0.getClientServerProtocolVersion())
        .isEqualTo(KnownVersion.GEODE_1_10_0);
    assertThat(KnownVersion.GEODE_1_11_0.getClientServerProtocolVersion())
        .isEqualTo(KnownVersion.GEODE_1_11_0);
    assertThat(KnownVersion.GEODE_1_12_0.getClientServerProtocolVersion())
        .isEqualTo(KnownVersion.GEODE_1_12_0);
    assertThat(KnownVersion.GEODE_1_12_1.getClientServerProtocolVersion())
        .isEqualTo(KnownVersion.GEODE_1_12_1);
    assertThat(KnownVersion.GEODE_1_13_0.getClientServerProtocolVersion())
        .isEqualTo(KnownVersion.GEODE_1_13_0);
    assertThat(KnownVersion.GEODE_1_13_2.getClientServerProtocolVersion())
        .isEqualTo(KnownVersion.GEODE_1_13_2);
    assertThat(KnownVersion.GEODE_1_14_0.getClientServerProtocolVersion())
        .isEqualTo(KnownVersion.GEODE_1_14_0);
  }

  private void compare(KnownVersion later, KnownVersion earlier) {
    assertThat(later).isEqualTo(later);
    assertThat(later).isNotEqualTo(earlier);

    assertThat(later).isGreaterThan(earlier);
    assertThat(later).isEqualByComparingTo(later);
    assertThat(earlier).isLessThan(later);

    assertThat((Version) later).isGreaterThan(Versioning.getVersion(earlier.ordinal()));
    assertThat((Version) later).isEqualByComparingTo(Versioning.getVersion(later.ordinal()));
    assertThat((Version) earlier).isLessThan(Versioning.getVersion(later.ordinal()));

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
  public void testFromOrdinalForCurrentVersionSucceeds() {
    final KnownVersion version = Versioning.getKnownVersionOrDefault(
        Versioning.getVersion(KnownVersion.CURRENT_ORDINAL), null);
    assertThat(version).isNotNull();
    assertThat(version).isEqualTo(KnownVersion.CURRENT);
  }

}
