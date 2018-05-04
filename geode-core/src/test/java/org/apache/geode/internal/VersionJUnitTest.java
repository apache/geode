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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.UnsupportedVersionException;
import org.apache.geode.internal.cache.tier.sockets.CommandInitializer;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
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
    compare(Version.GEODE_110, Version.GFE_82);
    compare(Version.GEODE_120, Version.GEODE_111);
    compare(Version.GEODE_130, Version.GEODE_120);
    compare(Version.GEODE_140, Version.GEODE_130);
    compare(Version.GEODE_150, Version.GEODE_140);
  }

  private void compare(Version later, Version earlier) {
    assertTrue(later.compareTo(earlier) > 0);
    assertTrue(later.equals(later));
    assertTrue(later.compareTo(later) == 0);
    assertTrue(earlier.compareTo(later) < 0);

    assertTrue(later.compareTo(earlier.ordinal()) > 0);
    assertTrue(later.compareTo(later.ordinal()) == 0);
    assertTrue(earlier.compareTo(later.ordinal()) < 0);
  }

  @Test
  public void testIsPre65() {
    assertTrue(Version.GFE_61.compareTo(Version.GFE_65) < 0);
    assertFalse(Version.GFE_65.compareTo(Version.GFE_65) < 0);
    assertFalse(Version.GFE_70.compareTo(Version.GFE_65) < 0);
  }

  @Test
  public void testCommandMapContainsAllVersions() {
    for (Version version : Version.getAllVersions()) {
      org.junit.Assert.assertNotNull(
          "Please add a commnd set for " + version + " of Geode to CommandInitializer",
          CommandInitializer.getCommands(version));
    }
  }

  @Test
  public void testFromOrdinalForCurrentVersionSucceeds() throws UnsupportedVersionException {
    Version.fromOrdinal(Version.CURRENT_ORDINAL, true);
  }
}
