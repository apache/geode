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
package org.apache.geode.cache;

import static org.junit.Assert.fail;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.cache.LocalRegion;

public class RegionNameValidationJUnitTest {
  private static final Pattern NAME_PATTERN = Pattern.compile("[aA-zZ0-9-_.]+");
  private static final String REGION_NAME = "MyRegion";


  @Test
  public void testInvalidNames() {
    InternalRegionArguments ira = new InternalRegionArguments();
    ira.setInternalRegion(false);
    try {
      LocalRegion.validateRegionName(null, ira);
      fail();
    } catch (IllegalArgumentException ignore) {
    }
    try {
      LocalRegion.validateRegionName("", ira);
      fail();
    } catch (IllegalArgumentException ignore) {
    }
    try {
      LocalRegion.validateRegionName("FOO" + Region.SEPARATOR, ira);
      fail();
    } catch (IllegalArgumentException ignore) {
    }

  }

  @Test
  public void testExternalRegionNames() {
    InternalRegionArguments ira = new InternalRegionArguments();
    ira.setInternalRegion(false);
    validateCharacters(ira);
    try {
      LocalRegion.validateRegionName("__InvalidInternalRegionName", ira);
      fail();
    } catch (IllegalArgumentException ignore) {
    }
  }

  @Test
  public void testInternalRegionNames() {
    InternalRegionArguments ira = new InternalRegionArguments();
    ira.setInternalRegion(true);
    LocalRegion.validateRegionName("__ValidInternalRegionName", ira);
  }

  private void validateCharacters(InternalRegionArguments ira) {
    for (int x = 0; x < Character.MAX_VALUE; x++) {
      String name = (char) x + REGION_NAME;
      Matcher matcher = NAME_PATTERN.matcher(name);
      if (matcher.matches()) {
        LocalRegion.validateRegionName(name, ira);
      } else {
        try {
          LocalRegion.validateRegionName(name, ira);
          fail("Should have received an IllegalArgumentException for character: " + (char) x + "["
              + x + "]");
        } catch (IllegalArgumentException ignore) {
        }
      }
    }
  }
}
