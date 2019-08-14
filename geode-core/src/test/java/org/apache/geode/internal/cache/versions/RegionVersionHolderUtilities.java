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
package org.apache.geode.internal.cache.versions;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;

public class RegionVersionHolderUtilities {
  static void assertSameExceptions(List<RVVException> actualExceptions,
      RVVException[] exceptions) {
    List<RVVException> expectedExceptions = Arrays.asList(exceptions);
    assertThat(actualExceptions)
        .withFailMessage("Expected exceptions %s but got %s", expectedExceptions, actualExceptions)
        .hasSize(exceptions.length);

    for (int i = 0; i < exceptions.length; i++) {
      assertThat(actualExceptions.get(i).sameAs(expectedExceptions.get(i)))
          .withFailMessage("Expected exceptions %s but got %s", expectedExceptions,
              actualExceptions)
          .isTrue();
    }
  }
}
