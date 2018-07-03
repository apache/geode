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

package org.apache.geode.cache.query;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.OQLIndexTest;

@Category({OQLIndexTest.class})
public class IndexTypeTest {

  @Test
  public void toStringMaintainsOldBehavior() throws Exception {
    assertThat(IndexType.FUNCTIONAL.toString()).isEqualTo("FUNCTIONAL");
    assertThat(IndexType.HASH.toString()).isEqualTo("HASH");
    assertThat(IndexType.PRIMARY_KEY.toString()).isEqualTo("PRIMARY_KEY");
  }

  @Test
  public void getNameReturnsExpectedValue() throws Exception {
    assertThat(IndexType.FUNCTIONAL.getName()).isEqualTo("RANGE");
    assertThat(IndexType.HASH.getName()).isEqualTo("HASH");
    assertThat(IndexType.PRIMARY_KEY.getName()).isEqualTo("KEY");
  }
}
