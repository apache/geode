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

package org.apache.geode.cache.lucene.internal;

import static org.apache.geode.cache.lucene.internal.CreateLuceneCommandParametersValidator.validateLuceneIndexName;
import static org.apache.geode.cache.lucene.internal.CreateLuceneCommandParametersValidator.validateRegionName;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.LuceneTest;


@Category({LuceneTest.class})
public class ValidateCommandParametersTest {

  @Test
  public void validateVariousVariationsOfRegionName() throws Exception {
    validateRegionName("/test");
    validateRegionName("test");
    validateRegionName("te/st");
    validateRegionName("te-st");
    validateRegionName("_test");
    validateRegionName("/_test");
    validateRegionName("/_tes/t");
    assertThatThrownBy(() -> validateRegionName("/__test"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> validateRegionName("__#@T"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> validateRegionName("__#@T"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> validateRegionName("/__#@T"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> validateRegionName("__")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> validateRegionName("/__"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> validateRegionName("")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> validateRegionName(null)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> validateRegionName(" ")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> validateRegionName("@#$%"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void validateVariousVariationsOfIndexName() throws Exception {
    assertThatThrownBy(() -> validateLuceneIndexName("/test"))
        .isInstanceOf(IllegalArgumentException.class);
    validateLuceneIndexName("test");
    validateLuceneIndexName("_test");
    validateLuceneIndexName("te-st");
    validateLuceneIndexName("_te-st");
    assertThatThrownBy(() -> validateLuceneIndexName("te/st"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> validateLuceneIndexName("__test"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> validateLuceneIndexName("__#@T"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> validateLuceneIndexName("/__#@T"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> validateLuceneIndexName("__"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> validateLuceneIndexName("/__"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> validateLuceneIndexName(""))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> validateLuceneIndexName(null))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> validateLuceneIndexName(" "))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> validateLuceneIndexName("@#$%"))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
