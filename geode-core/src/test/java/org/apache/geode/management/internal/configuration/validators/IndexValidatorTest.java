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

package org.apache.geode.management.internal.configuration.validators;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.management.configuration.Index;
import org.apache.geode.management.configuration.IndexType;
import org.apache.geode.management.internal.CacheElementOperation;

public class IndexValidatorTest {
  private IndexValidator indexValidator;
  private Index index;

  @Before
  public void init() {
    indexValidator = new IndexValidator();
    index = new Index();
    index.setName("testIndex");
    index.setExpression("testExpression");
    index.setRegionPath("testRegion");
  }

  @Test
  public void validate_happy() {
    indexValidator.validate(CacheElementOperation.CREATE, index);
  }

  @Test
  public void validate_missingExpression() {
    index.setExpression(null);

    assertThatThrownBy(() -> indexValidator.validate(CacheElementOperation.CREATE, index))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Expression is required");
  }

  @Test
  public void validate_hasRegionPath() {
    index.setRegionPath(null);

    assertThatThrownBy(() -> indexValidator.validate(CacheElementOperation.CREATE, index))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("RegionPath is required");
  }

  @Test
  public void validate_HashTypeNotAllowed() {
    index.setIndexType(IndexType.HASH_DEPRECATED);

    assertThatThrownBy(() -> indexValidator.validate(CacheElementOperation.CREATE, index))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("IndexType HASH is not allowed");
  }

  @Test
  public void validate_IndexHasName() {
    index.setName(null);

    assertThatThrownBy(() -> indexValidator.validate(CacheElementOperation.CREATE, index))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Name is required");
  }
}
