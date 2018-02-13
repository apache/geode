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
package org.apache.geode.internal.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.extension.ExtensionPoint;
import org.apache.geode.internal.cache.extension.SimpleExtensionPoint;
import org.apache.geode.test.junit.categories.UnitTest;

/**
 * Unit tests for {@link AbstractRegion}.
 *
 * @since GemFire 8.1
 */
@Category(UnitTest.class)
public class AbstractRegionJUnitTest {

  /**
   * Test method for {@link AbstractRegion#getExtensionPoint()}.
   *
   * Assert that method returns a {@link SimpleExtensionPoint} instance and assume that
   * {@link org.apache.geode.internal.cache.extension.SimpleExtensionPointJUnitTest} has covered the
   * rest.
   */
  @Test

  public void extensionPointIsSimpleExtensionPointByDefault() {
    AbstractRegion region = spy(AbstractRegion.class);
    ExtensionPoint<Region<?, ?>> extensionPoint = region.getExtensionPoint();
    assertThat(extensionPoint).isNotNull().isInstanceOf(SimpleExtensionPoint.class);
  }
}
