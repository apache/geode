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
package org.apache.geode.management.internal;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.apache.geode.management.internal.OpenTypeConverter.CompositeBuilderViaProxy;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.management.openmbean.CompositeData;

@Category(UnitTest.class)
public class CompositeBuilderViaProxyTest {

  @Test
  public void shouldBeMockable() throws Exception {
    CompositeBuilderViaProxy mockCompositeBuilderViaProxy = mock(CompositeBuilderViaProxy.class);
    CompositeData compositeData = null;
    String[] itemNames = new String[1];
    OpenTypeConverter[] converters = new OpenTypeConverter[1];
    Object result = new Object();

    when(mockCompositeBuilderViaProxy.fromCompositeData(eq(compositeData), eq(itemNames),
        eq(converters))).thenReturn(result);

    assertThat(mockCompositeBuilderViaProxy.fromCompositeData(compositeData, itemNames, converters))
        .isSameAs(result);
  }
}
