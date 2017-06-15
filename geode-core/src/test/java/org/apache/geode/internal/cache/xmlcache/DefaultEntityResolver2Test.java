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
package org.apache.geode.internal.cache.xmlcache;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.xml.sax.InputSource;

@Category(UnitTest.class)
public class DefaultEntityResolver2Test {

  @Test
  public void shouldBeMockable() throws Exception {
    DefaultEntityResolver2 mockDefaultEntityResolver2 = mock(DefaultEntityResolver2.class);
    InputSource inputSource = new InputSource();

    when(mockDefaultEntityResolver2.getClassPathInputSource(eq("publicId"), eq("systemId"),
        eq("path"))).thenReturn(inputSource);

    assertThat(mockDefaultEntityResolver2.getClassPathInputSource("publicId", "systemId", "path"))
        .isSameAs(inputSource);
  }
}
