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
package org.apache.geode.management.internal.web.domain;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.geode.management.internal.web.http.HttpMethod;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.URI;

@Category(UnitTest.class)
public class LinkTest {

  @Test
  public void shouldBeMockable() throws Exception {
    Link mockLink = mock(Link.class);
    URI href = null;
    HttpMethod method = HttpMethod.CONNECT;
    String relation = "";

    mockLink.setHref(href);
    mockLink.setMethod(method);
    mockLink.setRelation(relation);

    verify(mockLink, times(1)).setHref(href);
    verify(mockLink, times(1)).setMethod(method);
    verify(mockLink, times(1)).setRelation(relation);
  }
}
