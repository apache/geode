/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.web.domain;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.gemstone.gemfire.internal.util.CollectionUtils;
import com.gemstone.gemfire.management.internal.web.AbstractWebTestCase;
import com.gemstone.gemfire.management.internal.web.http.HttpMethod;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * The LinkJUnitTest class is a test suite of test cases testing the contract and functionality of the Link class.
 * <p/>
 * @see java.net.URI
 * @see com.gemstone.gemfire.management.internal.web.AbstractWebTestCase
 * @see com.gemstone.gemfire.management.internal.web.domain.Link
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since GemFire 8.0
 */
@Category(UnitTest.class)
public class LinkJUnitTest extends AbstractWebTestCase {

  @Test
  public void testConstructDefaultLink() {
    final Link link = new Link();

    assertNotNull(link);
    assertNull(link.getHref());
    assertNull(link.getMethod());
    assertNull(link.getRelation());
  }

  @Test
  public void testConstructLinkWithRelationAndHref() throws Exception {
    final Link link = new Link("get-resource", toUri("http://host:port/service/v1/resources/{id}"));

    assertNotNull(link);
    assertEquals("http://host:port/service/v1/resources/{id}", toString(link.getHref()));
    assertEquals(HttpMethod.GET, link.getMethod());
    assertEquals("get-resource", link.getRelation());
  }

  @Test
  public void testConstructLinkWithRelationHrefAndMethod() throws Exception {
    final Link link = new Link("create-resource", toUri("http://host:port/service/v1/resources"), HttpMethod.POST);

    assertNotNull(link);
    assertEquals("http://host:port/service/v1/resources", toString(link.getHref()));
    assertEquals(HttpMethod.POST, link.getMethod());
    assertEquals("create-resource", link.getRelation());
  }

  @Test
  public void testSetAndGetMethod() {
    final Link link = new Link();

    assertNotNull(link);
    assertNull(link.getMethod());

    link.setMethod(HttpMethod.POST);

    assertEquals(HttpMethod.POST, link.getMethod());

    link.setMethod(null);

    assertEquals(HttpMethod.GET, link.getMethod());
  }

  @Test
  public void testCompareTo() throws Exception {
    final Link link0 = new Link("resources", toUri("http://host:port/service/v1/resources"));
    final Link link1 = new Link("resource", toUri("http://host:port/service/v1/resources"), HttpMethod.POST);
    final Link link2 = new Link("resource", toUri("http://host:port/service/v1/resources/{id}"));
    final Link link3 = new Link("resource", toUri("http://host:port/service/v1/resources/{name}"));
    final Link link4 = new Link("resource", toUri("http://host:port/service/v1/resources/{id}"), HttpMethod.DELETE);

    final List<Link> expectedList = new ArrayList<Link>(Arrays.asList(link1, link4, link2, link3, link0));

    final List<Link> actualList = CollectionUtils.asList(link0, link1, link2, link3, link4);

    Collections.sort(actualList);

    System.out.println(toString(expectedList.toArray(new Link[expectedList.size()])));
    System.out.println(toString(actualList.toArray(new Link[actualList.size()])));

    assertEquals(expectedList, actualList);
  }

  @Test
  public void testToHttpRequestLine() throws Exception {
    final Link link = new Link("get-resource", toUri("http://host.domain.com:port/service/v1/resources/{id}"));

    assertNotNull(link);
    assertEquals(HttpMethod.GET, link.getMethod());
    assertEquals("http://host.domain.com:port/service/v1/resources/{id}", toString(link.getHref()));
    assertEquals("GET ".concat("http://host.domain.com:port/service/v1/resources/{id}"), link.toHttpRequestLine());
  }

}
