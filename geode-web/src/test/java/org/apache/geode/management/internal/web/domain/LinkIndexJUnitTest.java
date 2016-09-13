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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.internal.util.CollectionUtils;
import com.gemstone.gemfire.management.internal.web.AbstractWebTestCase;
import com.gemstone.gemfire.management.internal.web.http.HttpMethod;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * The LinkIndexJUnitTest class is a test suite of test cases testing the contract and functionality of the LinkIndex class.
 * <p/>
 * @see java.net.URI
 * @see com.gemstone.gemfire.management.internal.web.AbstractWebTestCase
 * @see com.gemstone.gemfire.management.internal.web.domain.LinkIndex
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since GemFire 8.0
 */
@Category(UnitTest.class)
public class LinkIndexJUnitTest extends AbstractWebTestCase {

  @Test
  public void testAdd() throws Exception {
    final Link link = new Link("get-resource", toUri("http://host.domain.com:port/service/v1/resources/{id}"));

    final LinkIndex linkIndex = new LinkIndex();

    assertTrue(linkIndex.isEmpty());
    assertEquals(0, linkIndex.size());
    assertEquals(linkIndex, linkIndex.add(link));
    assertFalse(linkIndex.isEmpty());
    assertEquals(1, linkIndex.size());
    assertEquals(linkIndex, linkIndex.add(link)); // test duplicate addition
    assertFalse(linkIndex.isEmpty());
    assertEquals(1, linkIndex.size());
  }

  @Test(expected = AssertionError.class)
  public void testAddNullLink() {
    final LinkIndex linkIndex = new LinkIndex();

    assertTrue(linkIndex.isEmpty());

    try {
      linkIndex.add(null);
    }
    finally {
      assertTrue(linkIndex.isEmpty());
    }
  }

  @Test
  public void testAddAll() throws Exception {
    final Link create = new Link("create-resource", toUri("http://host.domain.com:port/service/v1/resources"), HttpMethod.POST);
    final Link retrieve = new Link("get-resource", toUri("http://host.domain.com:port/service/v1/resources/{id}"));
    final Link update = new Link("update-resource", toUri("http://host.domain.com:port/service/v1/resources"), HttpMethod.PUT);
    final Link delete = new Link("delete-resource", toUri("http://host.domain.com:port/service/v1/resources/{id}"), HttpMethod.DELETE);

    final LinkIndex linkIndex = new LinkIndex();

    assertTrue(linkIndex.isEmpty());
    assertEquals(linkIndex, linkIndex.addAll(create, retrieve, update, delete));
    assertFalse(linkIndex.isEmpty());
    assertEquals(4, linkIndex.size());
  }

  @Test(expected = AssertionError.class)
  public void testAddAllWithNullLinks() {
    final LinkIndex linkIndex = new LinkIndex();

    assertTrue(linkIndex.isEmpty());

    try {
      linkIndex.addAll((Iterable<Link>) null);
    }
    finally {
      assertTrue(linkIndex.isEmpty());
    }
  }

  @Test
  public void testFind() throws Exception {
    final Link list = new Link("get-resources", toUri("http://host.domain.com:port/service/v1/resources"));
    final Link create = new Link("create-resource", toUri("http://host.domain.com:port/service/v1/resources"), HttpMethod.POST);
    final Link retrieve = new Link("get-resource", toUri("http://host.domain.com:port/service/v1/resources/{id}"));
    final Link update = new Link("update-resource", toUri("http://host.domain.com:port/service/v1/resources"), HttpMethod.PUT);
    final Link delete = new Link("delete-resource", toUri("http://host.domain.com:port/service/v1/resources/{id}"), HttpMethod.DELETE);

    final LinkIndex linkIndex = new LinkIndex();

    assertTrue(linkIndex.isEmpty());
    assertEquals(linkIndex, linkIndex.addAll(list, create, retrieve, update, delete));
    assertFalse(linkIndex.isEmpty());
    assertEquals(5, linkIndex.size());
    assertEquals(list, linkIndex.find("get-resources"));
    assertEquals(retrieve, linkIndex.find("get-resource"));
    assertEquals(update, linkIndex.find("UPDATE-RESOURCE"));
    assertEquals(delete, linkIndex.find("Delete-Resource"));
    assertNull(linkIndex.find("destroy-resource"));
    assertNull(linkIndex.find("save-resource"));
  }

  @Test
  public void testFindAll() throws Exception {
    final Link list = new Link("get-resources", toUri("http://host.domain.com:port/service/v1/resources"));
    final Link create = new Link("create-resource", toUri("http://host.domain.com:port/service/v1/resources"), HttpMethod.POST);
    final Link retrieveById = new Link("get-resource", toUri("http://host.domain.com:port/service/v1/resources/{id}"));
    final Link retrieveByName = new Link("get-resource", toUri("http://host.domain.com:port/service/v1/resources/{name}"));
    final Link update = new Link("update-resource", toUri("http://host.domain.com:port/service/v1/resources"), HttpMethod.PUT);
    final Link delete = new Link("delete-resource", toUri("http://host.domain.com:port/service/v1/resources/{id}"), HttpMethod.DELETE);

    final LinkIndex linkIndex = new LinkIndex();

    assertTrue(linkIndex.isEmpty());
    assertEquals(linkIndex, linkIndex.addAll(list, create, retrieveById, retrieveByName, update, delete));
    assertFalse(linkIndex.isEmpty());
    assertEquals(6, linkIndex.size());

    final Link[] retrieveLinks = linkIndex.findAll("get-resource");

    assertNotNull(retrieveLinks);
    assertEquals(2, retrieveLinks.length);
    assertTrue(Arrays.asList(retrieveLinks).containsAll(Arrays.asList(retrieveById, retrieveByName)));

    final Link[] saveLinks = linkIndex.findAll("save-resource");

    assertNotNull(saveLinks);
    assertEquals(0, saveLinks.length);
  }

  @Test
  public void testIterator() throws Exception {
    final Link list = new Link("get-resources", toUri("http://host.domain.com:port/service/v1/resources"));
    final Link create = new Link("create-resource", toUri("http://host.domain.com:port/service/v1/resources"), HttpMethod.POST);
    final Link retrieveById = new Link("get-resource", toUri("http://host.domain.com:port/service/v1/resources/{id}"));
    final Link retrieveByName = new Link("get-resource", toUri("http://host.domain.com:port/service/v1/resources/{name}"));
    final Link update = new Link("update-resource", toUri("http://host.domain.com:port/service/v1/resources"), HttpMethod.PUT);
    final Link delete = new Link("delete-resource", toUri("http://host.domain.com:port/service/v1/resources/{id}"), HttpMethod.DELETE);

    final LinkIndex linkIndex = new LinkIndex();

    assertTrue(linkIndex.isEmpty());
    assertEquals(linkIndex, linkIndex.addAll(list, create, retrieveById, retrieveByName, update, delete));
    assertFalse(linkIndex.isEmpty());
    assertEquals(6, linkIndex.size());

    final Collection<Link> expectedLinks = Arrays.asList(list, create, retrieveById, retrieveByName, update, delete);

    final Collection<Link> actualLinks = new ArrayList<Link>(linkIndex.size());

    for (final Link link : linkIndex) {
      actualLinks.add(link);
    }

    assertTrue(actualLinks.containsAll(expectedLinks));
  }

  @Test
  public void testToList() throws Exception {
    final Link list = new Link("get-resources", toUri("http://host.domain.com:port/service/v1/resources"));
    final Link create = new Link("create-resource", toUri("http://host.domain.com:port/service/v1/resources"), HttpMethod.POST);
    final Link retrieveById = new Link("get-resource", toUri("http://host.domain.com:port/service/v1/resources/{id}"));
    final Link retrieveByName = new Link("get-resource", toUri("http://host.domain.com:port/service/v1/resources/{name}"));
    final Link update = new Link("update-resource", toUri("http://host.domain.com:port/service/v1/resources"), HttpMethod.PUT);
    final Link delete = new Link("delete-resource", toUri("http://host.domain.com:port/service/v1/resources/{id}"), HttpMethod.DELETE);

    final LinkIndex linkIndex = new LinkIndex();

    assertTrue(linkIndex.isEmpty());
    assertEquals(linkIndex, linkIndex.addAll(list, create, retrieveById, retrieveByName, update, delete));
    assertFalse(linkIndex.isEmpty());
    assertEquals(6, linkIndex.size());

    final List<Link> expectedList = CollectionUtils.asList(list, create, retrieveById, retrieveByName, update, delete);

    Collections.sort(expectedList);

    assertEquals(expectedList, linkIndex.toList());
  }

  @Test
  public void testToMap() throws Exception {
    final Link list = new Link("get-resources", toUri("http://host.domain.com:port/service/v1/resources"));
    final Link create = new Link("create-resource", toUri("http://host.domain.com:port/service/v1/resources"), HttpMethod.POST);
    final Link retrieveById = new Link("get-resource", toUri("http://host.domain.com:port/service/v1/resources/{id}"));
    final Link retrieveByName = new Link("get-resource", toUri("http://host.domain.com:port/service/v1/resources/{name}"));
    final Link update = new Link("update-resource", toUri("http://host.domain.com:port/service/v1/resources"), HttpMethod.PUT);
    final Link delete = new Link("delete-resource", toUri("http://host.domain.com:port/service/v1/resources/{id}"), HttpMethod.DELETE);

    final LinkIndex linkIndex = new LinkIndex();

    assertTrue(linkIndex.isEmpty());
    assertEquals(linkIndex, linkIndex.addAll(list, create, retrieveById, retrieveByName, update, delete));
    assertFalse(linkIndex.isEmpty());
    assertEquals(6, linkIndex.size());

    final Map<String, List<Link>> expectedMap = new HashMap<String, List<Link>>(5);

    expectedMap.put("get-resources", Arrays.asList(list));
    expectedMap.put("create-resource", Arrays.asList(create));
    expectedMap.put("get-resource", Arrays.asList(retrieveById, retrieveByName));
    expectedMap.put("update-resource", Arrays.asList(update));
    expectedMap.put("delete-resource", Arrays.asList(delete));

    assertEquals(expectedMap, linkIndex.toMap());
  }

}
