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
package org.apache.geode.cache.util;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import org.apache.geode.cache.EntryOperation;
import org.apache.geode.internal.cache.EntryOperationImpl;

public class StringPrefixPartitionResolverJUnitTest {
  static final String DELIMITER = StringPrefixPartitionResolver.DEFAULT_DELIMITER;

  @Test
  public void testGetName() {
    assertEquals("org.apache.geode.cache.util.StringPrefixPartitionResolver",
        (new StringPrefixPartitionResolver()).getName());
  }

  @Test
  public void testEquals() {
    StringPrefixPartitionResolver pr1 = new StringPrefixPartitionResolver();
    assertEquals(true, pr1.equals(pr1));
    StringPrefixPartitionResolver pr2 = new StringPrefixPartitionResolver();
    assertEquals(true, pr1.equals(pr2));
    assertEquals(false, pr1.equals(new Object()));
  }

  @Test
  public void testNonStringKey() {
    Object key = new Object();
    StringPrefixPartitionResolver pr = new StringPrefixPartitionResolver();
    assertThatThrownBy(() -> pr.getRoutingObject(createEntryOperation(key)))
        .isInstanceOf(ClassCastException.class);
  }

  @Test
  public void testNoDelimiterKey() {
    String key = "foobar";
    StringPrefixPartitionResolver pr = new StringPrefixPartitionResolver();
    assertThatThrownBy(() -> pr.getRoutingObject(createEntryOperation(key)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The key \"foobar\" does not contains the \"" + DELIMITER + "\" delimiter.");
  }

  @Test
  public void testEmptyPrefix() {
    String key = DELIMITER + "foobar";
    StringPrefixPartitionResolver pr = new StringPrefixPartitionResolver();
    assertEquals("", pr.getRoutingObject(createEntryOperation(key)));
  }

  @Test
  public void testAllPrefix() {
    String key = "foobar" + DELIMITER;
    StringPrefixPartitionResolver pr = new StringPrefixPartitionResolver();
    assertEquals("foobar", pr.getRoutingObject(createEntryOperation(key)));
  }

  @Test
  public void testSimpleKey() {
    String key = "1" + DELIMITER + "2";
    StringPrefixPartitionResolver pr = new StringPrefixPartitionResolver();
    assertEquals("1", pr.getRoutingObject(createEntryOperation(key)));
  }

  @Test
  public void testMulitPrefix() {
    String key = "one" + DELIMITER + "two" + DELIMITER + "three";
    StringPrefixPartitionResolver pr = new StringPrefixPartitionResolver();
    assertEquals("one", pr.getRoutingObject(createEntryOperation(key)));
  }

  @SuppressWarnings("unchecked")
  private EntryOperation<String, Object> createEntryOperation(Object key) {
    return new EntryOperationImpl(null, null, key, null, null);
  }
}
