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
package org.apache.geode.internal.cache.control;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashSet;

import org.junit.Test;

import org.apache.geode.cache.Region;

public class FilterByPathJUnitTest {

  @Test
  public void testDefault() {
    FilterByPath filter = new FilterByPath(null, null);
    assertTrue(filter.include(createRegion("a")));
    assertTrue(filter.include(createRegion("b")));
    assertTrue(filter.include(createRegion("c")));
  }

  @Test
  public void testInclude() {
    HashSet<String> included = new HashSet<String>();
    included.add("a");
    included.add("b");
    FilterByPath filter = new FilterByPath(included, null);
    assertTrue(filter.include(createRegion("a")));
    assertTrue(filter.include(createRegion("b")));
    assertFalse(filter.include(createRegion("c")));
  }

  @Test
  public void testExclude() {
    HashSet<String> excluded = new HashSet<String>();
    excluded.add("a");
    excluded.add("b");
    FilterByPath filter = new FilterByPath(null, excluded);
    assertFalse(filter.include(createRegion("a")));
    assertFalse(filter.include(createRegion("b")));
    assertTrue(filter.include(createRegion("c")));
  }

  @Test
  public void testBoth() {
    HashSet<String> included = new HashSet<String>();
    included.add("a");
    included.add("b");
    HashSet<String> excluded = new HashSet<String>();
    excluded.add("a");
    excluded.add("b");
    FilterByPath filter = new FilterByPath(included, excluded);
    assertTrue(filter.include(createRegion("a")));
    assertTrue(filter.include(createRegion("b")));
    assertFalse(filter.include(createRegion("c")));
  }

  private Region<?, ?> createRegion(String name) {
    RegionHandler handler = new RegionHandler(name);
    final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    return (Region<?, ?>) Proxy.newProxyInstance(contextClassLoader, new Class[] {Region.class},
        handler);
  }

  private static class RegionHandler implements InvocationHandler {

    private final String name;

    public RegionHandler(String name) {
      this.name = SEPARATOR + name;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      return name;
    }
  }
}
