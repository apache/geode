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
package com.gemstone.gemfire.internal.cache.control;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashSet;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

import junit.framework.TestCase;

/**
 *
 */
@Category(UnitTest.class)
public class FilterByPathJUnitTest extends TestCase {
  public void testDefault() {
    FilterByPath filter = new FilterByPath(null, null);
    assertTrue(filter.include(createRegion("a")));
    assertTrue(filter.include(createRegion("b")));
    assertTrue(filter.include(createRegion("c")));
  }
  
  public void testInclude() {
    HashSet<String> included = new HashSet<String>();
    included.add("a");
    included.add("b");
    FilterByPath filter = new FilterByPath(included, null);
    assertTrue(filter.include(createRegion("a")));
    assertTrue(filter.include(createRegion("b")));
    assertFalse(filter.include(createRegion("c")));
  }

  public void testExclude() {
    HashSet<String> excluded = new HashSet<String>();
    excluded.add("a");
    excluded.add("b");
    FilterByPath filter = new FilterByPath(null, excluded);
    assertFalse(filter.include(createRegion("a")));
    assertFalse(filter.include(createRegion("b")));
    assertTrue(filter.include(createRegion("c")));
  }
  
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
    return (Region<?,?>) Proxy.newProxyInstance(contextClassLoader, new Class[] {Region.class}, handler);
  }

  public static class RegionHandler implements InvocationHandler {

    private String name;
    
    public RegionHandler(String name) {
      this.name = "/"+name;
    }
    public Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable {
      return name;
    }
    
  }
}
