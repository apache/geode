/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.control;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashSet;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.junit.UnitTest;

import junit.framework.TestCase;

/**
 * @author dsmith
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
