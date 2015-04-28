/*=========================================================================
 * Copyright (c) 2005-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.cache.query.internal;

import java.util.ArrayList;
import java.util.Iterator;

import org.junit.experimental.categories.Category;

import junit.framework.TestCase;

import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.internal.types.StructTypeImpl;
import com.gemstone.gemfire.cache.query.internal.types.TypeUtils;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import com.gemstone.junit.UnitTest;

/**
 * @author Yogesh Mahajan
 *
 */
@Category(UnitTest.class)
public class OrderByComparatorJUnitTest extends TestCase {

  public OrderByComparatorJUnitTest(String testName) {
    super(testName);
  }

  public void testOredrByComparator() throws Exception {

    ObjectType types[] = { TypeUtils.OBJECT_TYPE, TypeUtils.OBJECT_TYPE };
    String names[] = { "pf", "pos" };
    StructTypeImpl structTypeImpl = new StructTypeImpl(names, types);

    SortedStructSet sss = new SortedStructSet(new OrderByComparator(structTypeImpl), structTypeImpl);

    String status[] = { "active", "inactive" };
    Boolean criteria[] = { new Boolean(false), new Boolean(true) };
    ArrayList list = null;
    for (int i = 0; i < 10; i++) {
      list = new ArrayList();
      Object[] arr = new Object[2];

      arr[0] = status[i % 1];
      arr[1] = criteria[i % 2];
      list.add(arr);

      Portfolio ptf = new Portfolio(i);
      Iterator pIter = ptf.positions.values().iterator();
      while (pIter.hasNext()) {
        Object values[] = { ptf, pIter.next() };
        // StructImpl s = new StructImpl(structTypeImpl, values);
        ((OrderByComparator) sss.comparator()).orderByMap.put(values, list);
        sss.addFieldValues(values);
      }
    }
  }
}
