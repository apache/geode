/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.cache.query.internal;

import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import com.gemstone.gemfire.cache.query.types.StructType;
import com.gemstone.gemfire.cache.query.internal.types.*;
import com.gemstone.junit.UnitTest;

import java.util.Iterator;

import org.junit.experimental.categories.Category;

/**
 * Test StructsBag Limit behaviour
 * 
 * @author Asif
 */
@Category(UnitTest.class)
public class StructBagLimitBehaviourJUnitTest extends ResultsBagLimitBehaviourJUnitTest {

  public StructBagLimitBehaviourJUnitTest(String testName) {
    super(testName);
  }

  public ResultsBag getBagObject(Class clazz) {
    ObjectType[] types = new ObjectType[] { new ObjectTypeImpl(clazz),
        new ObjectTypeImpl(clazz) };
    StructType type = new StructTypeImpl(new String[] { "field1", "field2" },
        types);
    return new StructBag(type, null);
  }

  public Object wrap(Object obj, ObjectType type) {
    StructTypeImpl stype = (StructTypeImpl)type;
    if (obj == null) {
      return new StructImpl(stype, null);
    }
    else {
      return new StructImpl(stype, new Object[] { obj, obj });
    }
  }

  public void testRemoveAllStructBagSpecificMthod() {
    StructBag bag1 = (StructBag)getBagObject(Integer.class);
    // Add Integer & null Objects
    bag1.add(wrap(null, bag1.getCollectionType().getElementType()));
    bag1.add(wrap(null, bag1.getCollectionType().getElementType()));
    bag1.add(wrap(new Integer(1), bag1.getCollectionType().getElementType()));
    bag1.add(wrap(new Integer(2), bag1.getCollectionType().getElementType()));
    bag1.add(wrap(new Integer(2), bag1.getCollectionType().getElementType()));
    bag1.add(wrap(new Integer(3), bag1.getCollectionType().getElementType()));
    bag1.add(wrap(new Integer(3), bag1.getCollectionType().getElementType()));
    bag1.add(wrap(new Integer(4), bag1.getCollectionType().getElementType()));
    bag1.applyLimit(4);
    StructBag bag2 = (StructBag)getBagObject(Integer.class);
    bag2.addAll(bag1);
    // Now remove the first element & it occurnece completelt from bag2
    Iterator itr2 = bag2.iterator();
    Struct first = (Struct)itr2.next();
    int occrnce = 0;
    while (itr2.hasNext()) {
      if (itr2.next().equals(first)) {
        itr2.remove();
        ++occrnce;
      }
    }
    assertTrue(bag1.removeAll(bag2));
    assertEquals(occrnce, bag1.size());
    Iterator itr = bag1.iterator();
    for (int i = 0; i < occrnce; ++i) {
      itr.next();
    }
    assertFalse(itr.hasNext());
  }

  public void testRetainAllStructBagSpecific() {
    StructBag bag1 = (StructBag)getBagObject(Integer.class);
    // Add Integer & null Objects
    // Add Integer & null Objects
    bag1.add(wrap(new Integer(1), bag1.getCollectionType().getElementType()));
    bag1.add(wrap(new Integer(2), bag1.getCollectionType().getElementType()));
    bag1.add(wrap(new Integer(2), bag1.getCollectionType().getElementType()));
    bag1.add(wrap(new Integer(3), bag1.getCollectionType().getElementType()));
    bag1.add(wrap(new Integer(3), bag1.getCollectionType().getElementType()));
    bag1.add(wrap(new Integer(4), bag1.getCollectionType().getElementType()));
    bag1.add(wrap(null, bag1.getCollectionType().getElementType()));
    bag1.add(wrap(null, bag1.getCollectionType().getElementType()));
    bag1.applyLimit(4);
    StructBag bag2 = (StructBag)getBagObject(Integer.class);
    bag2.addAll(bag1);
    // Now remove the first element & it occurnece completelt from bag2
    Iterator itr2 = bag2.iterator();
    Struct first = (Struct)itr2.next();
    int occrnce = 0;
    while (itr2.hasNext()) {
      if (itr2.next().equals(first)) {
        itr2.remove();
        ++occrnce;
      }
    }
    bag1.retainAll(bag2);
    assertEquals(4, bag1.size());
    Iterator itr = bag1.iterator();
    for (int i = 0; i < 4; ++i) {
      itr.next();
    }
    assertFalse(itr.hasNext());
  }

}
