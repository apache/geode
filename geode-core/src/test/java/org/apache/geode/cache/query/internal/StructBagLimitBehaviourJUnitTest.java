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
package org.apache.geode.cache.query.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.Test;

import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.internal.types.ObjectTypeImpl;
import org.apache.geode.cache.query.internal.types.StructTypeImpl;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.cache.query.types.StructType;

/**
 * Test StructBag Limit behaviour
 */
public class StructBagLimitBehaviourJUnitTest extends ResultsBagLimitBehaviourJUnitTest {

  public ResultsBag getBagObject(Class clazz) {
    ObjectType[] types = new ObjectType[] {new ObjectTypeImpl(clazz), new ObjectTypeImpl(clazz)};
    StructType type = new StructTypeImpl(new String[] {"field1", "field2"}, types);
    return new StructBag(type, null);
  }

  public Object wrap(Object obj, ObjectType type) {
    StructTypeImpl stype = (StructTypeImpl) type;
    if (obj == null) {
      return new StructImpl(stype, null);
    } else {
      return new StructImpl(stype, new Object[] {obj, obj});
    }
  }

  @Test
  public void testRemoveAllStructBagSpecificMthod() {
    StructBag bag1 = (StructBag) getBagObject(Integer.class);
    // Add Integer & null Objects
    bag1.add(wrap(null, bag1.getCollectionType().getElementType()));
    bag1.add(wrap(null, bag1.getCollectionType().getElementType()));
    bag1.add(wrap(1, bag1.getCollectionType().getElementType()));
    bag1.add(wrap(2, bag1.getCollectionType().getElementType()));
    bag1.add(wrap(2, bag1.getCollectionType().getElementType()));
    bag1.add(wrap(3, bag1.getCollectionType().getElementType()));
    bag1.add(wrap(3, bag1.getCollectionType().getElementType()));
    bag1.add(wrap(4, bag1.getCollectionType().getElementType()));
    bag1.applyLimit(4);
    StructBag bag2 = (StructBag) getBagObject(Integer.class);
    bag2.addAll((StructFields) bag1);
    // Now remove the first element & it occurnece completelt from bag2
    Iterator itr2 = bag2.iterator();
    Struct first = (Struct) itr2.next();
    int occrnce = 0;
    while (itr2.hasNext()) {
      if (itr2.next().equals(first)) {
        itr2.remove();
        ++occrnce;
      }
    }
    assertTrue(bag1.removeAll((StructFields) bag2));
    assertEquals(occrnce, bag1.size());
    Iterator itr = bag1.iterator();
    for (int i = 0; i < occrnce; ++i) {
      itr.next();
    }
    assertFalse(itr.hasNext());
  }

  @Test
  public void testRetainAllStructBagSpecific() {
    StructBag bag1 = (StructBag) getBagObject(Integer.class);
    // Add Integer & null Objects
    // Add Integer & null Objects
    bag1.add(wrap(1, bag1.getCollectionType().getElementType()));
    bag1.add(wrap(2, bag1.getCollectionType().getElementType()));
    bag1.add(wrap(2, bag1.getCollectionType().getElementType()));
    bag1.add(wrap(3, bag1.getCollectionType().getElementType()));
    bag1.add(wrap(3, bag1.getCollectionType().getElementType()));
    bag1.add(wrap(4, bag1.getCollectionType().getElementType()));
    bag1.add(wrap(null, bag1.getCollectionType().getElementType()));
    bag1.add(wrap(null, bag1.getCollectionType().getElementType()));
    bag1.applyLimit(4);
    StructBag bag2 = (StructBag) getBagObject(Integer.class);
    bag2.addAll((StructFields) bag1);
    // Now remove the first element & it occurnece completelt from bag2
    Iterator itr2 = bag2.iterator();
    Struct first = (Struct) itr2.next();
    int occrnce = 0;
    while (itr2.hasNext()) {
      if (itr2.next().equals(first)) {
        itr2.remove();
        ++occrnce;
      }
    }
    bag1.retainAll((StructFields) bag2);
    assertEquals(4, bag1.size());
    Iterator itr = bag1.iterator();
    for (int i = 0; i < 4; ++i) {
      itr.next();
    }
    assertFalse(itr.hasNext());
  }

}
