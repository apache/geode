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
package com.gemstone.gemfire.internal.size;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.test.junit.categories.UnitTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.*;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@Category(UnitTest.class)
public class ObjectTraverserJUnitTest {

  @Test
  public void testBasic() throws Exception {
    Set testData = new HashSet();
    Object one = new Object();
    testData.add(one);
    Object[] two = new Object[2];
    testData.add(two);
    ArrayList three = new ArrayList();
    two[0]= three;
    three.add(testData);
    
    TestVisitor visitor = new TestVisitor();
    ObjectTraverser.breadthFirstSearch(testData, visitor, false);
    
    assertNotNull(visitor.visited.remove(testData));
    assertNotNull(visitor.visited.remove(one));
    assertNotNull(visitor.visited.remove(two));
    assertNotNull(visitor.visited.remove(three));
  }

  @Test
  public void testStatics() throws Exception {
    final Object staticObject = new Object();
    TestObject1.test2 = staticObject;
    TestObject1 test1 = new TestObject1();
    
    TestVisitor visitor = new TestVisitor();
    ObjectTraverser.breadthFirstSearch(test1, visitor, false);
    assertNull(visitor.visited.get(staticObject));
    
    visitor = new TestVisitor();
    ObjectTraverser.breadthFirstSearch(test1, visitor, true);
    assertNotNull(visitor.visited.get(staticObject));
  }

  @Test
  public void testStop() throws Exception {
    Set set1 = new HashSet();
    final Set set2 = new HashSet();
    Object object3 = new Object();
    set1.add(set2);
    set2.add(object3);
    
    TestVisitor visitor = new TestVisitor();
    visitor = new TestVisitor() {
      public boolean visit(Object parent, Object object) {
        super.visit(parent, object);
        return object != set2;
      }
    };
    
    ObjectTraverser.breadthFirstSearch(set1, visitor, true);
    
    assertNotNull(visitor.visited.get(set1));
    assertNotNull(visitor.visited.get(set2));
    assertNull(visitor.visited.get(object3));
  }
  
  /** This test is commented out because it needs to be verified manually */
  @Ignore("commented out because it needs to be verified manually")
  @Test
  public void testHistogram() throws Exception {
    Set set1 = new HashSet();
    final Set set2 = new HashSet();
    Object object3 = new Object();
    set1.add(set2);
    set2.add(object3);

    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "ObjectSizer.SIZE_OF_CLASS", "com.gemstone.gemfire.internal.size.SizeOfUtil0");
    System.out.println(ObjectGraphSizer.histogram(set1, true));
  }
  
  private static class TestVisitor implements ObjectTraverser.Visitor {
    private static final Object VALUE = new Object();
    
    public Map visited = new IdentityHashMap();

    public boolean visit(Object parent, Object object) {
      assertNull(visited.put(object, VALUE));
      return true;
    }
  }
  
  private static class TestObject1 {
    protected static Object test2;
  }
  
}
