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
package org.apache.geode.internal.size;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;

import org.apache.geode.util.internal.GeodeGlossary;

public class ObjectTraverserJUnitTest {

  @Test
  public void testBasic() throws Exception {
    Set<Object> testData = new HashSet<>();
    Object one = new Object();
    testData.add(one);
    Object[] two = new Object[2];
    testData.add(two);
    ArrayList<Object> three = new ArrayList<>();
    two[0] = three;
    three.add(testData);

    TestVisitor visitor = new TestVisitor();
    new ObjectTraverser().breadthFirstSearch(testData, visitor, false);

    assertNotNull(visitor.visited.remove(testData));
    assertNotNull(visitor.visited.remove(one));
    assertNotNull(visitor.visited.remove(two));
    assertNotNull(visitor.visited.remove(three));
  }

  @SuppressWarnings("InstantiationOfUtilityClass")
  @Test
  public void testStatics() throws Exception {
    final Object staticObject = new Object();
    TestObject1.test2 = staticObject;
    TestObject1 test1 = new TestObject1();

    TestVisitor visitor = new TestVisitor();
    ObjectTraverser nonStaticTraverser = new ObjectTraverser();
    nonStaticTraverser.breadthFirstSearch(test1, visitor, false);
    assertThat(visitor.visited.get(staticObject)).isNull();
    assertThat(nonStaticTraverser.getStaticFieldCache().get(test1.getClass())).isNull();

    visitor = new TestVisitor();
    ObjectTraverser staticTraverser = new ObjectTraverser();
    staticTraverser.breadthFirstSearch(test1, visitor, true);
    assertThat(visitor.visited.get(staticObject)).isNotNull();
    assertThat(staticTraverser.getStaticFieldCache().get(test1.getClass())).isNotNull();
  }

  @Test
  public void testStop() throws Exception {
    Set<Object> set1 = new HashSet<>();
    final Set<Object> set2 = new HashSet<>();
    Object object3 = new Object();
    set1.add(set2);
    set2.add(object3);

    TestVisitor visitor = new TestVisitor() {
      @Override
      public boolean visit(Object parent, Object object) {
        super.visit(parent, object);
        return object != set2;
      }
    };

    new ObjectTraverser().breadthFirstSearch(set1, visitor, true);

    assertNotNull(visitor.visited.get(set1));
    assertNotNull(visitor.visited.get(set2));
    assertNull(visitor.visited.get(object3));
  }

  /** This test is commented out because it needs to be verified manually */
  @Ignore("commented out because it needs to be verified manually")
  @Test
  public void testHistogram() throws Exception {
    Set<Object> set1 = new HashSet<>();
    final Set<Object> set2 = new HashSet<>();
    Object object3 = new Object();
    set1.add(set2);
    set2.add(object3);

    System.setProperty(GeodeGlossary.GEMFIRE_PREFIX + "ObjectSizer.SIZE_OF_CLASS",
        "org.apache.geode.internal.size.SizeOfUtil0");
    System.out.println(ObjectGraphSizer.histogram(set1, true));
  }

  private static class TestVisitor implements ObjectTraverser.Visitor {
    private static final Object VALUE = new Object();

    public Map<Object, Object> visited = new IdentityHashMap<>();

    @Override
    public boolean visit(Object parent, Object object) {
      assertNull(visited.put(object, VALUE));
      return true;
    }
  }

  private static class TestObject1 {
    protected static Object test2;
  }

}
