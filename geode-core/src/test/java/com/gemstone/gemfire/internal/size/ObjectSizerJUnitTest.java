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

import static com.gemstone.gemfire.internal.size.SizeTestUtil.*;
import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class ObjectSizerJUnitTest {

  @Test
  public void test() throws Exception {
    assertEquals(roundup(OBJECT_SIZE), ObjectGraphSizer.size(new Object()));
    
    assertEquals(roundup(OBJECT_SIZE + 4), ObjectGraphSizer.size(new TestObject1()));
    assertEquals(roundup(OBJECT_SIZE + 4), ObjectGraphSizer.size(new TestObject2()));
    assertEquals(roundup(OBJECT_SIZE), ObjectGraphSizer.size(new TestObject3()));
    assertEquals(roundup(OBJECT_SIZE * 2 + REFERENCE_SIZE), ObjectGraphSizer.size(new TestObject3(), true));
    assertEquals(roundup(OBJECT_SIZE + REFERENCE_SIZE), ObjectGraphSizer.size(new TestObject4()));
    assertEquals(roundup(OBJECT_SIZE + REFERENCE_SIZE) + roundup(OBJECT_SIZE + 4), ObjectGraphSizer.size(new TestObject5()));
    assertEquals(roundup(OBJECT_SIZE + REFERENCE_SIZE) + roundup(OBJECT_SIZE + REFERENCE_SIZE * 4 + 4) + roundup(OBJECT_SIZE + 4), ObjectGraphSizer.size(new TestObject6()));
    assertEquals(roundup(OBJECT_SIZE + 7), ObjectGraphSizer.size(new TestObject7()));
  }
  
  private static class TestObject1 {
    int a;
  }
  
  private static class TestObject2 {
    int a;
  }
  
  private static class TestObject3 {
    static TestObject1 a = new TestObject1();
  }
  
  private static class TestObject4 {
    TestObject1 object = null;
  }
  
  private static class TestObject5 {
    TestObject1 object = new TestObject1();
  }
  
  private static class TestObject6 {
    TestObject1[] array =new TestObject1[4];
    
    public TestObject6() {
      array[3] = new TestObject1();
      array[2] = array[3];
    }
  }
  
  private static class TestObject7 {
    byte b1;
    byte b2;
    byte b3;
    byte b4;
    byte b5;
    byte b6;
    byte b7;
  }
}