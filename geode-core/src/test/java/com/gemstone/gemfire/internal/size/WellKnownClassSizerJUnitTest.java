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

import com.gemstone.gemfire.cache.util.ObjectSizer;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class WellKnownClassSizerJUnitTest {

  @Test
  public void testByteArrays() {
    byte[] test1 = new byte[5];
    byte[] test2 = new byte[8];
    
    ReflectionSingleObjectSizer referenceSizer = new ReflectionSingleObjectSizer();
    
    assertEquals(referenceSizer.sizeof(test1), WellKnownClassSizer.sizeof(test1));
    assertEquals(referenceSizer.sizeof(test2), WellKnownClassSizer.sizeof(test2));
    
    assertEquals(0, WellKnownClassSizer.sizeof(new Object()));
  }

  @Test
  public void testStrings() {
    String test1 = "123";
    String test2 = "012345678";
    
    ReflectionSingleObjectSizer referenceSizer = new ReflectionSingleObjectSizer();
    test1.toCharArray();
    
    //The size of a string varies based on the JDK version. With 1.7.0_06
    //a couple of fields were removed. So just measure the size of an empty string.
    String emptyString = "";
    int emptySize = ObjectSizer.SIZE_CLASS_ONCE.sizeof(emptyString) - ObjectSizer.SIZE_CLASS_ONCE.sizeof(new char[0]);
    
    assertEquals(emptySize + roundup(OBJECT_SIZE +4 + 3*2), WellKnownClassSizer.sizeof(test1));
    assertEquals(emptySize + roundup(OBJECT_SIZE +4 + 9*2), WellKnownClassSizer.sizeof(test2));
  }

}
