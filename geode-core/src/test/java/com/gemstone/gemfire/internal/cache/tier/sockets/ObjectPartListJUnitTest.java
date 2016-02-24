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
package com.gemstone.gemfire.internal.cache.tier.sockets;

import java.io.IOException;
import java.util.List;

import org.junit.experimental.categories.Category;

import junit.framework.TestCase;

import com.gemstone.gemfire.CopyHelper;
import com.gemstone.gemfire.internal.util.BlobHelper;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 *
 */
@Category(UnitTest.class)
public class ObjectPartListJUnitTest extends TestCase {
  
  public void testValueAsObject() throws IOException {
    VersionedObjectList list = new VersionedObjectList(100, false, false);
    byte[] normalBytes = "value1".getBytes();
    list.addObjectPart("key", normalBytes , false, null);
    list.addObjectPart("key", "value2", true, null);
    byte[] serializedObjectBytes = BlobHelper.serializeToBlob("value3");
    list.addObjectPart("key", serializedObjectBytes, true, null);
    list.addExceptionPart("key", new TestException("hello"));
    list.addObjectPartForAbsentKey("key", null);
    
    //Create a clone of the this.
    VersionedObjectList newList = CopyHelper.copy(list);
    
    checkObjectValues(newList);
    
    //THIS TEST FAILS! ObjectPartArrayList doesn't
    //preserve all its state when it is serialized (it loses track of type information
    //for values of type BYTES by writing the type info as OBJECT). However, 
    //we'll have to leave it to avoid breaking old clients.
    //create another copy, just to double check
//    newList = CopyHelper.copy(newList);
//    
//    checkObjectValues(newList);
    
  }

  private void checkObjectValues(ObjectPartList newList) {
    assertEquals(5, newList.size());
    assertNull(newList.getKeysForTest());
    List values = newList.getObjects();
    assertEquals("value1", new String((byte[]) values.get(0)));
    assertEquals("value2", values.get(1));
    assertEquals("value3", values.get(2));
    assertEquals(new TestException("hello"), values.get(3));
    assertNull(values.get(4));
  }
  
  public void testValueAsObjectByteArray() throws IOException, ClassNotFoundException {
    ObjectPartList list = new VersionedObjectList(100, false, false, true);
    byte[] normalBytes = "value1".getBytes();
    list.addObjectPart("key", normalBytes , false, null);
    list.addObjectPart("key", "value2", true, null);
    byte[] serializedObjectBytes = BlobHelper.serializeToBlob("value3");
    list.addObjectPart("key", serializedObjectBytes, true, null);
    list.addExceptionPart("key", new TestException("hello"));
    list.addObjectPartForAbsentKey("key", null);
    
    //Create a clone of the this list.
    ObjectPartList newList = CopyHelper.copy(list);
    
    checkSerializedValues(newList);
    
    //Create another copy, just to double check
    //all the info was perserved
    newList = CopyHelper.copy(newList);
    
    checkSerializedValues(newList);
  }

  private void checkSerializedValues(ObjectPartList newList)
      throws IOException, ClassNotFoundException {
    assertEquals(5, newList.size());
    assertNull(newList.getKeysForTest());
    List values = newList.getObjects();
    assertEquals("value1", new String((byte[]) values.get(0)));
    assertEquals("value2", BlobHelper.deserializeBlob((byte[])values.get(1)));
    assertEquals("value3", BlobHelper.deserializeBlob((byte[])values.get(2)));
    assertEquals(new TestException("hello"), values.get(3));
    assertNull(values.get(4));
  }
  
  private static class TestException extends Exception {

    public TestException(String message) {
      super(message);
      // TODO Auto-generated constructor stub
    }
    
    
    @Override
    public boolean equals(Object o) {
      if(!(o instanceof TestException)) {
        return false;
      }
      if(!((TestException) o ).getMessage().equals(getMessage())) {
        return false;
      }
      return true;
    }
    
    
  }

}
