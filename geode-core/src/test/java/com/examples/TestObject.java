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
package com.examples;

/**
 * A simple test object used by the 
 * com.gemstone.gemfire.internal.enhancer.serializer.SerializingStreamPerfTest 
 * (this test no longer exists?) 
 * that must be in a non-<code>com.gemstone</code> package.
 *
 * @author David Whitlock
 *
 * @since 3.5
 */
public class TestObject {

  private int intField;
  private String stringField;
  private Object objectField;

  /**
   * Creates a new <code>TestObject</code>
   */
  public TestObject() {
    this.intField = 42;
    this.stringField = "123456789012345678901234567890";
    this.objectField = new Integer(67);
  }

  //////////////////////  Inner Classes  //////////////////////

  /**
   * A <code>Serializable</code> object that is serialized
   */
  public static class SerializableTestObject extends TestObject
    implements java.io.Serializable {
    
  }
  
}

