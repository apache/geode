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
package com.gemstone.gemfire.internal.cache.lru;

/**
 * An interface that allows an object to define its own size.<br>
 *
 *<b>Sample Implementation</b><br>
 *<code>public int getSizeInBytes(){</code><br>
    // The sizes of the primitive as well as object instance variables are calculated:<br>
    
    <code>int size = 0;</code><br>
    
    // Add overhead for this instance.<br>
    <code>size += Sizeable.PER_OBJECT_OVERHEAD;</code><br>

    // Add object references (implements Sizeable)<br>
    // value reference = 4 bytes <br>
    
    <code>size += 4;</code><br>
    
    // Add primitive instance variable size<br>
    // byte bytePr = 1 byte<br>
    // boolean flag = 1 byte<br>
    
    <code>size += 2;</code><br>
    
    // Add individual object size<br> 
    <code>size += (value.getSizeInBytes());</code><br> 
     
    <code>return size;</code><br>
  }<br>
 *
 *
 * @since GemFire 3.2
 */
public interface Sizeable {

  /** The overhead of an object in the VM in bytes */
  public static final int PER_OBJECT_OVERHEAD = 8; // TODO for a 64bit jvm with small oops this is 12; for other 64bit jvms it is 16

  /**
   * Returns the size (in bytes) of this object including the {@link
   * #PER_OBJECT_OVERHEAD}.
   */
  public int getSizeInBytes();

}
