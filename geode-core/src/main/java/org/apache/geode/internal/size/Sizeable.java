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

/**
 * An interface that allows an object to define its own size.
 *
 * <p>
 * <b>Sample Implementation</b>
 *
 * <pre>
 * public int getSizeInBytes() {
 *   // The sizes of the primitive as well as object instance variables are calculated:
 *   int size = 0;
 *
 *   // Add overhead for this instance.
 *   size += Sizeable.PER_OBJECT_OVERHEAD;
 *
 *   // Add object references (implements Sizeable)
 *   // value reference = 4 bytes
 *   size += 4;
 *
 *   // Add primitive instance variable size
 *   // byte bytePr = 1 byte
 *   // boolean flag = 1 byte
 *   size += 2;
 *
 *   // Add individual object size
 *   size += (value.getSizeInBytes());
 *
 *   return size;
 * }
 * </pre>
 *
 * @since GemFire 3.2
 */
public interface Sizeable {

  // TODO: for a 64bit jvm with small oops this is 12; for other 64bit jvms it is 16
  /**
   * The overhead of an object in the VM in bytes
   */
  int PER_OBJECT_OVERHEAD = 8;

  /**
   * Returns the size (in bytes) of this object including the {@link #PER_OBJECT_OVERHEAD}.
   */
  int getSizeInBytes();
}
