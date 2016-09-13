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
package org.apache.geode;

/**
 * Indicates that attempts to allocate more objects in off-heap memory has
 * failed and the Cache will be closed to prevent it from losing distributed
 * consistency.
 * 
 */
public class OutOfOffHeapMemoryException extends CancelException {
  private static final long serialVersionUID = 4111959438738739010L;

  /**
   * Constructs an <code>OutOfOffHeapMemoryError</code> with no detail message.
   */
  public OutOfOffHeapMemoryException() {
  }

  /**
   * Constructs an <code>OutOfOffHeapMemoryError</code> with the specified
   * detail message.
   *
   * @param   message   the detail message.
   */
  public OutOfOffHeapMemoryException(String message) {
    super(message);
  }

}
