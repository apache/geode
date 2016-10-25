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
package org.apache.geode.cache;

import org.apache.geode.GemFireException;

/**
 * This exception is thrown when two nodes are defined with same primary
 * partitions
 * 
 * @since GemFire 6.6
 */
public class DuplicatePrimaryPartitionException extends GemFireException {

  private static final long serialVersionUID = 1L;

  /**
   * Creates a new <code>DuplicatePrimaryPartitionException</code> with no
   * detailed message.
   */

  public DuplicatePrimaryPartitionException() {
    super();
  }

  /**
   * Creates a new <code>DuplicatePrimaryPartitionException</code> with the
   * given detail message.
   */
  public DuplicatePrimaryPartitionException(String message) {
    super(message);
  }

  /**
   * Creates a new <code>DuplicatePrimaryPartitionException</code> with the
   * given cause and no detail message
   */
  public DuplicatePrimaryPartitionException(Throwable cause) {
    super(cause);
  }

  /**
   * Creates a new <code>DuplicatePrimaryPartitionException</code> with the
   * given detail message and cause.
   */
  public DuplicatePrimaryPartitionException(String message, Throwable cause) {
    super(message, cause);
  }

}
