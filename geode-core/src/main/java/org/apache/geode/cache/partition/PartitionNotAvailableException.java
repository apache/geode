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
package org.apache.geode.cache.partition;

import org.apache.geode.GemFireException;

/**
 * This exception is thrown when for the given fixed partition, datastore (local-max-memory > 0) is
 * not available.
 * 
 * @since GemFire 6.6
 */

public class PartitionNotAvailableException extends GemFireException {

  private static final long serialVersionUID = 1L;

  /**
   * Creates a new <code>PartitionNotAvailableException</code> with no detailed message.
   */

  public PartitionNotAvailableException() {
    super();
  }

  /**
   * Creates a new <code>PartitionNotAvailableException</code> with the given detail message.
   */
  public PartitionNotAvailableException(String message) {
    super(message);
  }

  /**
   * Creates a new <code>PartitionNotAvailableException</code> with the given cause and no detail
   * message
   */
  public PartitionNotAvailableException(Throwable cause) {
    super(cause);
  }

  /**
   * Creates a new <code>PartitionNotAvailableException</code> with the given detail message and
   * cause.
   */
  public PartitionNotAvailableException(String message, Throwable cause) {
    super(message, cause);
  }

}
