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

package org.apache.geode.internal.cache.tier;

import org.apache.geode.GemFireCheckedException;

/**
 * An exception thrown during batch processing.
 *
 *
 * @since GemFire 4.2
 */
// Note that since this class is inside of an internal package,
// we make it extend Exception, thereby making it a checked exception.
public class BatchException extends GemFireCheckedException {
  private static final long serialVersionUID = -6707074107791305564L;

  protected int _index;

  /**
   * Required for serialization
   */
  public BatchException() {}

  /**
   * Constructor. Creates an instance of <code>RegionQueueException</code> with the specified detail
   * message.
   *
   * @param msg the detail message
   * @param index the index in the batch list where the exception occurred
   */
  public BatchException(String msg, int index) {
    super(msg);
    _index = index;
  }

  /**
   * Constructor. Creates an instance of <code>RegionQueueException</code> with the specified cause.
   *
   * @param cause the causal Throwable
   * @param index the index in the batch list where the exception occurred
   */
  public BatchException(Throwable cause, int index) {
    super(cause);
    _index = index;
  }

  /**
   * Answers the index in the batch where the exception occurred
   *
   * @return the index in the batch where the exception occurred
   */
  public int getIndex() {
    return _index;
  }
}
