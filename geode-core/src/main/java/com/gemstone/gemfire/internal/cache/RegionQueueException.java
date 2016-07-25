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

package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.CacheException;
/**
 * An exception thrown by a <code>RegionQueue</code>.
 *
 *
 * @since GemFire 4.2
 */
// Since this exception is in an internal package, we make it
// a checked exception.
public class RegionQueueException extends CacheException {
private static final long serialVersionUID = 4159307586325821105L;

  /**
   * Required for serialization
   */
  public RegionQueueException() {
  }

  /**
   * Constructor.
   * Creates an instance of <code>RegionQueueException</code> with the
   * specified detail message.
   * @param msg the detail message
   */
  public RegionQueueException(String msg) {
    super(msg);
  }

}
