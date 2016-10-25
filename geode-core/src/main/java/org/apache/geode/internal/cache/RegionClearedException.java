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
package org.apache.geode.internal.cache;

/**
 * Indicates that a Clear Operation happened while an entry operation
 * was in progress, which would result in the ongoing entry operation to abort
 * @since GemFire 5.1
 */
public class RegionClearedException extends Exception  {
private static final long serialVersionUID = 1266503771775907997L;
  /**
   * Constructs a new <code>RegionClearedException</code>.
   */
  public RegionClearedException() {
    super();
  }
  
  /**
   * Constructs a new <code>RegionClearedException</code> with a message string.
   *
   * @param msg a message string
   */
  public RegionClearedException(String msg) {
    super(msg);
  }
  
  /**
   * Constructs a new <code>RegionClearedException</code> with a message string
   * and a cause.
   *
   * @param msg the message string
   * @param cause a causal Throwable
   */
  public RegionClearedException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  /**
   * Constructs a new <code>RegionClearedException</code> with a cause.
   *
   * @param cause a causal Throwable
   */
  public RegionClearedException(Throwable cause) {
    super(cause);
  }
}
