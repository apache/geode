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

package org.apache.geode.cache.client;

import org.apache.geode.cache.OperationAbortedException;

/**
 * A <code>ClientNotReadyException</code> indicates a client attempted to invoke
 * the {@link org.apache.geode.cache.Cache#readyForEvents}
 * method, but failed.
 * <p>This exception was moved from the <code>util</code> package in 5.7.
 * 
 *
 * @since GemFire 5.7
 * @deprecated as of 6.5 this exception is no longer thrown by GemFire so any code that catches it should be removed.
 * 
 */
public class ClientNotReadyException extends OperationAbortedException {
private static final long serialVersionUID = -315765802919271588L;
  /**
   * Constructs an instance of <code>ClientNotReadyException</code> with the
   * specified detail message.
   * 
   * @param msg the detail message
   */
  public ClientNotReadyException(String msg) {
    super(msg);
  }
  
  /**
   * Constructs an instance of <code>ClientNotReadyException</code> with the
   * specified detail message and cause.
   * 
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public ClientNotReadyException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
