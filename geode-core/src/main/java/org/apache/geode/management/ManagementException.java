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
package org.apache.geode.management;

import org.apache.geode.GemFireException;

/**
 * A <code>ManagementException</code> is a general exception that may be thrown
 * when any administration or monitoring operation on a GemFire component
 * fails.
 * 
 * Various management and monitoring exceptions are wrapped in
 * <code>ManagementException<code>s.
 * 
 * @since GemFire 7.0
 * 
 */
public class ManagementException extends GemFireException {

  private static final long serialVersionUID = 879398950879472121L;

  /**
   * Constructs a new exception with a <code>null</code> detail message. The
   * cause is not initialized, and may subsequently be initialized by a call to
   * {@link Throwable#initCause}.
   */
  public ManagementException() {
    super();
  }

  /**
   * Constructs a new exception with the specified detail message. The cause is
   * not initialized and may subsequently be initialized by a call to
   * {@link Throwable#initCause}.
   * 
   * @param message
   *          The detail message.
   */
  public ManagementException(String message) {
    super(message);
  }

  /**
   * Constructs a new ManagementException with the specified detail message and
   * cause.
   * <p>
   * Note that the detail message associated with <code>cause</code> is
   * <i>not</i> automatically incorporated in this runtime exception's detail
   * message.
   * 
   * @param message
   *          The detail message.
   * @param cause
   *          The cause of this exception or <code>null</code> if the cause is
   *          unknown.
   */
  public ManagementException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructs a new ManagementException by wrapping the specified cause. The
   * detail for this exception will be null if the cause is null or
   * cause.toString() if a cause is provided.
   * 
   * @param cause
   *          The cause of this exception or <code>null</code> if the cause is
   *          unknown.
   */
  public ManagementException(Throwable cause) {
    super(cause);
  }
}
