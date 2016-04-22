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
package com.gemstone.gemfire;

/**
 * An <code>SystemConnectException</code> is thrown when a
 * GemFire application tries to connect to an
 * existing distributed system and is unable to contact all members of
 * the distributed system to announce its presence.  This is usually due
 * to resource depletion problems (low memory or too few file descriptors)
 * in other processes.
 */
public class SystemConnectException extends GemFireException {
private static final long serialVersionUID = -7378174428634468238L;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>SystemConnectException</code>.
   */
  public SystemConnectException(String message) {
    super(message);
  }
  
  public SystemConnectException(String message, Throwable cause) {
    super(message, cause);
  }
}
