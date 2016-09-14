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

import org.apache.geode.distributed.internal.InternalDistributedSystem;

/**
 * An exception indicating that a failure has happened on the server
 * while processing an operation that was sent to it by a client.
 * @since GemFire 5.7
 */
public class ServerOperationException extends ServerConnectivityException {
private static final long serialVersionUID = -3106323103325266219L;

  /**
   * Create a new instance of ServerOperationException without a detail message or cause.
   */
  public ServerOperationException() {
  }

  /**
   * 
   * Create a new instance of ServerOperationException with a detail message
   * @param message the detail message
   */
  public ServerOperationException(String message) {
    super(getServerMessage(message));
  }

  /**
   * Create a new instance of ServerOperationException with a detail message and cause
   * @param message the detail message
   * @param cause the cause
   */
  public ServerOperationException(String message, Throwable cause) {
    super(getServerMessage(message), cause);
  }

  /**
   * Create a new instance of ServerOperationException with a cause
   * @param cause the cause
   */
  public ServerOperationException(Throwable cause) {
    super(getServerMessage(cause), cause);
  }

  private static String getServerMessage(Throwable cause) {
    return getServerMessage(cause != null ? cause.toString() : null);
  }

  private static String getServerMessage(String msg) {
    // To fix bug 44679 add a description of the member the server is on.
    // Do this without changing how this class gets serialized so that old
    // clients will still work.
    InternalDistributedSystem ids = InternalDistributedSystem.getAnyInstance();
    if (ids != null) {
      if (msg != null) {
        return "remote server on " + ids.getMemberId() + ": " + msg;
      } else {
        return "remote server on " + ids.getMemberId();
      }
    } else {
      if (msg != null) {
        return "remote server on unknown location: " + msg;
      } else {
        return "remote server on unknown location";
      }
    }
  }

}
