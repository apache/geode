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
package com.gemstone.gemfire.internal.tcp;

import com.gemstone.gemfire.GemFireException;

/**
 * An exception indicating that the same thread that is in the middle
 * of trying to connect has tried to obtain a connection to the same
 * member further down the call stack.
 * 
 * This condition has been observered when using an AlertListener, because 
 * we try to transmit messages logged during a connection to the very member
 * we're trying to connect to. 
 *
 */
public class ReenteredConnectException extends GemFireException {

  public ReenteredConnectException() {
    super();
  }

  public ReenteredConnectException(String message, Throwable cause) {
    super(message, cause);
  }

  public ReenteredConnectException(String message) {
    super(message);
  }

  public ReenteredConnectException(Throwable cause) {
    super(cause);
  }


}
