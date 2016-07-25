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
package com.gemstone.gemfire.cache.client.internal.pooling;

import com.gemstone.gemfire.GemFireException;

/**
 * Indicates that the current connection has already been destroyed.
 * This exception should not propagate all the way back to the 
 * user, but is a signal to retry an attempt.
 *
 */
public class ConnectionDestroyedException extends GemFireException {
private static final long serialVersionUID = -6918516787578041316L;

  public ConnectionDestroyedException() {
    super();
  }

  public ConnectionDestroyedException(String message, Throwable cause) {
    super(message, cause);
  }

  public ConnectionDestroyedException(String message) {
    super(message);
  }

  public ConnectionDestroyedException(Throwable cause) {
    super(cause);
  }

  
}
