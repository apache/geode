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
package com.gemstone.gemfire.cache.persistence;

import com.gemstone.gemfire.GemFireException;

/**
 * Thrown when a replicated region is configured for persistence
 * on some members but none of those members are currently online.
 * 
 * If you see this exception you should restart members that are
 * configured for hosting persistent replicates.
 * 
 * @since GemFire 7.0
 */
public class PersistentReplicatesOfflineException extends GemFireException {
  private static final long serialVersionUID = 6209644027346609970L;

  public PersistentReplicatesOfflineException() {
  }
  
  public PersistentReplicatesOfflineException(String message) {
    super(message);
  }
  
  public PersistentReplicatesOfflineException(String message, Throwable cause) {
    super(message, cause);
  }
}
