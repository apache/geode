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
package org.apache.geode.internal.exception;

import org.apache.geode.annotations.Experimental;

/*
 * Indicates that OperationContext was missing required data. This will typically happen if a
 * operation that is supposed to run on a server runs on a locator and receives a locator in its
 * context instead of a cache. The reverse case applies as well.
 */
@Experimental
public class InvalidExecutionContextException extends Exception {
  public InvalidExecutionContextException(String message) {
    super(message);
  }

  public InvalidExecutionContextException(String message, Throwable cause) {
    super(message, cause);
  }
}
