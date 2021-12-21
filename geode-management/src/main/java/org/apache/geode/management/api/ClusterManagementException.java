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
package org.apache.geode.management.api;


/**
 * Base class of all exceptions thrown by {@link ClusterManagementService} implementations.
 */
public class ClusterManagementException extends RuntimeException {
  private final ClusterManagementResult result;

  /**
   * for internal use only
   */

  public ClusterManagementException(ClusterManagementResult.StatusCode statusCode, String message) {
    super(statusCode.name() + ": " + message);
    result = new ClusterManagementResult(statusCode, message);
  }

  public ClusterManagementException(ClusterManagementResult result) {
    super(result.toString());
    this.result = result;
  }

  /**
   * for internal use only
   */
  public ClusterManagementException(ClusterManagementResult result, Throwable cause) {
    super(result.toString(), cause);
    this.result = result;
  }

  /**
   * for internal use only
   */
  public ClusterManagementResult getResult() {
    return result;
  }

  /**
   * get the status code of the unsuccessful result
   */
  public ClusterManagementResult.StatusCode getStatusCode() {
    return result.getStatusCode();
  }

  /**
   * get the status message of the unsuccessful result, if available
   */
  public String getStatusMessage() {
    return result.getStatusMessage();
  }
}
