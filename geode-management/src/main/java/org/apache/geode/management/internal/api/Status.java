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
package org.apache.geode.management.internal.api;

public class Status {
  public enum Result {
    SUCCESS, FAILURE, NOT_APPLICABLE, NO_OP
  }

  Result status;
  String message;

  // needed for json deserialization
  public Status() {}

  public Status(Result status, String message) {
    this.status = status;
    this.message = message;
  }

  public Status(boolean success, String message) {
    this.status = success ? Result.SUCCESS : Result.FAILURE;
    this.message = message;
  }

  public Result getStatus() {
    return status;
  }

  public void setStatus(Result status) {
    this.status = status;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }
}
