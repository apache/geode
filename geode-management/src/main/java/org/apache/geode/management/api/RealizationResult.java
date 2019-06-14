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

import java.io.Serializable;


/**
 * this holds the information returned by the ConfigurationRealizers to indicate the
 * success/failure of the realization step.
 *
 * It by default should have at least memberName, success and a message.
 */

public class RealizationResult implements Serializable {
  private String memberName;
  private boolean success = true;
  private String message = "success";

  public RealizationResult() {
    setSuccess(true);
    setMessage("success");
  }

  public String getMessage() {
    return message;
  }

  public RealizationResult setMessage(String message) {
    this.message = message;
    return this;
  }

  public String getMemberName() {
    return memberName;
  }

  public RealizationResult setMemberName(String memberName) {
    this.memberName = memberName;
    return this;
  }

  public boolean isSuccess() {
    return success;
  }

  public RealizationResult setSuccess(boolean success) {
    this.success = success;
    return this;
  }
}
