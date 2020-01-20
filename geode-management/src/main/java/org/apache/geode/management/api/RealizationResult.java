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

import org.apache.geode.annotations.Experimental;


/**
 * this holds the information returned by the ConfigurationRealizers to indicate the
 * success/failure of the realization step.
 *
 * It by default should have at least memberName, success and a message.
 */
@Experimental
public class RealizationResult implements Serializable {
  private String memberName;
  private boolean success = true;
  private String message = "Success.";

  /**
   * for internal use only
   */
  public RealizationResult() {}

  /**
   * returns optional information to supplement {@link #isSuccess()}
   */
  public String getMessage() {
    return message;
  }

  /**
   * for internal use only
   */
  public RealizationResult setMessage(String message) {
    this.message = message;
    return this;
  }

  /**
   * returns the member name this realization result applies to
   */
  public String getMemberName() {
    return memberName;
  }

  /**
   * for internal use only
   */
  public RealizationResult setMemberName(String memberName) {
    this.memberName = memberName;
    return this;
  }

  /**
   * returns true if realization succeeded on this member
   */
  public boolean isSuccess() {
    return success;
  }

  /**
   * for internal use only
   */
  public RealizationResult setSuccess(boolean success) {
    this.success = success;
    return this;
  }

  @Override
  public String toString() {
    return memberName + ": " + success + ", " + message;
  }
}
