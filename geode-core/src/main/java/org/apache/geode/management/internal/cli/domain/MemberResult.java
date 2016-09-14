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
package org.apache.geode.management.internal.cli.domain;

import java.io.Serializable;

/***
 * Data class used to return the result of a function on a member. 
 * Typically to return the status of an action on a member.
 * Not suitable if you wish to return specific data from member
 *
 */
public class MemberResult implements Serializable {

  private static final long serialVersionUID = 1L;
  protected String memberNameOrId;
  protected boolean isSuccessful;
  protected boolean opPossible;
  protected String exceptionMessage;
  protected String successMessage;
  protected String errorMessage;


  public MemberResult(String memberNameOrId) {
    this.memberNameOrId = memberNameOrId;
    this.isSuccessful = true;
    this.opPossible = true;
  }

  public MemberResult(String memberNameOrId, String errorMessage) {
    this.memberNameOrId = memberNameOrId;
    this.errorMessage = errorMessage;
    this.opPossible = false;
    this.isSuccessful = false;
  }

  public String getMemberNameOrId() {
    return memberNameOrId;
  }

  public void setMemberNameOrId(String memberNameOrId) {
    this.memberNameOrId = memberNameOrId;
  }

  public boolean isSuccessful() {
    return isSuccessful;
  }

  public void setSuccessful(boolean isSuccessful) {
    this.isSuccessful = isSuccessful;
  }

  public String getExceptionMessage() {
    return exceptionMessage;
  }

  public void setExceptionMessage(String exceptionMessage) {
    this.exceptionMessage = exceptionMessage;
    this.opPossible = true;
    this.isSuccessful = false;
  }

  public boolean isOpPossible() {
    return this.opPossible;
  }

  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
    this.opPossible = false;
    this.isSuccessful = false;
  }

  public String getErrorMessage() {
    return this.errorMessage;
  }

  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("MemberNameOrId : ");
    sb.append(this.memberNameOrId);
    sb.append("\nSuccessfull : ");
    sb.append(this.isSuccessful);
    sb.append("\n isOpPossible");
    sb.append(this.isOpPossible());
    sb.append("Success Message : ");
    sb.append(this.successMessage);
    sb.append("\nException Message : ");
    sb.append(this.exceptionMessage);
    return sb.toString();
  }

  public String getSuccessMessage() {
    return successMessage;
  }

  public void setSuccessMessage(String successMessage) {
    this.successMessage = successMessage;
  }
}
