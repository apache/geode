/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.domain;

import java.io.Serializable;

/***
 * Data class used to return the result of a function on a member. 
 * Typically to return the status of an action on a member.
 * Not suitable if you wish to return specific data from member
 * @author bansods
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
