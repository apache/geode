/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.domain;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/****
 * Data class used for sending back names for the durable client cq
 * for a client
 */
public class DurableCqNamesResult extends MemberResult implements Serializable{

  private static final long serialVersionUID = 1L;
  private List<String> cqNames = new ArrayList<String>();
  
  public DurableCqNamesResult(final String memberNameOrId) {
	 super(memberNameOrId);
  }
  public DurableCqNamesResult (final String memberNameOrId, List<String> cqNames) {
	  super(memberNameOrId);
	  cqNames.addAll(cqNames);
  }
  
  public void addCqName(String cqName) {
    cqNames.add(cqName);
    super.isSuccessful = true;
    super.opPossible = true;
  }
  
  public List<String> getCqNamesList() {
    return cqNames;
  }
  
  public void setCqNamesList(List<String> cqNamesList) {
	  this.cqNames.addAll(cqNamesList);
	  this.isSuccessful = true;
	  this.opPossible = true;
  }
  
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append(super.toString());
    for (String cqName: cqNames) {
      sb.append("\nCqName : " + cqName);
    }
    return sb.toString();
  }
}
