/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.domain;

import java.io.Serializable;

public class StackTracesPerMember implements Serializable {

  private static final long serialVersionUID = 1L;
  private final String memberNameOrId;
  private final byte []stackTraces;
  
  public StackTracesPerMember (String memberNameOrId, byte [] stackTraces) {
    this.memberNameOrId = memberNameOrId;
    this.stackTraces = stackTraces;
  }
  
  public String getMemberNameOrId () {
    return this.memberNameOrId;
  }
  
  public byte[] getStackTraces() {
    return this.stackTraces;
  }
}
