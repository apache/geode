/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.help.format;

public class Row {
  private String[] info;

  public String[] getInfo() {
    return info;
  }

  public Row setInfo(String[] info) {
    this.info = info;
    return this;
  }
}
