/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.help.format;

/**
 * @author Nikhil Jadhav
 *
 */
public class Block {
  private String heading;
  private Row[] rows;

  public String getHeading() {
    return heading;
  }
  
  public Block setHeading(String heading) {
    this.heading = heading;
    return this;
  }
  
  public Row[] getRows() {
    return rows;
  }

  public Block setRows(Row[] rows) {
    this.rows = rows;
    return this;
  }

}
