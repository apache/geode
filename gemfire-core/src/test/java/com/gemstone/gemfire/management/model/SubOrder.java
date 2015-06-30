/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.model;

/**
 * @rishim
 */
public class SubOrder extends Order {
  private static final long serialVersionUID = 2049641616996906291L;

  @Override
  public String getId() {
    return super.getId() + "1";
  }

}
