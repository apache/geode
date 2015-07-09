/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.domain;

import java.io.Serializable;

public final class Stock implements Serializable{
  private String key;
  private double value;
  
  public Stock (String key, double value) {
    this.key = key;
    this.value = value;
  }
  
  public String getKey() {
    return this.key;
  }
  
  public double getValue() {
    return this.value;
  }
}
