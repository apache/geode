/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.examples.ds;

public class Company {

  /** The name of this company */
  private String name;

  /** The address of this company */
  private Address address;

  /**
   * Creates a new company
   */
  public Company(String name, Address address) {
    this.name = name;
    this.address = address;
  }

  public String getName() {
    return this.name;
  }

  public Address getAddress() {
    return this.address;
  }

}
