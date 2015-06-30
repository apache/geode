/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.rest.internal.web.controllers;

import java.io.Serializable;
import com.gemstone.gemfire.internal.lang.ObjectUtils;

/**
 * The Customer class models a customer entity.
 * <p/>
 * @author Nilkanth Patel
 * @since 8.0
 */

public class Customer implements Serializable {

  private Long customerId;
  private String firstName;
  private String lastName;

  public Customer() {
  }

  public Customer(final Long custId) {
    this.customerId = custId;
  }

  public Customer(final Long custId, final String fname, final String lname) {
    this.customerId = custId;
    this.firstName = fname;
    this.lastName = lname;
  }

  public Long getCustomerId() {
    return customerId;
  }

  public void setCustomerId(Long customerId) {
    this.customerId = customerId;
  }

  public String getFirstName() {
    return firstName;
  }

  public void setFirstName(String firstName) {
    this.firstName = firstName;
  }

  public String getLastName() {
    return lastName;
  }

  public void setLastName(String lastName) {
    this.lastName = lastName;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
      return true;
    }

    if (!(obj instanceof Customer)) {
      return false;
    }

    final Customer that = (Customer) obj;

    return (ObjectUtils.equals(this.getCustomerId(), that.getCustomerId())
        && ObjectUtils.equals(this.getLastName(), that.getLastName()) && ObjectUtils
          .equals(this.getFirstName(), that.getFirstName()));
  }

  @Override
  public int hashCode() {
    int hashValue = 17;
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getCustomerId());
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getLastName());
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getFirstName());
    return hashValue;
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder("{ type = ");
    buffer.append(getClass().getName());
    buffer.append(", customerId = ").append(getCustomerId());
    buffer.append(", firstName = ").append(getFirstName());
    buffer.append(", lastName = ").append(getLastName());
    buffer.append(" }");
    return buffer.toString();
  }

}
