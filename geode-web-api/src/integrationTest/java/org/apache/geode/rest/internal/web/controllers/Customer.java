/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.rest.internal.web.controllers;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.geode.internal.lang.ObjectUtils;

/**
 * The Customer class models a customer entity.
 * <p/>
 *
 * @since GemFire 8.0
 */
@JsonInclude(Include.NON_NULL)
public class Customer implements Serializable {

  public Long customerId;
  private String firstName;
  private String lastName;
  @JsonProperty("ssn")
  private String socialSecurityNumber;

  public Customer() {}

  public Customer(final Long custId) {
    this.customerId = custId;
  }

  public Customer(final Long custId, final String fname, final String lname, final String ssn) {
    this.customerId = custId;
    this.firstName = fname;
    this.lastName = lname;
    this.socialSecurityNumber = ssn;
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
        && ObjectUtils.equals(this.getLastName(), that.getLastName())
        && ObjectUtils.equals(this.getFirstName(), that.getFirstName())
        && ObjectUtils.equals(this.getSocialSecurityNumber(), that.getSocialSecurityNumber()));
  }

  public String getSocialSecurityNumber() {
    return socialSecurityNumber;
  }

  public void setSocialSecurityNumber(final String ssn) {
    this.socialSecurityNumber = ssn;
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
    buffer.append(", ssn = ").append(getSocialSecurityNumber());
    buffer.append(" }");
    return buffer.toString();
  }

}
