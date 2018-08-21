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

import java.security.Principal;
import java.util.Properties;

import org.apache.geode.security.PostProcessor;

/**
 * This is example that implements PostProcessor that will redact Social Security Numbers from
 * Customer objects
 */
public class RedactingPostProcessor implements PostProcessor {

  @Override
  public void init(final Properties securityProps) {}

  /**
   * Protect the Customer's Social Security Number from unauthorized users
   *
   * @param principal The principal that's accessing the value
   * @param regionName The region that's been accessed. This could be null.
   * @param id the customer id the lookup was done with. This could be null.
   * @param customer a @Customer object, this could be null.
   * @return the processed value
   */
  @Override
  public Object processRegionValue(Object principal, String regionName, Object id,
      Object customer) {
    if (customer == null)
      return null;
    if (customer instanceof Customer) {
      String username = getUsername(principal);
      // Unable to retrieve the role at this point, so for this demo we'll just work with the
      // username
      if (username.equals("super-user"))
        return customer;
      Customer cust = (Customer) customer;
      return new Customer(cust.getCustomerId(), cust.getFirstName(), cust.getLastName(),
          "*********");
    } else
      return customer;
  }

  private String getUsername(final Object principal) {
    String name = null;
    if (principal instanceof Principal) {
      name = ((Principal) principal).getName();
    } else {
      name = principal.toString();
    }
    return name;
  }
}
