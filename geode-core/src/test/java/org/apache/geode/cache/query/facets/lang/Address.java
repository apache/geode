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

package org.apache.geode.cache.query.facets.lang;


public class Address {
  public String street;
  private String city;
  public String state;
  public String postalCode;


  public Address() {
    if (Employee.rand.nextFloat() < .5) {
      street = "20575 NW von Neumann Dr";
    }
    if (Employee.rand.nextFloat() < 0.5) {
      city = "Beaverton";
    }
    if (Employee.rand.nextFloat() < 0.5) {
      state = "OR";
    }
    if (Employee.rand.nextFloat() < 0.5) {
      postalCode = "97006";
    }
  }


  public String toString() {
    return street + '|' + city + ' ' + state + "  " + postalCode;
  }

  public String city() {
    return city;
  }

  public void city(String city) {
    this.city = city;
  }


}
