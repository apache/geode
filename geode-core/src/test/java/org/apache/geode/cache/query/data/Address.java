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
/*
 * Address.java
 *
 * Created on March 2, 2005, 12:23 PM
 */

package org.apache.geode.cache.query.data;

import java.util.*;


/**
 *
 */
public class Address {
  public String zipCode;
  public String city;

  public Set street;
  public Set phoneNo;

  /** Creates a new instance of Address */
  public Address(String zipCode, String city) {
    this.zipCode = zipCode;
    this.city = city;
  }

  public Address(String zipCode, String city, Set street, Set phoneNo) {
    this.zipCode = zipCode;
    this.city = city;
    this.street = street;
    this.phoneNo = phoneNo;
  }

}// end of class
