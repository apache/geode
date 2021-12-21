/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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
package org.apache.geode.cache.lucene.test;

import java.io.Serializable;

public class Person implements Serializable {
  private final String name;
  private final String email;
  private final int revenue;
  private final String address;
  private final String[] phoneNumbers;
  private final Page homepage;

  public Person(String name, String[] phoneNumbers, int pageId) {
    this.name = name;
    email = name.replace(' ', '.') + "@pivotal.io";
    revenue = pageId * 100;
    address = "" + pageId + " NW Greenbrier PKWY, Portland OR 97006";
    this.phoneNumbers = phoneNumbers;
    homepage = new Page(pageId);
  }

  @Override
  public String toString() {
    return "Person[name=" + name + ",email=" + email + ",revenue=" + revenue + ",address=" + address
        + ",phone numbers=" + phoneNumbers + ",homepage=" + homepage + "]";
  }
}
