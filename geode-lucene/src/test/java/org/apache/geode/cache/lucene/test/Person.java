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
import java.util.Collection;
import java.util.Collections;

public class Person implements Serializable {
  private String name;
  private String email;
  private int revenue;
  private String address;
  private String[] phoneNumbers;
  private Page homepage;

  public Person(String name, String[] phoneNumbers, int pageId) {
    this.name = name;
    this.email = name.replace(' ', '.') + "@pivotal.io";
    this.revenue = pageId * 100;
    this.address = "" + pageId + " NW Greenbrier PKWY, Portland OR 97006";
    this.phoneNumbers = phoneNumbers;
    this.homepage = new Page(pageId);
  }

  @Override
  public String toString() {
    return "Person[name=" + name + ",email=" + email + ",revenue=" + revenue + ",address=" + address
        + ",phone numbers=" + phoneNumbers + ",homepage=" + homepage + "]";
  }
}
