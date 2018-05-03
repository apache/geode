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
 * Village.java
 *
 * Created on September 30, 2005, 6:23 PM
 */

package org.apache.geode.cache.query.data;

import java.io.Serializable;

public class Village implements Serializable {
  public String name;
  public int zip;

  /** Creates a new instance of Village */
  public Village(String name, int zip) {
    this.name = name;
    this.zip = zip;
  }// end of constructor 1

  public Village(int i) {
    String arr1[] =
        {"MAHARASHTRA_VILLAGE1", "PUNJAB_VILLAGE1", "KERALA_VILLAGE1", "GUJARAT_VILLAGE1"};
    this.name = arr1[i % 4];
    this.zip = 425125 + i;
  }// end of constructor 2

  ///////////////////////////////

  public String getName() {
    return name;
  }

  public int getZip() {
    return zip;
  }
}// end of class
