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
 * City.java
 *
 * Created on September 30, 2005, 6:20 PM
 */

package org.apache.geode.cache.query.data;

import java.io.Serializable;

public class City implements Serializable {
  public static int ZIP_START = 300000;
  public int id;
  public String name;
  public int zip;

  /** Creates a new instance of City */
  public City(String name, int zip) {
    this.name = name;
    this.zip = zip;
  }// end of constructor 1

  public City(int i) {
    String arr1[] = {"MUMBAI", "PUNE", "GANDHINAGAR", "CHANDIGARH"};
    /* this is for the test to have 50% of the objects belonging to one city */
    this.name = arr1[i % 2];
    this.zip = ZIP_START + i;
    this.id = i;
  }// end of constructor 2

  ////////////////////////////

  public String getName() {
    return name;
  }

  public int getZip() {
    return zip;
  }
}// end of class
