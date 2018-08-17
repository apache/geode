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
 * District.java
 *
 * Created on September 30, 2005, 6:16 PM
 */

package org.apache.geode.cache.query.data;

import java.io.Serializable;
import java.util.Set;

public class District implements Serializable {
  public String name;
  public Set cities;
  public Set villages;

  /** Creates a new instance of District */
  public District(String name, Set cities, Set villages) {
    this.name = name;
    this.cities = cities;
    this.villages = villages;
  }// end of contructor 1

  public District(int i, Set cities, Set villages) {
    String arr1[] = {"MUMBAIDIST", "PUNEDIST", "GANDHINAGARDIST", "CHANDIGARHDIST", "KOLKATADIST"};
    /* this is for the test to have 20% of the objects belonging to one districtr */
    this.name = arr1[i % 5];
    this.cities = cities;
    this.villages = villages;
  }// end of contructor 2

  ///////////////////////////////

  public String getName() {
    return name;
  }

  public Set getCities() {
    return cities;
  }

  public Set getVillages() {
    return villages;
  }
}// end of class
