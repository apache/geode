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
 * State.java
 *
 * Created on September 30, 2005, 6:10 PM
 */

package org.apache.geode.cache.query.data;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class State implements Serializable {
  public String name;
  public String zone;
  public Set districts;

  /** Creates a new instance of State */
  public State(String name, String zone, Set districts) {
    this.name = name;
    this.zone = zone;
    this.districts = districts;
  }// end of contructor 1

  public State(int i, Set districts) {
    String arr1[] = {"MAHARASHTRA", "GUJARAT", "PUNJAB", "KERALA", "AASAM"};
    String arr2[] = {"WEST", "WEST", "NORTH", "SOUTH", "EAST"};
    /* this is for the test to have 33.33% of the objects belonging to one state */
    this.name = arr1[i % 3];
    this.zone = arr2[i % 3];
    this.districts = districts;
  }// end of contructor 2

  //////////////////////////////

  public String getName() {
    return name;
  }

  public String getZone() {
    return zone;
  }

  public Set getDistricts() {
    return districts;
  }


  public Set getDistrictsWithSameName(District dist) {
    Set districtsWithSameName = new HashSet();
    Iterator itr2 = districts.iterator();
    District dist1;
    while (itr2.hasNext()) {
      dist1 = (District) itr2.next();
      if (dist1.getName().equalsIgnoreCase(dist.getName())) {
        districtsWithSameName.add(dist1);
      }
    }
    return districtsWithSameName;
  }// end of getDistrictsWithSameName

}// end of class
