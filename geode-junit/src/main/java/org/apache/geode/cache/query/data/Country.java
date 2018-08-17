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
 * Country.java
 *
 * Created on September 30, 2005, 6:02 PM
 */

package org.apache.geode.cache.query.data;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class Country implements Serializable {
  public String name;
  public String continent;
  public Set states;

  /** Creates a new instance of Country */

  public Country(String name, String continent, Set states) {
    this.name = name;
    this.continent = continent;
    this.states = states;
  }// end of constructor 1

  public Country(int i, Set states) {
    String arr1[] = {"USA", "INDIA", "ISRAEL", "CANADA", "AUSTRALIA"};
    String arr2[] = {"AMERICA", "ASIA", "AFRICA", "AMERICA", "AUSTRALIA"};
    /* this is for the test to have 20% of the objects belonging to one country */
    this.name = arr1[i % 5];
    this.continent = arr2[i % 5];
    this.states = states;
  }// end of constructor 2

  public Country(int i, int numStates, int numDistricts, int numCities, int numVillages) {
    String arr1[] = {"USA", "INDIA", "ISRAEL", "CANADA", "AUSTRALIA"};
    String arr2[] = {"AMERICA", "ASIA", "AFRICA", "AMERICA", "AUSTRALIA"};
    /* this is for the test to have 20% of the objects belonging to one country */
    this.name = arr1[i % 5];
    this.continent = arr2[i % 5];

    //////// create villages
    Set villages = new HashSet();
    for (int j = 0; j < numVillages; j++) {
      villages.add(new Village(j));
    }

    //////// create cities
    Set cities = new HashSet();
    for (int j = 0; j < numCities; j++) {
      cities.add(new City(j));
    }

    //////// create districts
    Set districts = new HashSet();
    for (int j = 0; j < numDistricts; j++) {
      districts.add(new District(j, cities, villages));
    }

    //////// create states
    Set states = new HashSet();
    for (int j = 0; j < numStates; j++) {
      states.add(new State(j, districts));
    }

    this.states = states;
  }// end of constructor 3

  //////////////////////////////////
  public String getName() {
    return name;
  }

  public String getRegion() {
    return continent;
  }

  public Set getStates() {
    return states;
  }
  //////////////////////////////////
}// end of class
