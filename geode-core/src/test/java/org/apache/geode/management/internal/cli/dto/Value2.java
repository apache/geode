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
package org.apache.geode.management.internal.cli.dto;

import java.io.Serializable;


/**
 * Sample class for Data DUnit tests with JSON keys and values
 *
 */

public class Value2 implements Serializable {

  private String stateName;
  private String capitalCity;
  private int population;
  private double areaInSqKm;

  public Value2(int i) {
    this.stateName = "stateName" + i;
    this.capitalCity = "capitalCity" + i;
    this.population = i * 1000;
    this.areaInSqKm = i;
  }

  public Value2() {}

  public boolean equals(Object other) {
    if (other instanceof Value2) {
      Value2 v2 = (Value2) other;
      return v2.stateName.equals(stateName);
    } else
      return false;
  }

  public int hashCode() {
    return stateName.hashCode();
  }

  public String getStateName() {
    return stateName;
  }

  public void setStateName(String stateName) {
    this.stateName = stateName;
  }

  public String getCapitalCity() {
    return capitalCity;
  }

  public void setCapitalCity(String capitalCity) {
    this.capitalCity = capitalCity;
  }

  public int getPopulation() {
    return population;
  }

  public void setPopulation(int population) {
    this.population = population;
  }

  public double getAreaInSqKm() {
    return areaInSqKm;
  }

  public void setAreaInSqKm(double areaInSqKm) {
    this.areaInSqKm = areaInSqKm;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(" Value2 [ stateName : ").append(stateName).append(" capitalCity : ")
        .append(capitalCity).append(" population : ").append(population).append(" areaInSqKm : ")
        .append(areaInSqKm).append(" ]");
    return sb.toString();
  }

}
