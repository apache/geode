/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.dto;

import java.io.Serializable;


/**
 * Sample class for Data DUnit tests with JSON keys and values
 * @author tushark
 *
 */
 
public class Value2 implements Serializable{
  
  private String stateName;
  private String capitalCity;
  private int population;
  private double areaInSqKm;
  
  public Value2(int i){
    this.stateName = "stateName" +i;
    this.capitalCity = "capitalCity" + i;    
    this.population = i*1000;
    this.areaInSqKm =  i;
  }
  
  public Value2() {    
  }

  public boolean equals(Object other){
    if(other instanceof Value2){
      Value2 v2 = (Value2)other;
      return v2.stateName.equals(stateName);
    }else return false;
  }
  
  public int hashCode(){
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
  
  public String toString(){
    StringBuilder sb = new StringBuilder();
    sb.append(" Value2 [ stateName : ").append(stateName)
      .append(" capitalCity : ").append(capitalCity)
      .append(" population : ").append(population)
      .append(" areaInSqKm : ").append(areaInSqKm).append(" ]");
    return sb.toString();
  }

}
