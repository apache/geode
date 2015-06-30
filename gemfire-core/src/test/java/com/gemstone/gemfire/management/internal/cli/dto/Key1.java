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

public class Key1 implements Serializable{
  
  private String id;
  private String name;
  
  public String getId() {
    return id;
  }
  public void setId(String id) {
    this.id = id;
  }
  public String getName() {
    return name;
  }
  public void setName(String name) {
    this.name = name;
  }
  
  public boolean equals(Object other){
    if(other instanceof Key1){
      Key1 k1 = (Key1)other;
      return k1.id.equals(id);
    }else
      return false;
  }
  
  public int hashCode(){    
    return id.hashCode();
  }
  
  public String toString(){
    StringBuilder sb = new StringBuilder();
    sb.append(" Key1 [ id : ").append(id)
      .append(" name : ").append(name).append(" ]");
    return sb.toString();
  }

  

}
