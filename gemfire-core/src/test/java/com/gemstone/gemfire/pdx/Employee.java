/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.pdx;

public class Employee implements PdxSerializable
{
  private Long Id;
  private String fname;
  private String lname;
  
  public Employee(){}
  
  public Employee(Long id, String fn, String ln){
    this.Id = id;
    this.fname = fn;
    this.lname = ln;
  }
  
  public Long getId() {
    return Id;
  }
  
  public void setId(Long id) {
    Id = id;
  }
  
  public String getFname() {
    return fname;
  }
  
  public void setFname(String fname) {
    this.fname = fname;
  }
  
  public String getLname() {
    return lname;
  }
  
  public void setLname(String lname) {
    this.lname = lname;
  }

  @Override
  public void fromData(PdxReader in) {
    this.Id = in.readLong("Id");
    this.fname = in.readString("fname");
    this.lname = in.readString("lname");
  }

  @Override
  public void toData(PdxWriter out) {
    out.writeLong("Id", Id);
    out.writeString("fname", fname);
    out.writeString("lname", lname);
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
      
    Employee other = (Employee) obj;
    if( Id.longValue() != other.Id.longValue() )
      return false;
    
    if(!fname.equals(other.fname))
      return false;
    
    if(!lname.equals(other.lname))
      return false;
    
    return true;
  }
}