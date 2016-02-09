/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
