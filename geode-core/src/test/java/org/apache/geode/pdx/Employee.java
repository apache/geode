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
package org.apache.geode.pdx;

public class Employee implements PdxSerializable {
  private Long id;
  private String fname;
  private String lname;

  public Employee() {}

  public Employee(Long id, String fn, String ln) {
    this.id = id;
    this.fname = fn;
    this.lname = ln;
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
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
    this.id = in.readLong("id");
    this.fname = in.readString("fname");
    this.lname = in.readString("lname");
  }

  @Override
  public void toData(PdxWriter out) {
    out.writeLong("id", id);
    out.writeString("fname", fname);
    out.writeString("lname", lname);
  }

  @Override
  public String toString() {
    return "Employee [Id=" + id + ", fname=" + fname + ", lname=" + lname + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((fname == null) ? 0 : fname.hashCode());
    result = prime * result + ((id == null) ? 0 : id.hashCode());
    result = prime * result + ((lname == null) ? 0 : lname.hashCode());
    return result;
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
    if (fname == null) {
      if (other.fname != null)
        return false;
    } else if (!fname.equals(other.fname))
      return false;
    if (id == null) {
      if (other.id != null)
        return false;
    } else if (!id.equals(other.id))
      return false;
    if (lname == null) {
      if (other.lname != null)
        return false;
    } else if (!lname.equals(other.lname))
      return false;
    return true;
  }


}
