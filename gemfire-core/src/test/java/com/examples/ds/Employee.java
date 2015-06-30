/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.examples.ds;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import java.io.*;
import java.util.Date;

public class Employee implements DataSerializable {
  private int id;
  private String name;
  private Date birthday;
  private Company employer;

  public Employee(int id, String name, Date birthday,
                  Company employer){
    this.id = id;
    this.name = name;
    this.birthday = birthday;
    this.employer = employer;
  }

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.id);
    out.writeUTF(this.name);
    DataSerializer.writeDate(this.birthday, out);
    DataSerializer.writeObject(this.employer, out);
  }

  public void fromData(DataInput in) 
    throws IOException, ClassNotFoundException {

    this.id = in.readInt();
    this.name = in.readUTF();
    this.birthday = DataSerializer.readDate(in);
    this.employer = (Company) DataSerializer.readObject(in);
  }
}
