/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.examples.ds;

import com.gemstone.gemfire.DataSerializer;
import java.io.*;

public class CompanySerializer extends DataSerializer {

  static {
    DataSerializer.register(CompanySerializer.class);
  }

  /**
   * May be invoked reflectively if instances of Company are
   * distributed to other VMs.
   */
  public CompanySerializer() {

  }

  public int getId() {
    return 42;
  }
  
  public Class[] getSupportedClasses() {
    return new Class[] { Company.class };
  }

  public boolean toData(Object o, DataOutput out)
    throws IOException {
    if (o instanceof Company) {
      Company company = (Company) o;
      out.writeUTF(company.getName());

      // Let's assume that Address is java.io.Serializable
      Address address = company.getAddress();
      writeObject(address, out);
      return true;

    } else {
      return false;
    }
  }

  public Object fromData(DataInput in)
    throws IOException, ClassNotFoundException {

    String name = in.readUTF();
    Address address = (Address) readObject(in);
    return new Company(name, address);
  }
}
