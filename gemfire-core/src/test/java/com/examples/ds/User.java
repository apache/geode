/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.examples.ds;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.Instantiator;
import java.io.*;

public class User implements DataSerializable {
  private String name;
  private int userId;

  static {
    Instantiator.register(new Instantiator(User.class, (byte) 45) {
        public DataSerializable newInstance() {
          return new User();
        }
      });
  }

  public User(String name, int userId) {
    this.name = name;
    this.userId = userId;
  }

  /**
   * Creates an "empty" User whose contents are filled in by
   * invoking its toData() method
   */
  protected User() {

  }

  public void toData(DataOutput out) throws IOException {
    out.writeUTF(this.name);
    out.writeInt(this.userId);
  }

  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    this.name = in.readUTF();
    this.userId = in.readInt();
  }
}
