/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
//package org.apache.geode.cache.query.data;
package javaobject;

import java.util.*;
import java.io.*;
import org.apache.geode.*;
import org.apache.geode.cache.Declarable;


/**
* @brief Example 13.7 Implementing an Embedded Object Using the Java API.
*/
public class User implements DataSerializable
{
  private String name;
  private int userId;
  private ExampleObject eo;
  static
  {
    Instantiator.register(
      new Instantiator(User.class, (byte)45)
    {
      public DataSerializable newInstance()
      {
        return new User();
      }
    }
  );
  }
  /**
* Creates an "empty" User whose contents are filled in by
* invoking its toData() method
*/
  private User()
    {
      this.name = "";
      this.userId = 0;
    this.eo = new ExampleObject(0);
  }
  public User(String name, int userId)
  {
    this.name = name;
    this.userId = userId;
    this.eo = new ExampleObject(userId);
  }
  public void setEO(ExampleObject eo)
  {
    this.eo = eo;
  }
  public ExampleObject getEO()
  {
    return eo;
  }
  public void toData(DataOutput out) throws IOException
  {
    out.writeUTF(this.name);
    out.writeInt(this.userId);
    eo.toData(out);
  }
  public void fromData(DataInput in) throws IOException,
  ClassNotFoundException
  {
    this.name = in.readUTF();
    this.userId = in.readInt();
    this.eo.fromData(in);
  }
  public int getUserId()
  {
    return userId;
  }
  public String getName()
  {
    return name;
  }
  public boolean equals(User o)
  {
    if (!this.name.equals(o.name)) return false;
    if (this.userId != o.userId) return false;
    if (!this.eo.equals(o.eo)) return false;
    return true;
  }
}
