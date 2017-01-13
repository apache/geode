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


public class TradeKey implements Declarable, Serializable, DataSerializable
{
  private int m_id;
  private int m_accountid;

  static
  {
    Instantiator.register(new Instantiator(TradeKey.class, (byte)4)
    {
      public DataSerializable newInstance()
      {
        return new TradeKey();
      }
    });
  }

  public void init(Properties props)
  {
    this.m_id = Integer.parseInt(props.getProperty("m_id"));
    this.m_accountid = Integer.parseInt(props.getProperty("m_accountid"));
  }

  /* public no-arg constructor required for DataSerializable */
  public TradeKey() { }

  public TradeKey(int customerNum, int accountNum)
  {
    m_id = customerNum;
    m_accountid = accountNum;
  }

  public static void showAccountIdentifier()
  {   
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException
  {
    this.m_id = in.readInt();
    this.m_accountid = in.readInt();
  }

  public void toData(DataOutput out) throws IOException
  {
    out.writeInt(this.m_id);
    out.writeInt(this.m_accountid);
  }

  public static boolean compareForEquals(Object first, Object second)
  {
    if (first == null && second == null) return true;
    if (first != null && first.equals(second)) return true;
    return false;
  }

  public boolean equals(Object other)
  {
    if (other == null) return false;
    if (!(other instanceof TradeKey)) return false;

    TradeKey pos = (TradeKey)other;

    if (this.m_id != pos.m_id) return false;
    if (this.m_accountid != pos.m_accountid) return false;

    return true;
  }

  public int hashCode()
  {
    Integer customer = new Integer(m_id);
    Integer id = new Integer(m_accountid);

    int hashcode =
    customer.hashCode() ^
    id.hashCode();

    return hashcode;
  }
  
  public int getId()
  {
    System.out.println("TradeKey::getId() java side m_id = " + m_id);
	return m_id;
  }
}
