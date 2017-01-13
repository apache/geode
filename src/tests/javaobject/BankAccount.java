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


public class BankAccount implements Declarable, Serializable, DataSerializable
{
  private int customerId;
  private int accountId;

  static
  {
    Instantiator.register(new Instantiator(BankAccount.class, (byte)11)
    {
      public DataSerializable newInstance()
      {
        return new BankAccount();
      }
    });
  }

  public void init(Properties props)
  {
    this.customerId = Integer.parseInt(props.getProperty("customerId"));
    this.accountId = Integer.parseInt(props.getProperty("accountId"));
  }

  /* public no-arg constructor required for DataSerializable */
  public BankAccount() { }

  public BankAccount(int customerNum, int accountNum)
  {
    customerId = customerNum;
    accountId = accountNum;
  }

  public static void showAccountIdentifier()
  {   
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException
  {
    this.customerId = in.readInt();
    this.accountId = in.readInt();
  }

  public void toData(DataOutput out) throws IOException
  {
    out.writeInt(this.customerId);
    out.writeInt(this.accountId);
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
    if (!(other instanceof BankAccount)) return false;

    BankAccount pos = (BankAccount)other;

    if (this.customerId != pos.customerId) return false;
    if (this.accountId != pos.accountId) return false;

    return true;
  }

  public int hashCode()
  {
    Integer customer = new Integer(customerId);
    Integer id = new Integer(accountId);

    int hashcode =
    customer.hashCode() ^
    id.hashCode();

    return hashcode;
  }
}
