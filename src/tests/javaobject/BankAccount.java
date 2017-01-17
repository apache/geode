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
