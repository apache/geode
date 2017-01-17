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
