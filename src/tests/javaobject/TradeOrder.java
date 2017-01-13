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
import org.apache.geode.*; // for DataSerializable
import org.apache.geode.cache.Declarable;


public class TradeOrder implements Declarable, Serializable, DataSerializable
{
  public int price;
  public String pkid;
  public String type;
  public String status;
  public String[] names ={ "aaa", "bbb", "ccc", "ddd" };
  public byte[] newVal;
  public Date creationDate = new Date();
  public byte[] arrayZeroSize;
  public byte[] arrayNull;

  /**
   * Initializes an instance of <code>Portfolio</code> from a
   * <code>Properties</code> object assembled from data residing in a
   * <code>cache.xml</code> file.
   */
    public void init(Properties props)
  {
    this.price = Integer.parseInt(props.getProperty("price"));
    this.pkid = props.getProperty("pkid");
    this.type = props.getProperty("type", "type1");
    this.status = props.getProperty("status", "active");
  }

  public int getPrice()
  {
    return price;
  }

  public String getPk()
  {
    return pkid;
  }

  public Date getCreationDate()
  {
    return creationDate;
  }

  public boolean testMethod(boolean booleanArg)
  {
    return true;
  }

  public boolean isActive()
  {
    return status.equals("active");
  }
  public byte[] getNewVal()
  {
    return this.newVal;
  }
  public byte[] ArrayZeroSize()
  {
    return this.arrayZeroSize;
  }
  public byte[] ArrayNull()
  {
    return this.arrayNull;
  }

  static
  {
    Instantiator.register(new Instantiator(TradeOrder.class, (byte)4)
    {
      public DataSerializable newInstance()
      {
        return new TradeOrder();
      }
    });
  }


  public static String secIds[] = { "SUN", "IBM", "YHOO", "GOOG", "MSFT",
      "AOL", "APPL", "ORCL", "SAP", "DELL", "RHAT", "NOVL", "HP"};

  /* public no-arg constructor required for Deserializable */
  public TradeOrder()
  {
  }
 
  public TradeOrder(int i, int size)
  {
    price = 0;
    pkid = null;
    status = null;
    type = null;
    newVal = null;
    creationDate = null;
    arrayZeroSize = null;
    arrayNull = null;
  }

  public String toString()
  {
    String out = "TradeOrder [price=" + price + " status=" + status + " type=" + type
        + "pkid=" + pkid + "creationDate=" + creationDate + "\n ";
    return out + "\n]";
  }

  /**
   * Getter for property type.S
   *
   * @return Value of property type.
   */
  public String getType()
  {
    return this.type;
  }

  public boolean boolFunction(String strArg)
  {
    if (strArg == "active")
    {
      return true;
    }
    else
    {
      return false;
    }
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException
  {
    this.price = in.readInt();
    this.pkid = (String)DataSerializer.readObject(in);
    this.type = (String)DataSerializer.readObject(in);
    this.status = in.readUTF();
    this.names = (String[])DataSerializer.readObject(in);
    this.newVal = (byte[])DataSerializer.readByteArray(in);
    this.creationDate = (Date)DataSerializer.readObject(in);
    this.arrayNull = DataSerializer.readByteArray(in);
    this.arrayZeroSize = DataSerializer.readByteArray(in);
  }

  public void toData(DataOutput out) throws IOException
  {
    out.writeInt(this.price);
    DataSerializer.writeObject(this.pkid, out);
    DataSerializer.writeObject(this.type, out);
    out.writeUTF(this.status);
    DataSerializer.writeObject(this.names, out);
    DataSerializer.writeByteArray(this.newVal, out);
    DataSerializer.writeObject(this.creationDate, out);
    DataSerializer.writeByteArray(this.arrayNull, out);
    DataSerializer.writeByteArray(this.arrayZeroSize, out);
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
    if (!(other instanceof Portfolio)) return false;

    TradeOrder port = (TradeOrder)other;

    if (this.price != port.price) return false;

    if (!TradeOrder.compareForEquals(this.pkid, port.pkid)) return false;
    if (!TradeOrder.compareForEquals(this.type, port.type)) return false;
    if (!TradeOrder.compareForEquals(this.status, port.status)) return false;
    if (!TradeOrder.compareForEquals(this.names, port.names)) return false;
    if (!TradeOrder.compareForEquals(this.newVal, port.newVal)) return false;
    if (!TradeOrder.compareForEquals(this.creationDate, port.creationDate)) return false;

    return true;
  }

  public int hashCode()
  {
    int hashcode = price;
    if (this.pkid != null) hashcode ^= pkid.hashCode();
    if (this.type != null) hashcode ^= type.hashCode();
    if (this.status != null) hashcode ^= status.hashCode();
    if (this.names != null) hashcode ^= names.hashCode();
    if (this.newVal != null) hashcode ^= newVal.hashCode();
    if (this.creationDate != null) hashcode ^= creationDate.hashCode();
    return hashcode;
  }
}


