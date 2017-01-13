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
* @brief Example 13.8 Implementing Complex Data Types Using the Java API.
*/
public class ExampleObject implements DataSerializable {
  private double double_field;
  private long long_field;
  private float float_field;
  private int int_field;
  private short short_field;
  private java.lang.String string_field;
  private Vector string_vector;
  static {
    Instantiator.register(new Instantiator(ExampleObject.class, (byte) 46)
    {
      public DataSerializable newInstance() {
        return new ExampleObject();
      }
    });
  }
  public ExampleObject( ) {
  this.double_field = 0.0D;
  this.long_field = 0L;
  this.float_field = 0.0F;
  this.int_field = 0;
  this.short_field = 0;
  this.string_field = null;
  this.string_vector = null;
  }
  public ExampleObject(int id) {
  this.int_field = id;
  this.string_field = String.valueOf(id);
  this.short_field = Short.parseShort(string_field);
  this.double_field = Double.parseDouble(string_field);
  this.float_field = Float.parseFloat(string_field);
  this.long_field = Long.parseLong(string_field);
  this.string_vector = new Vector();
  for (int i=0; i<3; i++) {
  this.string_vector.addElement(string_field);
  }
  }
  public ExampleObject(String id_str) {
  this.int_field = Integer.parseInt(id_str);
  this.string_field = id_str;
  this.short_field = Short.parseShort(string_field);
  this.double_field = Double.parseDouble(string_field);
  this.float_field = Float.parseFloat(string_field);
  this.long_field = Long.parseLong(string_field);
  this.string_vector = new Vector();
  for (int i=0; i<3; i++) {
    this.string_vector.addElement(string_field);
  }
  }
  public double getDouble_field( ) {
  return this.double_field;
  }
  public void setDouble_field( double double_field ) {
  this.double_field = double_field;
  }
  public long getLong_field( ) {
  return this.long_field;
  }
  public void setLong_field( long long_field ) {
  this.long_field = long_field;
  }
  public float getFloat_field( ) {
  return this.float_field;
  }
  public void setFloat_field( float float_field ) {
  this.float_field = float_field;
  }
  public int getInt_field( ) {
  return this.int_field;
  }
  public void setInt_field( int int_field ) {
  this.int_field = int_field;
  }
  public short getShort_field( ) {
  return this.short_field;
  }
  public void setShort_field( short short_field ) {
  this.short_field = short_field;
  }
  public java.lang.String getString_field( ) {
  return this.string_field;
  }
  public void setString_field( java.lang.String string_field ) {
  this.string_field = string_field;
  }
  public Vector getString_vector( ) {
  return this.string_vector;
  }
  public void setString_vector( Vector string_vector ) {
  this.string_vector = string_vector;
  }
  public void toData(DataOutput out) throws IOException
  {
    out.writeDouble(double_field);
    out.writeFloat(float_field);
    out.writeLong(long_field);
    out.writeInt(int_field);
    out.writeShort(short_field);
    out.writeUTF(string_field);
    out.writeInt(string_vector.size());
    for (int i = 0; i < string_vector.size(); i++)
    {
      out.writeUTF((String)string_vector.elementAt(i));
    }
  }
  public void fromData(DataInput in) throws IOException,
  ClassNotFoundException
  {
    this.double_field = in.readDouble();
    this.float_field = in.readFloat();
    this.long_field = in.readLong();
    this.int_field = in.readInt();
    this.short_field = in.readShort();
    this.string_field = in.readUTF();
    this.string_vector = new Vector();
    int size = in.readInt();
    for (int i = 0; i < size; i++)
    {
      String s = in.readUTF();
      string_vector.add(i, s);
    }
  }
  public boolean equals(ExampleObject o)
  {
    if (this.double_field != o.double_field) return false;
    if (this.float_field != o.float_field) return false;
    if (this.long_field != o.long_field) return false;
    if (this.int_field != o.int_field) return false;
    if (this.short_field != o.short_field) return false;
    if (!this.string_field.equals(o.string_field)) return false;
    if (!this.string_vector.equals(o.string_vector)) return false;
    return true;
  }
}
