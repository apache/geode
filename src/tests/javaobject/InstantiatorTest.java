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


public class InstantiatorTest implements  Declarable,Serializable, DataSerializable {

  public boolean m_bool;
  public int m_int;
  public int[] m_intArray;
  public String m_fileName;
  public String  m_string;
  public String[] m_stringArray;
  public HashSet m_hashset;
  public HashMap<String, String> m_hashmap;
  public Date m_date;
  public Vector m_vector;
  public byte[] m_object;
  
  private boolean m_initialized = false;
  
  /**
   * Initializes an instance of <code>Portfolio</code> from a
   * <code>Properties</code> object assembled from data residing in a
   * <code>cache.xml</code> file.
   */
  public void init(Properties props) {
      m_bool = true;
      m_int = 1000;
      m_intArray = new int[3];
      m_intArray[0] = 1;
      m_intArray[1] = 2;
      m_intArray[2] = 3;
      m_fileName = "gemstone.txt";
      m_string = "asciistring";
      m_stringArray = new String[2];
      m_stringArray[0] = "one";
      m_stringArray[1] = "two";
      m_hashset = new HashSet();
      m_hashset.add("first");
      m_hashset.add("second");
      m_hashmap = new HashMap<String, String>();
      m_hashmap.put("key-hm", "value-hm");
      m_date = new Date();
      m_vector = new Vector();
      m_object = new byte[100];
  }
  
  public InstantiatorTest select(){
    return this;
  }
  public boolean getBool() {
    return m_bool;
  }

  public int getInt() {
    return m_int;
  }
  public int[] getIntArray() {
    return m_intArray;
  }
  
  public String getFileName() {
    return m_fileName;
  }

  public String getString() {
    return m_string;
  }
  
  public String [] getStringArray() {
    return m_stringArray;
  }
  
  public HashSet getHashSet() {
    return m_hashset;
  }
  
  public HashMap getHashMap() {
    return m_hashmap;
  }
  
  public Date getDate() {
    return m_date;
  }
  
  public Vector getVector() {
    return m_vector;
  }
  
  public byte [] getObject() {
    return m_object;
  }
  /*
  static {
     Instantiator.register(new Instantiator(InstantiatorTest.class, (byte) 4) {
     public DataSerializable newInstance() {
        return new InstantiatorTest();
     }
   });
 }
*/
  /* public no-arg constructor required for Deserializable */
  public InstantiatorTest() {
  }

  public InstantiatorTest(boolean initialized) {
    if (initialized) {
      m_initialized = true;
      m_bool = true;
      m_int = 1000;
      m_intArray = new int[3];
      m_intArray[0] = 1;
      m_intArray[1] = 2;
      m_intArray[2] = 3;
      m_fileName = "gemstone.txt";
      m_string = "asciistring";
      m_stringArray = new String[2];
      m_stringArray[0] = "one";
      m_stringArray[1] = "two";
      m_hashset = new HashSet();
      m_hashset.add("first");
      m_hashset.add("second");
      m_hashmap = new HashMap<String, String>();
      m_hashmap.put("key-hm", "value-hm");
      m_date = new Date();
      m_vector = new Vector();
      m_vector.add("one-vec");
      m_vector.add("two-vec");
      m_object = new byte[100];
    }
  }

  public String toString() {
    String out = "DefaultCacehable [m_bool=" + m_bool + "\n "
                                    + "m_int"  + m_int + "\n"
                                     +"m_fileName" +m_fileName + "\n"
                                      +"m_string" + m_string + "\n"  ;
    return out + "\n]";
  }

  public static void writeHashmap( DataOutput output, HashMap map ) throws IOException
  {
    output.writeInt( map.size() );
    for (Iterator iter = map.entrySet().iterator();iter.hasNext();) {
      Map.Entry entry = (Map.Entry) iter.next();
      String mykey = (String)entry.getKey();
      DataSerializer.writeString(mykey,output);
      DataSerializer.writeObject(entry.getValue(),output);
    }
  }

  public static HashMap readHashmap( DataInput input ) throws IOException, ClassNotFoundException
  {
    HashMap map = new HashMap( );
    int mapSize = input.readInt();
    for (int i = 0; i < mapSize; i++) {
      String key = DataSerializer.readString(input);
      Position pos = (Position)DataSerializer.readObject(input);
      map.put(key,pos);
    }
    return map;
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    m_bool = in.readBoolean();
    m_int = in.readInt();
    m_intArray = DataSerializer.readIntArray(in);
    //m_fileName = (String)DataSerializer.readString(in);
    //m_string = (String)DataSerializer.readString(in);
    m_stringArray = (String [])DataSerializer.readStringArray(in);
    m_hashset = (HashSet)DataSerializer.readHashSet(in);
    m_hashmap = (HashMap)DataSerializer.readHashMap(in);
    m_date = (Date)DataSerializer.readDate(in);
    m_vector = (Vector)DataSerializer.readVector(in);
    m_object = (byte[])DataSerializer.readByteArray(in);
  }

  public void toData(DataOutput out) throws IOException {
    out.writeBoolean(m_bool);
    out.writeInt(m_int);
    DataSerializer.writeIntArray(m_intArray, out);
    //DataSerializer.writeString(m_fileName,out);
    //DataSerializer.writeString(m_string,out);
    DataSerializer.writeStringArray(m_stringArray,out);
    DataSerializer.writeHashSet(m_hashset, out);
    DataSerializer.writeHashMap(m_hashmap, out);
    DataSerializer.writeDate(m_date, out);
    DataSerializer.writeVector(m_vector, out);
    DataSerializer.writeByteArray(m_object, out);
  }

  public static boolean compareForEquals(Object first, Object second) {
    if (first == null && second == null) return true;
    if (first != null && first.equals(second)) return true;
    return false;
  }

  public boolean equals(Object other) {
    if (other==null) return false;
    if (!(other instanceof InstantiatorTest)) return false;

    InstantiatorTest dc = (InstantiatorTest) other;

    if (this.m_bool != dc.getBool()) return false;

    return true;
  }

  public int hashCode() {
    return this.getInt();
  }
}


