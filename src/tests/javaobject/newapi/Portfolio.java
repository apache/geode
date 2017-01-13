/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
//package org.apache.geode.cache.query.data;
package javaobject.newapi;


import java.util.*;
import java.io.*;
import org.apache.geode.*; // for DataSerializable
import org.apache.geode.cache.Declarable;


public class Portfolio implements Declarable, Serializable, DataSerializable {

  public int ID;
  public String pkid;
  public Position position1;
  public Position position2;
  public HashMap positions = new HashMap();
  public String type;
  public String status;
  public String [] names={"aaa","bbb","ccc","ddd"};
  public byte[] newVal;
  public Date creationDate = new Date();
  public byte[] arrayZeroSize;
  public byte[] arrayNull;

  /**
   * Initializes an instance of <code>Portfolio</code> from a
   * <code>Properties</code> object assembled from data residing in a
   * <code>cache.xml</code> file.
   */
  public void init(Properties props) {
    this.ID = Integer.parseInt(props.getProperty("ID"));
    this.pkid = props.getProperty("pkid");
    this.type = props.getProperty("type", "type1");
    this.status = props.getProperty("status", "active");
    // get the positions. These are stored in the properties object
    // as Positions, not String, so use Hashtable protocol to get at them.
    // the keys are named "positionN", where N is an integer.
    for (Iterator itr = props.entrySet().iterator(); itr.hasNext(); ) {
      Map.Entry entry = (Map.Entry)itr.next();
      String key = (String)entry.getKey();
      if (key.startsWith("position")) {
        Position pos = (Position)entry.getValue();
        this.positions.put(pos.getSecId(), pos);
      }
    }
  }

  public Portfolio select(){
    return this;
  }
  public int getID() {
    return ID;
  }

  public String getPk() {
    return pkid;
  }
  public HashMap getPositions() {
    return positions;
  }

  public HashMap getPositions(String str) {
    return positions;
  }

  public HashMap getPositions(Integer i) {
    return positions;
  }

  public HashMap getPositions(int i) {
    return positions;
  }

  public Position getP1() {
    return position1;
  }

  public Position getP2() {
    return position2;
  }

  public Date getCreationDate() {
    return creationDate;
  }

  public boolean testMethod(boolean booleanArg) {
    return true;
  }

  public boolean isActive() {
    return status.equals("active");
  }
  public byte[] getNewVal(){
    return this.newVal;
  }
  public byte[] ArrayZeroSize(){
      return this.arrayZeroSize;
  }
  public byte[] ArrayNull(){
      return this.arrayNull;
  }

  static {
     Instantiator.register(new Instantiator(Portfolio.class, (byte) 8) {
     public DataSerializable newInstance() {
        return new Portfolio();
     }
   });
 }


  public static String secIds[] = { "SUN", "IBM", "YHOO", "GOOG", "MSFT",
      "AOL", "APPL", "ORCL", "SAP", "DELL", "RHAT", "NOVL", "HP"};

  /* public no-arg constructor required for Deserializable */
  public Portfolio() {
  }

  public Portfolio(int i, int size) {
    ID = i;
    pkid = "" + i;
    status = i % 2 == 0 ? "active" : "inactive";
    type = "type" + (i % 3);
    position1 = new Position(secIds[Position.cnt % secIds.length],
        Position.cnt * 1000);
    if (i % 2 != 0) {
      position2 = new Position(secIds[Position.cnt % secIds.length],
          Position.cnt * 1000);
    }
    else {
      position2 = null;
    }
    positions.put(secIds[Position.cnt % secIds.length], new Position(secIds[Position.cnt % secIds.length], Position.cnt * 1000));
    newVal = new byte[size];
    for(int index = 0; index < size; index++) {
      newVal[index] = 'B';
    }
    arrayZeroSize = new byte[0];
    arrayNull = null;
  }

  public String toString() {
    String out = "Portfolio [ID=" + ID + " status=" + status + " type=" + type
        + "pkid=" + pkid + "creationDate=" + creationDate + "\n ";
    Iterator iter = positions.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry entry = (Map.Entry) iter.next();
      out += entry.getKey() + ":" + entry.getValue() + ", ";
    }
    out += "\n P1:" + position1 + ", P2:" + position2;
    return out + "\n]";
  }

  /**
   * Getter for property type.S
   *
   * @return Value of property type.
   */
  public String getType() {
    return this.type;
  }

  public boolean boolFunction(String strArg){
      if(strArg=="active"){
      return true;
      }
      else{
          return false;
      }
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
    this.ID = in.readInt();
	  this.pkid = in.readUTF();
    this.position1 = (Position)DataSerializer.readObject(in);
    this.position2 = (Position)DataSerializer.readObject(in);
    this.positions = (HashMap)DataSerializer.readHashMap(in);
    this.type = in.readUTF();
    this.status = in.readUTF();
    this.names = (String[])DataSerializer.readObject(in);
    this.newVal = (byte[])DataSerializer.readByteArray(in);
    this.creationDate = DataSerializer.readDate(in);
    this.arrayNull = DataSerializer.readByteArray(in);
    this.arrayZeroSize = DataSerializer.readByteArray(in);
  }
  
  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.ID);
	  out.writeUTF(this.pkid);
    DataSerializer.writeObject(this.position1, out);
    DataSerializer.writeObject(this.position2, out);
	  DataSerializer.writeHashMap(this.positions,out);
	  out.writeUTF(this.type);
    out.writeUTF(this.status);
    DataSerializer.writeObject(this.names, out);
    DataSerializer.writeByteArray(this.newVal,out);
    DataSerializer.writeDate(this.creationDate,out);
    DataSerializer.writeByteArray(this.arrayNull, out);
    DataSerializer.writeByteArray(this.arrayZeroSize, out);
  }
  
  public static boolean compareForEquals(Object first, Object second) {
    if (first == null && second == null) return true;
    if (first != null && first.equals(second)) return true;
    return false;
  }

  public boolean equals(Object other) {
    if (other==null) return false;
    if (!(other instanceof Portfolio)) return false;

    Portfolio port = (Portfolio) other;

    if (this.ID != port.ID) return false;

    if (!Portfolio.compareForEquals(this.pkid, port.pkid)) return false;
    if (!Portfolio.compareForEquals(this.position1, port.position1)) return false;
    if (!Portfolio.compareForEquals(this.position2, port.position2)) return false;
    if (!Portfolio.compareForEquals(this.positions, port.positions)) return false;
    if (!Portfolio.compareForEquals(this.type, port.type)) return false;
    if (!Portfolio.compareForEquals(this.status, port.status)) return false;
    if (!Portfolio.compareForEquals(this.names, port.names)) return false;
    if (!Portfolio.compareForEquals(this.newVal, port.newVal)) return false;
    if (!Portfolio.compareForEquals(this.creationDate, port.creationDate)) return false;

    return true;
  }

  public int hashCode() {
    int hashcode = ID;
    if (this.pkid != null) hashcode ^= pkid.hashCode();
    if (this.position1 != null) hashcode ^= position1.hashCode();
    if (this.position2 != null) hashcode ^= position2.hashCode();
    if (this.positions != null) hashcode ^= positions.hashCode();
    if (this.type != null) hashcode ^= type.hashCode();
    if (this.status != null) hashcode ^= status.hashCode();
    if (this.names != null) hashcode ^= names.hashCode();
    if (this.newVal != null) hashcode ^= newVal.hashCode();
    if (this.creationDate != null) hashcode ^= creationDate.hashCode();
    return hashcode;
  }
}


