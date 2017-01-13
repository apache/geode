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
import org.apache.geode.*;
import org.apache.geode.cache.Declarable;


public class Position implements Declarable, Serializable, DataSerializable {
  private long avg20DaysVol=0;
  private String bondRating = "";
  private double convRatio = 0;
  private String country ="";
  private double delta =0;
  private long industry =0;
  private long issuer =0;
  private double mktValue =0 ;
  private double qty =0;
  public String secId="";
  private String secLinks="";
  public String secType="";
  private int sharesOutstanding=0;
  public String underlyer="";
  private long volatility=0;
  private int pid=0;
  public static int cnt = 0;

  static {
     Instantiator.register(new Instantiator(Position.class, (byte) 7) {
     public DataSerializable newInstance() {
        return new Position();
     }
   });
  }

  public void init(Properties props) {
    this.secId = props.getProperty("secId");

    if(props.getProperty("qty") != null) { 
      this.qty = Double.parseDouble( props.getProperty("qty") );
    }

    if(props.getProperty("mktValue") != null) { 
      this.mktValue = Double.parseDouble( props.getProperty("mktValue") );
    }

    this.sharesOutstanding = Integer.parseInt(props.getProperty("sharesOutstanding"));
    this.secType = props.getProperty("secType");
    this.pid = Integer.parseInt(props.getProperty("pid"));
  }

  /* public no-arg constructor required for DataSerializable */  
  public Position() {}

  public Position(String id, int out){
    secId = id;
    sharesOutstanding = out;
    secType = "a";
    pid = cnt++;
  }
  
  public static void resetCounter() {
    cnt = 0;
  }
  public String getSecId(){
    return secId;
  }
  
  public int getId(){
    return pid;
  }
  
  public int getSharesOutstanding(){
    return sharesOutstanding;
  }
  
  public String toString(){
    return "Position [secId="+secId+" sharesOutstanding="+sharesOutstanding+ " type="+secType +" id="+pid+"]";
  }
  
  public Set getSet(int size){
    Set set = new HashSet();
    for(int i=0;i<size;i++){
      set.add(""+i);
    }
    return set;
  }
  
  public Set getCol(){
    Set set = new HashSet();
    for(int i=0;i<2;i++){
      set.add(""+i);
    }
    return set;
  }
  
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.avg20DaysVol = in.readLong();
    this.bondRating = in.readUTF();
	  this.convRatio = in.readDouble();
    this.country = in.readUTF(); 
	  this.delta = in.readDouble();
    this.industry = in.readLong();
    this.issuer = in.readLong(); 
    this.mktValue = in.readDouble();
    this.qty = in.readDouble(); 
    this.secId = in.readUTF();
    this.secLinks = in.readUTF();
	  this.secType = in.readUTF();
    this.sharesOutstanding = in.readInt();
    this.underlyer = in.readUTF();
	  this.volatility = in.readLong();
    this.pid = in.readInt();
  }
  
  public void toData(DataOutput out) throws IOException {
    out.writeLong(this.avg20DaysVol);
    out.writeUTF(this.bondRating);
	  out.writeDouble(this.convRatio);
    out.writeUTF(this.country);
	  out.writeDouble(this.delta);
    out.writeLong(this.industry);
    out.writeLong(this.issuer);
    out.writeDouble(this.mktValue);
    out.writeDouble(this.qty);
    out.writeUTF(this.secId);
    out.writeUTF(this.secLinks);
	  out.writeUTF(this.secType);
    out.writeInt(this.sharesOutstanding);
    out.writeUTF(this.underlyer);
	  out.writeLong(this.volatility);
    out.writeInt(this.pid);
  } 
  
  public static boolean compareForEquals(Object first, Object second) {
    if (first == null && second == null) return true;
    if (first != null && first.equals(second)) return true;
    return false;
  }
  
  public boolean equals(Object other) {
    if (other==null) return false;
    if (!(other instanceof Position)) return false;
    
    Position pos = (Position) other;
    
    if (this.avg20DaysVol != pos.avg20DaysVol) return false;
    if (this.convRatio != pos.convRatio) return false;
    if (this.delta != pos.delta) return false;
    if (this.industry != pos.industry) return false;
    if (this.issuer != pos.issuer) return false;
    if (this.mktValue != pos.mktValue) return false;
    if (this.qty != pos.qty) return false;
    if (this.sharesOutstanding != pos.sharesOutstanding) return false;
    if (this.volatility != pos.volatility) return false;
    if (this.pid != pos.pid) return false;

    if (!Position.compareForEquals(this.bondRating, pos.bondRating)) return false;
    if (!Position.compareForEquals(this.country, pos.country)) return false;
    if (!Position.compareForEquals(this.secId, pos.secId)) return false;
    if (!Position.compareForEquals(this.secLinks, pos.secLinks)) return false;
    if (!Position.compareForEquals(this.secType, pos.secType)) return false;
    if (!Position.compareForEquals(this.underlyer, pos.underlyer)) return false;
        
    return true;    
  }
  
  public int hashCode() {
    Long avg = new Long(avg20DaysVol);
    Double convRat = new Double(convRatio);
    Double del = new Double(delta);
    Long ind = new Long(industry);
    Long iss = new Long(issuer);
    Double mktVal = new Double(mktValue);
    Double quant = new Double(qty);
    Integer shout = new Integer(sharesOutstanding);
    Long vol = new Long(volatility);
    Integer id = new Integer(pid);
    
    int hashcode =
    avg.hashCode() ^
    convRat.hashCode() ^
    del.hashCode() ^
    ind.hashCode() ^
    iss.hashCode() ^
    mktVal.hashCode() ^
    quant.hashCode() ^
    shout.hashCode() ^
    vol.hashCode() ^
    id.hashCode();
    
    if (this.country != null) hashcode ^= country.hashCode();
    if (this.bondRating != null) hashcode ^= bondRating.hashCode();
    if (this.secId != null) hashcode ^= secId.hashCode();
    if (this.secLinks != null) hashcode ^= secLinks.hashCode();
    if (this.secType != null) hashcode ^= secType.hashCode();
    if (this.underlyer != null) hashcode ^= underlyer.hashCode();
    
    return hashcode;
  }
}
