/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.query.data;

import java.io.*;
import java.util.HashSet;
import java.util.Set;
import com.gemstone.gemfire.*;


public class Position implements Serializable, DataSerializable, Comparable {
  private long avg20DaysVol=0;
  private String bondRating;
  private double convRatio;
  private String country;
  private double delta;
  private long industry;
  private long issuer;
  public double mktValue;
  private double qty;
  public String secId;
  public String secIdIndexed;
  private String secLinks;
  public String secType;
  private double sharesOutstanding;
  public String underlyer;
  private long volatility;
  private int pid;
  public static int cnt = 0;
  public int portfolioId = 0;
  
  /* public no-arg constructor required for DataSerializable */  
  public Position() {}

  public Position(String id, double out) {
    secId = id;
    secIdIndexed = secId;
    sharesOutstanding = out;
    secType = "a";
    pid = cnt++;
    this.mktValue = cnt;
  }
  
  public boolean equals(Object o) {
    if (!(o instanceof Position)) return false;
    return this.secId.equals(((Position)o).secId);
  }
  
  public int hashCode() {
    return this.secId.hashCode();
  }
  
  
  public static void resetCounter() {
    cnt = 0;
  }
  
  public double getMktValue() {
    return this.mktValue;
  }
  
  public String getSecId(){
    return secId;
  }
  
  public int getId(){
    return pid;
  }
  
  public double getSharesOutstanding(){
    return sharesOutstanding;
  }
  
  public String toString(){
    return "Position [secId=" + this.secId + " out=" + this.sharesOutstanding
           + " type=" + this.secType + " id=" + this.pid + " mktValue="
           + this.mktValue + "]";
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
  
  public int getPid(){
    return pid;
  }
  
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.avg20DaysVol = in.readLong();
    this.bondRating = DataSerializer.readString(in);
    this.convRatio = in.readDouble();
    this.country = DataSerializer.readString(in);
    this.delta = in.readDouble();
    this.industry = in.readLong();
    this.issuer = in.readLong();
    this.mktValue = in.readDouble();
    this.qty = in.readDouble();
    this.secId = DataSerializer.readString(in);
    this.secIdIndexed = DataSerializer.readString(in);
    this.secLinks = DataSerializer.readString(in);
    this.sharesOutstanding = in.readDouble();
    this.underlyer = DataSerializer.readString(in);
    this.volatility = in.readLong();
    this.pid = in.readInt();
    this.portfolioId = in.readInt();
  }
  
  public void toData(DataOutput out) throws IOException {
    out.writeLong(this.avg20DaysVol);
    DataSerializer.writeString(this.bondRating, out);
    out.writeDouble(this.convRatio);
    DataSerializer.writeString(this.country, out);
    out.writeDouble(this.delta);
    out.writeLong(this.industry);
    out.writeLong(this.issuer);
    out.writeDouble(this.mktValue);
    out.writeDouble(this.qty);
    DataSerializer.writeString(this.secId, out);
    DataSerializer.writeString(this.secIdIndexed, out);
    DataSerializer.writeString(this.secLinks, out);
    out.writeDouble(this.sharesOutstanding);
    DataSerializer.writeString(this.underlyer, out);
    out.writeLong(this.volatility);
    out.writeInt(this.pid);
    out.writeInt(this.portfolioId);
  }


  public int compareTo(Object o)
  {
    if( o == this) {
      return 0;
    }else {
      if (this.pid == ((Position)o).pid) return 0;
      else return this.pid < ((Position)o).pid ? -1:1;
    }
     
  } 
  
}
