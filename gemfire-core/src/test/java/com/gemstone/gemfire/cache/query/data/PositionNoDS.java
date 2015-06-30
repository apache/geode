package com.gemstone.gemfire.cache.query.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;


public class PositionNoDS implements Serializable, Comparable{
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
  public PositionNoDS() {}

  public PositionNoDS(String id, double out) {
    secId = id;
    secIdIndexed = secId;
    sharesOutstanding = out;
    secType = "a";
    pid = cnt++;
    this.mktValue = cnt;
  }
  
  public boolean equals(Object o) {
    if (!(o instanceof PositionNoDS)) return false;
    return this.secId.equals(((PositionNoDS)o).secId);
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
  
  


  public int compareTo(Object o)
  {
    if( o == this) {
      return 0;
    }else {
      if (this.pid == ((PositionNoDS)o).pid) return 0;
      else return this.pid < ((PositionNoDS)o).pid ? -1:1;
    }
     
  } 
  
}
