/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package javaobject;

import org.apache.geode.internal.NanoTimer;
import org.apache.geode.cache.util.ObjectSizer;

import java.util.*;
import java.io.*;
import org.apache.geode.*;
import org.apache.geode.cache.Declarable;

/**
 * An account containing assets, useful for queries.
 */
public class FastAssetAccount implements Declarable,Serializable,DataSerializable {

  public static boolean encodeTimestamp = false;
  //protected  boolean encodeTimestamp;
  protected int acctId;
  protected String customerName;
  protected double netWorth = 0.0;
  protected Map assets;
  protected long timestamp;
  
  static {
     Instantiator.register(new Instantiator(FastAssetAccount.class, (byte) 23) {
       public DataSerializable newInstance() {
           return new FastAssetAccount();
       }
    });
  }

  public void init(Properties props) {
    this.acctId = Integer.parseInt(props.getProperty("acctId"));
	this.customerName = props.getProperty("customerName");     
	if(props.getProperty("netWorth") != null) { 
	  this.netWorth = Double.parseDouble( props.getProperty("netWorth") );
	}
  }

  public FastAssetAccount() {
  }

  public FastAssetAccount(int index,int maxVal,int asstSize) {
    this.acctId = index;
    this.customerName = "Milton Moneybags";

    this.assets = new HashMap();
    int size = asstSize;
    this.netWorth = 0.0;
    for (int i = 0; i < size; i++) {
      FastAsset asset = new FastAsset(i,maxVal);
      this.assets.put(new Integer(i), asset);
      this.netWorth += asset.getValue();
    }

    if (encodeTimestamp) {
      this.timestamp = NanoTimer.getTime();
    }
  }

  public int getAcctId() {
    return this.acctId;
  }

  public String getCustomerName() {
    return this.customerName;
  }

  public double getNetWorth() {
    return this.netWorth;
  }

  public void incrementNetWorth() {
    ++this.netWorth;
  }

  public Map getAssets(){
    return this.assets;
  }

//------------------------------------------------------------------------------
// ConfigurableObject

  public int getIndex() {
    return this.acctId;
  }

  // TimestampedObject

  public long getTimestamp() {
    if (encodeTimestamp) {
      return this.timestamp;
    } else {
      return this.timestamp = 0;
    }
  }

  public void resetTimestamp() {
    if (encodeTimestamp) {
      this.timestamp = NanoTimer.getTime();
    } else {
      this.timestamp = 0;
    }
  }

//------------------------------------------------------------------------------
// UpdatableObject

  public synchronized void update() {
    incrementNetWorth();
    if (encodeTimestamp) {
      resetTimestamp();
    }
  }

//------------------------------------------------------------------------------
// miscellaneous

  public boolean equals(Object obj) {
    if (obj != null && obj instanceof FastAssetAccount) { 
    	FastAssetAccount acct = (FastAssetAccount)obj;
      if (this.acctId == acct.acctId) {
        return true;
      }
    }
    return false;
  }

  public int hashCode() {
    return this.acctId;
  }

  public String toString() {
    StringBuffer buf = new StringBuffer();
    buf.append("FastAssetAccount [acctId=" + this.acctId
              + " customerName=" + this.customerName
              + " netWorth=" + this.netWorth
              + " timestamp=" + this.timestamp);
    for (Iterator i = assets.keySet().iterator(); i.hasNext();) {
      Object key = i.next();
      buf.append(" " + key + "=" + assets.get(key));
    }
    return buf.toString();
  }

 public void toData(DataOutput out)
 throws IOException {
   out.writeInt(this.acctId);
   DataSerializer.writeObject(this.customerName, out);
   out.writeDouble(this.netWorth);
   DataSerializer.writeHashMap((HashMap)this.assets, out);
   out.writeLong(this.timestamp);
 }
 public void fromData(DataInput in)
 throws IOException, ClassNotFoundException {
   this.acctId = in.readInt();
   this.customerName = (String)DataSerializer.readObject(in);
   this.netWorth = in.readDouble();
   this.assets = DataSerializer.readHashMap(in);
   this.timestamp = in.readLong();
 }

//------------------------------------------------------------------------------
//ObjectSizer
  public int sizeof(Object o) {
	if (o instanceof FastAssetAccount) {
	  FastAssetAccount obj = (FastAssetAccount) o;
	  int mapSize = 0;
	  if (obj.assets != null) {
		for (Iterator i = obj.assets.keySet().iterator(); i.hasNext();) {
		  Object key = i.next();
		  FastAsset asset = (FastAsset) obj.assets.get(key);
		  mapSize += ObjectSizer.DEFAULT.sizeof(key) + ObjectSizer.DEFAULT.sizeof(asset) + 16; // 32-bit
		  // add guess at hashmap entry overhead
		}
	  }
	  return ObjectSizer.DEFAULT.sizeof(obj.acctId) + ObjectSizer.DEFAULT.sizeof(obj.customerName)
			+ ObjectSizer.DEFAULT.sizeof(obj.netWorth) + mapSize
			+ ObjectSizer.DEFAULT.sizeof(obj.timestamp);
	  } else {
		return ObjectSizer.DEFAULT.sizeof(o);
	  }
	}

}
