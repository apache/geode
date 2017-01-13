/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package javaobject;

import org.apache.geode.DataSerializable;
import org.apache.geode.Delta;
import org.apache.geode.Instantiator;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.cache.Declarable;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.*;
import java.io.*;
import java.util.*;

/**
 * A {@link objects.FastAssetAccount} that implements the Delta interface.
 */
public class DeltaFastAssetAccount implements Declarable,Serializable,DataSerializable ,Delta, Cloneable {

	  public static boolean encodeTimestamp = false;
	  //protected  boolean encodeTimestamp;
	  protected int acctId;
	  protected String customerName;
	  protected double netWorth = 0.0;
	  protected Map assets;
	  protected long timestamp;
	  protected static final boolean getBeforeUpdate = false;
	  
	  static {
	     Instantiator.register(new Instantiator(DeltaFastAssetAccount.class, (byte) 41) {
	       public DataSerializable newInstance() {
	           return new DeltaFastAssetAccount();
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
  
  
  public DeltaFastAssetAccount() {
  }
  public DeltaFastAssetAccount(int index,int maxVal,int asstSize) {
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
//
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
		//out.writeBoolean(this.encodeTimestamp);
		out.writeInt(this.acctId);
		DataSerializer.writeObject(this.customerName, out);
		out.writeDouble(this.netWorth);
		DataSerializer.writeObject(this.assets, out);
	    out.writeLong(this.timestamp);
	   }
	  public void fromData(DataInput in)
	  throws IOException, ClassNotFoundException {
		//this.encodeTimestamp = in.readBoolean();
		this.acctId = in.readInt();
		this.customerName = (String)DataSerializer.readObject(in);
		this.netWorth = in.readDouble();
		this.assets = (Map)DataSerializer.readObject(in);
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
//------------------------------------------------------------------------------
// Delta

  public boolean hasDelta() {
    return true;
  }

  public void toDelta(DataOutput out)
  throws IOException {
    out.writeDouble(this.netWorth);
    if (encodeTimestamp) {
      out.writeLong(this.timestamp);
    }
  }

  public void fromDelta(DataInput in)
  throws IOException {
    if (getBeforeUpdate) {
      this.netWorth = in.readDouble();
    } else {
      this.netWorth += in.readDouble();
    }
    if (encodeTimestamp) {
      this.timestamp = in.readLong();
    }
  
  }

//------------------------------------------------------------------------------
// Cloneable

  /**
   * Makes a deep copy of this account.
   */
  public Object clone() throws CloneNotSupportedException {
    DeltaFastAssetAccount acct = (DeltaFastAssetAccount)super.clone();
    acct.assets = new HashMap();
    for (Iterator i = this.assets.keySet().iterator(); i.hasNext();) {
      Integer key = (Integer)i.next();
      FastAsset asset = (FastAsset)this.assets.get(key);
      acct.assets.put(key, asset.copy());
    }
   
    return acct;
  }

//------------------------------------------------------------------------------
// miscellaneous

  public boolean equals(Object obj) {
    if (obj != null && obj instanceof DeltaFastAssetAccount) { 
      DeltaFastAssetAccount acct = (DeltaFastAssetAccount)obj;
      if (this.acctId == acct.acctId) {
        return true;
      }
    }
    return false;
  }
}
