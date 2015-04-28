/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.internal.cache.locks;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
//import com.gemstone.gemfire.cache.Region;
import java.io.*;

/** 
 * Represents one transaction lock.
 *
 * @author Kirk Lund 
 */
public class TXLockToken implements DataSerializable {
  private static final long serialVersionUID = 8172108573123093776L;
  
  private String regionFullPath;
  
  private Object name;
  
  public TXLockToken(String regionFullPath, Object name) {
    this.regionFullPath = regionFullPath;
    this.name = name;
  }
  
  @Override
	public int hashCode() {
		int result = 17;
		final int mult = 37;

		result = mult * result + 
			(this.regionFullPath == null ? 0 : this.regionFullPath.hashCode());
		result = mult * result + 
			(this.name == null ? 0 : this.name.hashCode());

		return result;
	}
  
  @Override
	public boolean equals(Object other) {
		if (other == this) return true;
		if (other == null) return false;
		if (!(other instanceof TXLockToken)) return  false;
		final TXLockToken that = (TXLockToken) other;

		if (this.regionFullPath != that.regionFullPath &&
	  		!(this.regionFullPath != null &&
	  		this.regionFullPath.equals(that.regionFullPath))) return false;
		if (this.name != that.name &&
	  		!(this.name != null &&
	  		this.name.equals(that.name))) return false;

		return true;
	}
  
  @Override
  public String toString() {
    return "[TXLockToken: " + this.regionFullPath + "//" + this.name + "]";
	}

  // -------------------------------------------------------------------------
  //   DataSerializable support
  // -------------------------------------------------------------------------
  
  public TXLockToken() {}
  
  public void fromData(DataInput in)
  throws IOException, ClassNotFoundException {
    this.regionFullPath = DataSerializer.readString(in);
    this.name = DataSerializer.readObject(in);
  }
  
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.regionFullPath, out);
    DataSerializer.writeObject(this.name, out);
  }
  
}

