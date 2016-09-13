/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.internal.cache.locks;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
//import org.apache.geode.cache.Region;
import java.io.*;

import org.apache.commons.lang.StringUtils;

/** 
 * Represents one transaction lock.
 *
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

		if (!StringUtils.equals(this.regionFullPath, that.regionFullPath)) return false;
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

