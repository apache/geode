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
package javaobject;

import java.util.*;
import java.io.*;
import org.apache.geode.*;
import org.apache.geode.cache.Declarable;

/**
 * Represents an asset held in {@link AssetAccount}.
 */
public class FastAsset implements Declarable,Serializable,DataSerializable {

  private static final Random rng = new Random(12); // need determinism

  private int assetId;
  private double value;

  static {
	Instantiator.register(new Instantiator(FastAsset.class, (byte)24) {
	  public DataSerializable newInstance() {
	    return new FastAsset();
	  }
	});
  }

  public void init(Properties props) {
	this.assetId = Integer.parseInt(props.getProperty("assetId"));
	if(props.getProperty("value") != null) { 
	  this.value = Double.parseDouble( props.getProperty("value") );
	}
  }
  public FastAsset() {
  }

  public FastAsset(int anAssetId, int maxVal) {
    this.assetId = anAssetId;
    this.value = rng.nextDouble() * (maxVal - 1)  + 1;
  }
  
  /**
   * Makes a copy of this asset.
   */
  public FastAsset copy() {
    FastAsset asset = new FastAsset();
    asset.setAssetId(this.getAssetId());
    asset.setValue(this.getValue());
    return asset;
  }
  /**
   * Returns the id of the asset.
   */
  public int getAssetId(){
    return this.assetId;
  }

  /**
   * Returns the asset value.
   */
  public double getValue(){
    return this.value;
  }

  /**
   * Sets the asset value.
   */
  public void setValue(double d) {
    this.value = d;
  }
  /**
   * Sets the id of the asset.
   */
  public void setAssetId(int i){
    this.assetId = i;
  }
  public int getIndex() {
    return this.assetId;
  }

  public String toString(){
    return "FastAsset [assetId=" + this.assetId + " value=" + this.value + "]";
  }

  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    } else if (obj instanceof FastAsset) {
    	FastAsset asset = (FastAsset)obj;
      return this.assetId == asset.assetId;
    } else {
      return false;
    }
  }

  public int hashCode() {
    return this.assetId;
  }
  
//DataSerializable
  public void toData(DataOutput out)
  throws IOException {
	out.writeInt(this.assetId);
	out.writeDouble(this.value);
  }
  public void fromData(DataInput in)
  throws IOException, ClassNotFoundException {
	this.assetId = in.readInt();
	this.value = in.readDouble();
  }
}
