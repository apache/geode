/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
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
