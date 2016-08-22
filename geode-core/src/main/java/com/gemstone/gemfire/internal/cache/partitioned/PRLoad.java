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
package com.gemstone.gemfire.internal.cache.partitioned;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

/**
 * A class which holds the load for a partitioned region
 * on a given VM.
 * 
 * @since GemFire 6.0
 */
public final class PRLoad implements DataSerializable {

  private static final long serialVersionUID = 778886995284953922L;
  private final float weight;
  private final float[] bucketReadLoads;
  private final float[] bucketWriteLoads;
  
  /**
   * Creates a new immutable instance of PRLoad from the provided DataInput.
   * Actually {@link #addBucket(int, float, float)} will allow the bucket
   * loads to be mutated.
   * 
   * @param in the input stream to gather state from
   * @return new immutable instance of PRLoad
   * @throws IOException if DataSerializer failed to read object from input 
   * stream
   * @throws ClassNotFoundException if DataSerializer failed to find class to
   * read object from input
   */
  public static PRLoad createFromDataInput(DataInput in) 
  throws IOException, ClassNotFoundException {
    float weight = in.readFloat();
    float[] bucketReadLoads = DataSerializer.readFloatArray(in);
    float[] bucketWriteLoads = DataSerializer.readFloatArray(in);
    return new PRLoad(weight, bucketReadLoads, bucketWriteLoads);
  }
  
  /**
   * Use {@link PRLoad#createFromDataInput(DataInput)} instead.
   */
  public PRLoad() {
    throw new UnsupportedOperationException(
        "Use PRLoad#createFromDataInput(DataInput) instead.");
  }

  /**
   * Constructs a new PRLoad. Please use {@link #addBucket(int, float, float)}
   * to add bucket loads.
   * 
   * @param numBuckets the number of buckets in the the PR
   * @param weight the weight of the PR
   */
  public PRLoad(int numBuckets, float weight) {
    this.weight = weight;
    this.bucketReadLoads = new float[numBuckets];
    this.bucketWriteLoads = new float[numBuckets];
  }

  /**
   * Constructs a new PRLoad. The bucket read and writes loads are backed by
   * the provided arrays which will be owned and potentially modified by this
   * instance.
   * 
   * @param weight the weight of the PR
   * @param bucketReadLoads the read loads for all buckets
   * @param bucketWriteLoads the write loads for all buckets
   */
  public PRLoad(float weight, float[] bucketReadLoads, float[] bucketWriteLoads) {
    this.weight = weight;
    this.bucketReadLoads = bucketReadLoads;
    this.bucketWriteLoads = bucketWriteLoads;
  }
  
  /**
   * Add a bucket to the list of bucket loads
   */
  public void addBucket(int bucketId, float readLoad, float writeLoad) {
    this.bucketReadLoads[bucketId] =  readLoad;
    this.bucketWriteLoads[bucketId] =  writeLoad;
  }

  /**
   * Get the read load for a bucket
   * @param bucketId the id of a bucket
   */
  public float getReadLoad(int bucketId) {
    return this.bucketReadLoads[bucketId];
  }

  /**
   * Get the write load for a bucket
   * @param bucketId the id of a bucket
   */
  public float getWriteLoad(int bucketId) {
    return this.bucketWriteLoads[bucketId];
  }

  /**
   * @return the weight
   */
  public float getWeight() {
    return this.weight;
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer("PRLoad@");
    sb.append(Integer.toHexString(hashCode())); 
    sb.append(", weight: ").append(this.weight);
    sb.append(", numBuckets: ").append(this.bucketReadLoads.length);
    sb.append(", bucketReadLoads: ").append(Arrays.toString(this.bucketReadLoads));
    sb.append(", bucketWriteLoads: ").append(Arrays.toString(this.bucketWriteLoads));
    return sb.toString();
  }
  
  public void toData(DataOutput out) throws IOException {
    out.writeFloat(this.weight);
    DataSerializer.writeFloatArray(this.bucketReadLoads, out);
    DataSerializer.writeFloatArray(this.bucketWriteLoads, out);
  }
  
  /**
   * Unsupported. Use {@link PRLoad#createFromDataInput(DataInput)} instead.
   */
  public void fromData(DataInput in) 
  throws IOException, ClassNotFoundException {
    throw new UnsupportedOperationException(
        "Use PRLoad#createFromDataInput(DataInput) instead.");
  }
}
