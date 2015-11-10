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
package com.gemstone.gemfire.distributed.internal;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.Version;
//import com.gemstone.gemfire.InternalGemFireException;
import java.io.*;

/**
 * Contains state that is distributed among distribution managers
 * using the JGroups {@link
 * com.gemstone.org.jgroups.Channel#getState "state"} mechanism.
 * It contains information that distribution managers need when
 * starting up.
 *
 * @author David Whitlock
 *
 *
 * @since 2.1
 */
class DistributedState implements DataSerializable {
  private static final long serialVersionUID = -4776743091985815549L;

  /** The version of GemFire being used */
  private String version;

  /** The current "cache time" */
  private long cacheTime;

  /////////////////////  Static Methods  /////////////////////

  /**
   * Returns a <code>DistributedState</code> created from the given
   * byte array.
   *
   * @throws IOException
   *         Something went wrong while deserializing
   *         <code>bytes</code> 
   */
  public static DistributedState fromBytes(byte[] bytes) 
    throws IOException {

    DistributedState state = new DistributedState();

    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    DataInputStream dis = new DataInputStream(bais);

    state.fromData(dis);
    return state;
  }

  /**
   * Returns a <code>byte</code> representation of the given
   * <code>DistributedState</code>. 
   *
   * @throws IOException
   *         Something went wrong while serializing <code>state</code>
   */
  public static byte[] toBytes(DistributedState state)
    throws IOException {

    HeapDataOutputStream hdos = new HeapDataOutputStream(256, Version.CURRENT);
    state.toData(hdos);
    return hdos.toByteArray();
  }

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <Code>DistributedState</code>.  This method is
   * invoked when a member (usually the "coordinator") of the
   * JGroups group receives a {@link
   * com.gemstone.org.jgroups.GetStateEvent}.
   */
  public DistributedState() {

  }

  /////////////////////  Instance Methods  /////////////////////

  /**
   * Sets the version of GemFire being used
   */
  public void setGemFireVersion(String version) {
    this.version = version;
  }

  /**
   * Returns the version of GemFire being used (by the sender of this
   * state). 
   */
  public String getGemFireVersion() {
    return this.version;
  }

  /**
   * Sets the current "cache time"
   */
  public void setCacheTime(long cacheTime) {
    this.cacheTime = cacheTime;
  }

  /**
   * Returns the "cache time" of the sender of this state
   */
  public long getCacheTime() {
    return this.cacheTime;
  }

  /**
   * Serializes this <code>DistributedState</code> as data
   */
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.version, out);
    out.writeLong(this.cacheTime);
  }

  /**
   * Popuplates the <code>DistributedState</code> from the given
   * data. 
   */
  public void fromData(DataInput in) throws IOException {
    this.version = DataSerializer.readString(in);
    this.cacheTime = in.readLong();
  }
}
