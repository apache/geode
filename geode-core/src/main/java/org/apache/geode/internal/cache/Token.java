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

package org.apache.geode.internal.cache;

import org.apache.geode.internal.DSCODE;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.Version;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Internal tokens used as region values. These tokens
 * are never seen from the public API.
 *
 * These classes are Serializable and implement readResolve to support
 * canonicalization in the face of copysharing.
 *
 */
public abstract class Token {

  public static final Invalid INVALID = new Invalid();
  public static final LocalInvalid LOCAL_INVALID = new LocalInvalid();
  
  // DESTROYED token, used during getInitialImage only
  // and only for identity comparisons
  public static final Destroyed DESTROYED = new Destroyed();  

  /**
   * Tombstone is used to hold onto destroyed entries in regions
   * supporting entry versioning.  DESTROYED and REMOVED_PHASE2
   * tokens transition to TOMBSTONE during GII and normal entry
   * removal.
   */
  public static final Tombstone TOMBSTONE = new Tombstone();  

  /**
   * Used when a RegionEntry is removed from a RegionMap. In phase 1 of
   * removal, the destroy() operation is applied to the entry under
   * synchronization.  The lock is then released while distribution
   * is done.  During this period, the entry may be modified, preventing
   * the entry from being removed from its map.
   */
  public static final Removed REMOVED_PHASE1 = new Removed();
  
  /**
   * Used when a RegionEntry is removed from a RegionMap.  In phase 2 of
   * removal, callbacks for the entry are invoked under synchronization
   * and the entry is then removed from the map.  If an entry is seen in
   * this state, you should wait in a loop for the entry to be removed
   * from the map.
   */
  public static final Removed2 REMOVED_PHASE2 = new Removed2();

  /**
   * Used to designate end of stream in StreamingOperation
   */
  public static final EndOfStream END_OF_STREAM = new EndOfStream();

  /**
   * Indicates that a decision was made to not provide some information
   * that is normally available.
   */
  public static final NotAvailable NOT_AVAILABLE = new NotAvailable();

  // !!! NOTICE !!!
  // If you add a new Token to this class then add
  // support in OffHeapRegionEntryHelper to encode that
  // token as an address.
  // See OffHeapRegionEntryHelper.objectToAddress.
  
  /**
   * A token used to represent a value that is not a token.
   */
  public static final NotAToken NOT_A_TOKEN = new NotAToken();
  
  /**
   * Returns true if o is INVALID, LOCAL_INVALID, DESTROYED, or REMOVED.
   */
  public static final boolean isInvalidOrRemoved(Object o) {
    return isInvalid(o) || isRemoved(o);
  }
  public static final boolean isInvalid(Object o) {
    return o == INVALID || o == LOCAL_INVALID;
  }
  public static final boolean isRemoved(Object o) {
    return o == DESTROYED || o == REMOVED_PHASE1 || o == REMOVED_PHASE2 || o == TOMBSTONE;
  }
  public static final boolean isRemovedFromDisk(Object o) {
    return o == DESTROYED || o == REMOVED_PHASE1 || o == REMOVED_PHASE2;
  }
  public static final boolean isDestroyed(Object o) {
    return o == DESTROYED ;
  }
  
  /**
   * Singleton token indicating an Invalid Entry.
   */
  public static class Invalid extends Token implements DataSerializableFixedID, Serializable {
    private static final long serialVersionUID = -4133205114649525169L;
    protected Invalid() {
    }
    @Override
    public String toString() {
      return "INVALID";
    }
    private Object readResolve() throws ObjectStreamException {
      return INVALID;
    }
    public int getDSFID() {
      return TOKEN_INVALID;
    }
    public void toData(DataOutput out) throws IOException {}
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {}
    
    public boolean isSerializedValue(byte[] value) {
      ByteBuffer buf = ByteBuffer.wrap(value);
      return buf.capacity() == 3 
          && buf.get() == DSCODE.DS_FIXED_ID_SHORT 
          && buf.getShort() == getDSFID();
    }
    @Override
    public Version[] getSerializationVersions() {
      // TODO Auto-generated method stub
      return null;
    }
  }
  
  public static class LocalInvalid extends Invalid {
    private static final long serialVersionUID = 4110159168041217249L;
    protected LocalInvalid() {
    }
    @Override
    public String toString() {
      return "LOCAL_INVALID";
    }
    private Object readResolve() throws ObjectStreamException {
      return LOCAL_INVALID;
    }
    @Override
    public int getDSFID() {
      return TOKEN_LOCAL_INVALID;
    }
  }

  public static class Destroyed extends Token implements DataSerializableFixedID, Serializable {
    private static final long serialVersionUID = -1922513819482668368L;
    protected Destroyed() {
    }
    @Override
    public String toString() {
      return "DESTROYED";
    }
    private Object readResolve() throws ObjectStreamException {
      return DESTROYED;
    }
    public int getDSFID() {
      return TOKEN_DESTROYED;
    }
    public void toData(DataOutput out) throws IOException {}
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {}
    @Override
    public Version[] getSerializationVersions() {
      // TODO Auto-generated method stub
      return null;
    }
  }  

  public static class Tombstone extends Token implements DataSerializableFixedID, Serializable {
    static final long serialVersionUID = -6388232623019450170L;
    protected Tombstone() {
    }
    @Override
    public String toString() {
      return "TOMBSTONE";
    }
    private Object readResolve() throws ObjectStreamException {
      return TOMBSTONE;
    }
    public int getDSFID() {
      return TOKEN_TOMBSTONE;
    }
    public void toData(DataOutput out) throws IOException {}
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {}
    @Override
    public Version[] getSerializationVersions() {
      // TODO Auto-generated method stub
      return null;
    }
  }  

  public static class Removed extends Token implements DataSerializableFixedID, Serializable {
    private static final long serialVersionUID = -1999836887955504653L;
    protected Removed() {
    }
    @Override
    public String toString() {
      return "REMOVED_PHASE1";
    }
    private Object readResolve() throws ObjectStreamException {
      return REMOVED_PHASE1;
    }
    public int getDSFID() {
      return TOKEN_REMOVED;
    }
    public void toData(DataOutput out) throws IOException {}
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {}
    @Override
    public Version[] getSerializationVersions() {
      // TODO Auto-generated method stub
      return null;
    }
  }
  
  public static class Removed2 extends Token implements DataSerializableFixedID, Serializable {
    private static final long serialVersionUID = 5122235867167804597L;
    protected Removed2() {
    }
    @Override
    public String toString() {
      return "REMOVED_PHASE2";
    }
    private Object readResolve() throws ObjectStreamException {
      return REMOVED_PHASE2;
    }
    public int getDSFID() {
      return TOKEN_REMOVED2;
    }
    public void toData(DataOutput out) throws IOException {}
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {}
    @Override
    public Version[] getSerializationVersions() {
      // TODO Auto-generated method stub
      return null;
    }
  }
  
  public static class NotAvailable extends Token {
    protected NotAvailable() {
    }
    @Override
    public String toString() {
      return "NOT_AVAILABLE";
    }
    // to fix bug 50200 no longer serializable
  }
  
  public static class NotAToken extends Token {
    protected NotAToken() {
    }
    @Override
    public String toString() {
      return "NOT_A_TOKEN";
    }
    // to fix bug 50200 no longer serializable
  }
  
  /** Token used in StreamingOperation, StreamingPartitionOperation */
  public static final class EndOfStream extends Token implements DataSerializableFixedID {
    public EndOfStream() {
    }
    @Override
    public String toString() {
      return "EndOfStream";
    }
    private Object readResolve() throws ObjectStreamException {
      return END_OF_STREAM;
    }
    public int getDSFID() {
      return END_OF_STREAM_TOKEN;
    }
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {}
    public void toData(DataOutput out) throws IOException {}
    @Override
    public Version[] getSerializationVersions() {
      // TODO Auto-generated method stub
      return null;
    }
  }
  
}
