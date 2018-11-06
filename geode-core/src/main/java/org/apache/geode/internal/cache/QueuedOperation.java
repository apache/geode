/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.versions.ConcurrentCacheModificationException;
import org.apache.geode.internal.offheap.annotations.Released;

/**
 * Represents a single operation that can be queued for reliable delivery. Instances are owned in
 * the context of a region.
 *
 * @since GemFire 5.0
 */
public class QueuedOperation {
  private final Operation op;

  private final Object key; // may be null

  private final byte[] value; // may be null

  private final Object valueObj; // may be null

  /**
   * Deserialization policies defined in AbstractUpdateOperation
   *
   * @see org.apache.geode.internal.cache.AbstractUpdateOperation
   */
  private final byte deserializationPolicy;

  private final Object cbArg; // may be null

  /**
   * Creates an instance of a queued operation with the given data.
   */
  public QueuedOperation(Operation op, Object key, byte[] value, Object valueObj,
      byte deserializationPolicy, Object cbArg) {
    this.op = op;
    this.key = key;
    this.value = value;
    this.valueObj = valueObj;
    this.deserializationPolicy = deserializationPolicy;
    this.cbArg = cbArg;
  }

  public void process(LocalRegion lr, DistributedMember src, long lastMod) {
    if (this.op.isRegion()) {
      // it is a region operation
      RegionEventImpl re = new RegionEventImpl(lr, this.op, this.cbArg, true, src);
      // re.setQueued(true);
      if (this.op.isRegionInvalidate()) {
        lr.basicInvalidateRegion(re);
      } else if (this.op == Operation.REGION_CLEAR) {
        lr.cmnClearRegion(re, false/* cacheWrite */, false/* useRVV */);
      } else {
        throw new IllegalStateException(
            String.format("The %s should not have been queued.",
                this.op));
      }
    } else {
      // it is an entry operation
      // TODO :EventID should be passed from the sender & should be reused here
      @Released
      EntryEventImpl ee = EntryEventImpl.create(lr, this.op, this.key, null, this.cbArg, true, src);
      try {
        // ee.setQueued(true);
        if (this.op.isCreate() || this.op.isUpdate()) {
          UpdateOperation.UpdateMessage.setNewValueInEvent(this.value, this.valueObj, ee,
              this.deserializationPolicy);
          try {
            long time = lastMod;
            if (ee.getVersionTag() != null) {
              time = ee.getVersionTag().getVersionTimeStamp();
            }
            if (AbstractUpdateOperation.doPutOrCreate(lr, ee, time)) {
              // am I done?
            }
          } catch (ConcurrentCacheModificationException e) {
            // operation was rejected by the cache's concurrency control mechanism as being old
          }
        } else if (this.op.isDestroy()) {
          ee.setOldValueFromRegion();
          try {
            lr.basicDestroy(ee, false, null); // expectedOldValue
                                              // ???:ezoerner:20080815
                                              // can a remove(key, value) operation be
                                              // queued?
          } catch (ConcurrentCacheModificationException e) {
            // operation was rejected by the cache's concurrency control mechanism as being old
          } catch (EntryNotFoundException ignore) {
          } catch (CacheWriterException e) {
            throw new Error("CacheWriter should not be called", e);
          } catch (TimeoutException e) {
            throw new Error("DistributedLock should not be acquired", e);
          }
        } else if (this.op.isInvalidate()) {
          ee.setOldValueFromRegion();
          boolean forceNewEntry = lr.getDataPolicy().withReplication() && !lr.isInitialized();
          boolean invokeCallbacks = lr.isInitialized();
          try {
            lr.basicInvalidate(ee, invokeCallbacks, forceNewEntry);
          } catch (ConcurrentCacheModificationException e) {
            // operation was rejected by the cache's concurrency control mechanism as being old
          } catch (EntryNotFoundException ignore) {
          }
        } else {
          throw new IllegalStateException(
              String.format("The %s should not have been queued.",
                  this.op));
        }
      } finally {
        ee.release();
      }
    }
  }

  public static QueuedOperation createFromData(DataInput in)
      throws IOException, ClassNotFoundException {
    Operation op = Operation.fromOrdinal(in.readByte());
    Object key = null;
    byte[] value = null;
    Object valueObj = null;
    byte deserializationPolicy = DistributedCacheOperation.DESERIALIZATION_POLICY_NONE;
    Object cbArg = DataSerializer.readObject(in);
    if (op.isEntry()) {
      key = DataSerializer.readObject(in);
      if (op.isUpdate() || op.isCreate()) {
        deserializationPolicy = in.readByte();
        value = DataSerializer.readByteArray(in);
      }
    }
    return new QueuedOperation(op, key, value, valueObj, deserializationPolicy, cbArg);
  }


  public void toData(DataOutput out) throws IOException {
    out.writeByte(this.op.ordinal);
    DataSerializer.writeObject(this.cbArg, out);
    if (this.op.isEntry()) {
      DataSerializer.writeObject(this.key, out);
      if (this.op.isUpdate() || this.op.isCreate()) {
        out.writeByte(this.deserializationPolicy);
        DataSerializer.writeByteArray(this.value, out);
      }
    }
  }
}
