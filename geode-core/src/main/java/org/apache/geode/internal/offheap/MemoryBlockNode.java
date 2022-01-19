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
package org.apache.geode.internal.offheap;

import java.io.IOException;
import java.util.Arrays;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheClosedException;

/**
 * Basic implementation of MemoryBlock for test validation only.
 */
public class MemoryBlockNode implements MemoryBlock {
  private final MemoryAllocatorImpl ma;
  private final MemoryBlock block;

  MemoryBlockNode(MemoryAllocatorImpl ma, MemoryBlock block) {
    this.ma = ma;
    this.block = block;
  }

  @Override
  public State getState() {
    return block.getState();
  }

  @Override
  public long getAddress() {
    return block.getAddress();
  }

  @Override
  public int getBlockSize() {
    return block.getBlockSize();
  }

  @Override
  public MemoryBlock getNextBlock() {
    return ma.getMemoryInspector().getBlockAfter(this);
  }

  @Override
  public int getSlabId() {
    return ma.findSlab(getAddress());
  }

  @Override
  public int getFreeListId() {
    return block.getFreeListId();
  }

  @Override
  public int getRefCount() {
    return block.getRefCount(); // delegate to fix GEODE-489
  }

  @Override
  public String getDataType() {
    if (block.getDataType() != null) {
      return block.getDataType();
    }
    if (!isSerialized()) {
      // byte array
      if (isCompressed()) {
        return "compressed byte[" + ((OffHeapStoredObject) block).getDataSize() + "]";
      } else {
        return "byte[" + ((OffHeapStoredObject) block).getDataSize() + "]";
      }
    } else if (isCompressed()) {
      return "compressed object of size " + ((OffHeapStoredObject) block).getDataSize();
    }
    // Object obj = EntryEventImpl.deserialize(((Chunk)this.block).getRawBytes());
    byte[] bytes = ((OffHeapStoredObject) block).getRawBytes();
    return DataType.getDataType(bytes);
  }

  @Override
  public boolean isSerialized() {
    return block.isSerialized();
  }

  @Override
  public boolean isCompressed() {
    return block.isCompressed();
  }

  @Override
  public Object getDataValue() {
    String dataType = getDataType();
    if (dataType == null || dataType.equals("N/A")) {
      return null;
    } else if (isCompressed()) {
      return ((OffHeapStoredObject) block).getCompressedBytes();
    } else if (!isSerialized()) {
      // byte array
      // return "byte[" + ((Chunk)this.block).getDataSize() + "]";
      return ((OffHeapStoredObject) block).getRawBytes();
    } else {
      try {
        byte[] bytes = ((OffHeapStoredObject) block).getRawBytes();
        return DataSerializer.readObject(DataType.getDataInput(bytes));
      } catch (IOException e) {
        e.printStackTrace();
        return "IOException:" + e.getMessage();
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
        return "ClassNotFoundException:" + e.getMessage();
      } catch (CacheClosedException e) {
        e.printStackTrace();
        return "CacheClosedException:" + e.getMessage();
      }
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(MemoryBlock.class.getSimpleName());
    sb.append("{");
    sb.append("MemoryAddress=").append(getAddress());
    sb.append(", State=").append(getState());
    sb.append(", BlockSize=").append(getBlockSize());
    sb.append(", SlabId=").append(getSlabId());
    sb.append(", FreeListId=");
    if (getState() == State.UNUSED || getState() == State.ALLOCATED) {
      sb.append("NONE");
    } else if (getFreeListId() == -1) {
      sb.append("HUGE");
    } else {
      sb.append(getFreeListId());
    }
    sb.append(", RefCount=").append(getRefCount());
    sb.append(", isSerialized=").append(isSerialized());
    sb.append(", isCompressed=").append(isCompressed());
    sb.append(", DataType=").append(getDataType());
    {
      sb.append(", DataValue=");
      Object dataValue = getDataValue();
      if (dataValue instanceof byte[]) {
        byte[] ba = (byte[]) dataValue;
        if (ba.length < 1024) {
          sb.append(Arrays.toString(ba));
        } else {
          sb.append("<byte array of length " + ba.length + ">");
        }
      } else {
        sb.append(dataValue);
      }
    }
    sb.append("}");
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof MemoryBlockNode) {
      o = ((MemoryBlockNode) o).block;
    }
    return block.equals(o);
  }

  @Override
  public int hashCode() {
    return block.hashCode();
  }
}
