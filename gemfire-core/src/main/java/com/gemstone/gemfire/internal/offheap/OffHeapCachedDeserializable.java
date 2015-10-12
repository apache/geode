package com.gemstone.gemfire.internal.offheap;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.DSCODE;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.cache.BytesAndBitsForCompactor;
import com.gemstone.gemfire.internal.cache.CachedDeserializableFactory;
import com.gemstone.gemfire.internal.cache.EntryBits;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.Chunk;
import com.gemstone.gemfire.internal.offheap.annotations.Unretained;

/**
 * This abstract class is intended to be used by {@link MemoryChunk} implementations that also want
 * to be a CachedDeserializable.
 * 
 * @author darrel
 * @since 9.0
 */
public abstract class OffHeapCachedDeserializable implements MemoryChunkWithRefCount {
  public abstract void setSerializedValue(byte[] value);
  @Override
  public abstract byte[] getSerializedValue();
  @Override
  public abstract int getSizeInBytes();
  @Override
  public abstract int getValueSizeInBytes();
  @Override
  public abstract Object getDeserializedValue(Region r, RegionEntry re);

  @Override
  public Object getValueAsDeserializedHeapObject() {
    return getDeserializedValue(null, null);
  }
  
  @Override
  public byte[] getValueAsHeapByteArray() {
    if (isSerialized()) {
      return getSerializedValue();
    } else {
      return (byte[])getDeserializedForReading();
    }
  }
  
  @Override
  public Object getDeserializedForReading() {
    return getDeserializedValue(null, null);
  }

  @Override
  public String getStringForm() {
    try {
      return StringUtils.forceToString(getDeserializedForReading());
    } catch (RuntimeException ex) {
      return "Could not convert object to string because " + ex;
    }
  }

  @Override
  public Object getDeserializedWritableCopy(Region r, RegionEntry re) {
    return getDeserializedValue(null, null);
  }

  @Override
  public Object getValue() {
    if (isSerialized()) {
      return getSerializedValue();
    } else {
      throw new IllegalStateException("Can not call getValue on StoredObject that is not serialized");
    }
  }

  @Override
  public void writeValueAsByteArray(DataOutput out) throws IOException {
    DataSerializer.writeByteArray(getSerializedValue(), out);
  }

  @Override
  public void fillSerializedValue(BytesAndBitsForCompactor wrapper, byte userBits) {
    if (isSerialized()) {
      userBits = EntryBits.setSerialized(userBits, true);
    }
    wrapper.setChunkData((Chunk) this, userBits);
  }
  
  String getShortClassName() {
    String cname = getClass().getName();
    return cname.substring(getClass().getPackage().getName().length()+1);
  }

  @Override
  public String toString() {
    return getShortClassName()+"@"+this.hashCode();
  }
  @Override
  public void sendTo(DataOutput out) throws IOException {
    if (isSerialized()) {
      out.write(getSerializedValue());
    } else {
      Object objToSend = (byte[]) getDeserializedForReading(); // deserialized as a byte[]
      DataSerializer.writeObject(objToSend, out);
    }
  }
  @Override
  public void sendAsByteArray(DataOutput out) throws IOException {
    byte[] bytes;
    if (isSerialized()) {
      bytes = getSerializedValue();
    } else {
      bytes = (byte[]) getDeserializedForReading();
    }
    DataSerializer.writeByteArray(bytes, out);
  }
  @Override
  public void sendAsCachedDeserializable(DataOutput out) throws IOException {
    if (!isSerialized()) {
      throw new IllegalStateException("sendAsCachedDeserializable can only be called on serialized StoredObjects");
    }
    InternalDataSerializer.writeDSFIDHeader(DataSerializableFixedID.VM_CACHED_DESERIALIZABLE, out);
    sendAsByteArray(out);
  }
  public boolean checkDataEquals(@Unretained OffHeapCachedDeserializable other) {
    if (this == other) {
      return true;
    }
    if (isSerialized() != other.isSerialized()) {
      return false;
    }
    int mySize = getValueSizeInBytes();
    if (mySize != other.getValueSizeInBytes()) {
      return false;
    }
    // We want to be able to do this operation without copying any of the data into the heap.
    // Hopefully the jvm is smart enough to use our stack for this short lived array.
    final byte[] dataCache1 = new byte[1024];
    final byte[] dataCache2 = new byte[dataCache1.length];
    // TODO OFFHEAP: no need to copy to heap. Just get the address of each and compare each byte.
    int i;
    // inc it twice since we are reading two different off-heap objects
    SimpleMemoryAllocatorImpl.getAllocator().getStats().incReads();
    SimpleMemoryAllocatorImpl.getAllocator().getStats().incReads();
    for (i=0; i < mySize-(dataCache1.length-1); i+=dataCache1.length) {
      this.readBytes(i, dataCache1);
      other.readBytes(i, dataCache2);
      for (int j=0; j < dataCache1.length; j++) {
        if (dataCache1[j] != dataCache2[j]) {
          return false;
        }
      }
    }
    int bytesToRead = mySize-i;
    if (bytesToRead > 0) {
      // need to do one more read which will be less than dataCache.length
      this.readBytes(i, dataCache1, 0, bytesToRead);
      other.readBytes(i, dataCache2, 0, bytesToRead);
      for (int j=0; j < bytesToRead; j++) {
        if (dataCache1[j] != dataCache2[j]) {
          return false;
        }
      }
    }
    return true;
  }
  
  public boolean isSerializedPdxInstance() {
    byte dsCode = this.readByte(0);
    return dsCode == DSCODE.PDX || dsCode == DSCODE.PDX_ENUM || dsCode == DSCODE.PDX_INLINE_ENUM;
  }
  
  public boolean checkDataEquals(byte[] serializedObj) {
    // caller was responsible for checking isSerialized
    int mySize = getValueSizeInBytes();
    if (mySize != serializedObj.length) {
      return false;
    }
    // We want to be able to do this operation without copying any of the data into the heap.
    // Hopefully the jvm is smart enough to use our stack for this short lived array.
    // TODO OFFHEAP: compare as ByteBuffers?
    final byte[] dataCache = new byte[1024];
    int idx=0;
    int i;
    SimpleMemoryAllocatorImpl.getAllocator().getStats().incReads();
    for (i=0; i < mySize-(dataCache.length-1); i+=dataCache.length) {
      this.readBytes(i, dataCache);
      for (int j=0; j < dataCache.length; j++) {
        if (dataCache[j] != serializedObj[idx++]) {
          return false;
        }
      }
    }
    int bytesToRead = mySize-i;
    if (bytesToRead > 0) {
      // need to do one more read which will be less than dataCache.length
      this.readBytes(i, dataCache, 0, bytesToRead);
      for (int j=0; j < bytesToRead; j++) {
        if (dataCache[j] != serializedObj[idx++]) {
          return false;
        }
      }
    }
    return true;
  }
}
