package com.gemstone.gemfire.internal.cache;
public interface SerializationHelper {

    public byte[] convertObject2Bytes(Object obj);

    public Object convertBytes2Object(byte[] objBytes);

}
