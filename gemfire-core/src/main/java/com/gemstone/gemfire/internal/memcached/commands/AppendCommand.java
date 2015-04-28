/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.memcached.commands;

import java.nio.ByteBuffer;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.memcached.Reply;
import com.gemstone.gemfire.internal.memcached.RequestReader;
import com.gemstone.gemfire.internal.memcached.ResponseStatus;
import com.gemstone.gemfire.internal.memcached.ValueWrapper;

/**
 * "append" means "add this data to an existing key after existing data".
 * 
 * @author Swapnil Bawaskar
 *
 */
public class AppendCommand extends StorageCommand {

  @Override
  public ByteBuffer processStorageCommand(String key, byte[] value, int flags, Cache cache) {
    Region<Object, ValueWrapper> r = getMemcachedRegion(cache);
    ValueWrapper oldValWrapper = r.get(key);
    String retVal = Reply.NOT_FOUND.toString();
    if (oldValWrapper != null) {
      byte[] appendVal = (byte[]) value;
      byte[] oldVal = oldValWrapper.getValue();
      byte[] newVal = new byte[oldVal.length + appendVal.length];
      System.arraycopy(oldVal, 0, newVal, 0, oldVal.length);
      System.arraycopy(appendVal, 0, newVal, oldVal.length, appendVal.length);
      r.put(key, ValueWrapper.getWrappedValue(newVal, flags));
      retVal = Reply.STORED.toString();
    }
    return asciiCharset.encode(retVal);
  }

  @Override
  public ByteBuffer processBinaryStorageCommand(Object key, byte[] value, long cas,
      int flags, Cache cache, RequestReader request) {
    ByteBuffer response = request.getResponse();
    Region<Object, ValueWrapper> r = getMemcachedRegion(cache);
    try {
      ValueWrapper oldValWrapper = r.get(key);
      if (oldValWrapper != null) {
        byte[] appendVal = (byte[]) value;
        byte[] oldVal = oldValWrapper.getValue();
        byte[] newVal = new byte[oldVal.length + appendVal.length];
        System.arraycopy(oldVal, 0, newVal, 0, oldVal.length);
        System.arraycopy(appendVal, 0, newVal, oldVal.length, appendVal.length);
        ValueWrapper val = ValueWrapper.getWrappedValue(newVal, flags);
        try {
          r.put(key, val);
          if (isQuiet()) {
            return null;
          }
          response.putShort(POSITION_RESPONSE_STATUS, ResponseStatus.NO_ERROR.asShort());
          response.putLong(POSITION_CAS, val.getVersion());
        } catch (Exception e) {
          response = handleBinaryException(key, request, response, "append", e);
        }
      } else {
        response.putShort(POSITION_RESPONSE_STATUS, ResponseStatus.KEY_NOT_FOUND.asShort());
      }
    } catch (Exception e) {
      response = handleBinaryException(key, request, response, "append", e);
    }
    return response;
  }

  /**
   * Overridden by Q command
   */
  protected boolean isQuiet() {
    return false;
  }
}
