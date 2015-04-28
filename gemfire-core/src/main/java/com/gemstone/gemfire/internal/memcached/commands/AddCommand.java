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
 * general format of the command is:
 * <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
 * 
 * "add" means "store this data, but only if the server *doesn't* already
 *  hold data for this key".
 * 
 * @author Swapnil Bawaskar
 */
public class AddCommand extends StorageCommand {

  @Override
  public ByteBuffer processStorageCommand(String key, byte[] value, int flags, Cache cache) {
    Region<Object, ValueWrapper> r = getMemcachedRegion(cache);
    Object oldVal = r.putIfAbsent(key, ValueWrapper.getWrappedValue(value, flags));
    String reply = null;
    if (oldVal == null) {
      reply = Reply.STORED.toString();
    } else {
      reply = Reply.NOT_STORED.toString();
    }
    return asciiCharset.encode(reply);
  }

  @Override
  public ByteBuffer processBinaryStorageCommand(Object key, byte[] value, long cas,
      int flags, Cache cache, RequestReader request) {
    ByteBuffer response = request.getResponse();
    Region<Object, ValueWrapper> r = getMemcachedRegion(cache);
    ValueWrapper val = ValueWrapper.getWrappedValue(value, flags);
    try {
      Object oldVal = r.putIfAbsent(key, val);
      // set status
      if (oldVal == null) {
        if (getLogger().fineEnabled()) {
          getLogger().fine("added key: "+key);
        }
        if (isQuiet()) {
          return null;
        }
        response.putShort(POSITION_RESPONSE_STATUS, ResponseStatus.NO_ERROR.asShort());
        //set cas
        response.putLong(POSITION_CAS, val.getVersion());
      } else {
        if (getLogger().fineEnabled()) {
          getLogger().fine("key: "+key+" not added as is already exists");
        }
        response.putShort(POSITION_RESPONSE_STATUS, ResponseStatus.KEY_EXISTS.asShort());
      }
    } catch (Exception e) {
      response = handleBinaryException(key, request, response, "add", e);
    }
    return response;
  }
  
  /**
   * Overridden by AddQ
   */
  protected boolean isQuiet() {
    return false;
  }
}
