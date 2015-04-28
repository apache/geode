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
 * <code>
 * &lt;command name&gt; &lt;key&gt; &lt;flags&gt; &lt;exptime&gt; &lt;bytes&gt; [noreply]\r\n
 * </code><br/>
 * 
 * "set" means "store this data".
 * 
 * @author Swapnil Bawaskar
 *
 */
public class SetCommand extends StorageCommand {

  @Override
  public ByteBuffer processStorageCommand(String key, byte[] value, int flags, Cache cache) {
    Region<Object, ValueWrapper> r = getMemcachedRegion(cache);
    r.put(key, ValueWrapper.getWrappedValue(value, flags));
    return asciiCharset.encode(Reply.STORED.toString());
  }

  @Override
  public ByteBuffer processBinaryStorageCommand(Object key, byte[] value, long cas,
      int flags, Cache cache, RequestReader request) {
    ByteBuffer response = request.getResponse();
    Region<Object, ValueWrapper> r = getMemcachedRegion(cache);
    ValueWrapper val = ValueWrapper.getWrappedValue(value, flags);
    boolean success = true;

    try {
      if (cas != 0L) {
        ValueWrapper expected = ValueWrapper.getDummyValue(cas);
        success = r.replace(key, expected, val);
      } else {
        r.put(key, val);
      }
      
      if (getLogger().fineEnabled()) {
        getLogger().fine("set key:"+key+" succedded:"+success);
      }
      
      if (success) {
        if (isQuiet()) {
          return null;
        }
        response.putShort(POSITION_RESPONSE_STATUS, ResponseStatus.NO_ERROR.asShort());
        response.putLong(POSITION_CAS, val.getVersion());
      } else {
        response.putShort(POSITION_RESPONSE_STATUS, ResponseStatus.KEY_EXISTS.asShort());
      }
    } catch (Exception e) {
      response = handleBinaryException(key, request, response, "set", e);
    }
    return response;
  }

  /**
   * Overriden by SETQ
   */
  protected boolean isQuiet() {
    return false;
  }

}
