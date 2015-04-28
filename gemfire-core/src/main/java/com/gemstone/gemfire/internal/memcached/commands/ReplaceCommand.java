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
 * general format of the command is:<br/>
 * <code>
 * &lt;command name&gt; &lt;key&gt; &lt;flags&gt; &lt;exptime&gt; &lt;bytes&gt; [noreply]\r\n
 * </code><br/>
 * 
 * "replace" means "store this data, but only if the server *does*
 * already hold data for this key".
 * 
 * @author Swapnil Bawaskar
 */
public class ReplaceCommand extends StorageCommand {

  @Override
  public ByteBuffer processStorageCommand(String key, byte[] value, int flags, Cache cache) {
    Region<Object, ValueWrapper> r = getMemcachedRegion(cache);
    Object oldVal = r.replace(key, ValueWrapper.getWrappedValue(value, flags));
    String reply = null;
    if (oldVal == null) {
      reply = Reply.NOT_STORED.toString();
    } else {
      reply = Reply.STORED.toString();
    }
    return asciiCharset.encode(reply);
  }

  @Override
  public ByteBuffer processBinaryStorageCommand(Object key, byte[] value, long cas,
      int flags, Cache cache, RequestReader request) {
    ByteBuffer response = request.getResponse();
    Region<Object, ValueWrapper> r = getMemcachedRegion(cache);
    ValueWrapper val = ValueWrapper.getWrappedValue(value, flags);
    boolean success = false;
    
    try {
      if (cas != 0L) {
        ValueWrapper expected = ValueWrapper.getDummyValue(cas);
        success = r.replace(key, expected, val);
      } else {
        success = r.replace(key, val) != null;
      }
    } catch (Exception e) {
      return handleBinaryException(key, request, response, "replace", e);
    }
    
    if (getLogger().fineEnabled()) {
      getLogger().fine("replace:key:"+key+" cas:"+cas+" success:"+success);
    }
    
    // set status
    if (success) {
      if (isQuiet()) {
        return null;
      }
      response.putShort(POSITION_RESPONSE_STATUS, ResponseStatus.NO_ERROR.asShort());
      response.putLong(POSITION_CAS, val.getVersion());
    } else {
      if (cas != 0L) {
        response.putShort(POSITION_RESPONSE_STATUS, ResponseStatus.KEY_EXISTS.asShort());
      } else {
        response.putShort(POSITION_RESPONSE_STATUS, ResponseStatus.KEY_NOT_FOUND.asShort());
      }
      // set CAS
      //response.putLong(POSITION_CAS, val.getVersion());
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
