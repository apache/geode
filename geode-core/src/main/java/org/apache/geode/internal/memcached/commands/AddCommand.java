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
package org.apache.geode.internal.memcached.commands;

import java.nio.ByteBuffer;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.memcached.Reply;
import org.apache.geode.internal.memcached.RequestReader;
import org.apache.geode.internal.memcached.ResponseStatus;
import org.apache.geode.internal.memcached.ValueWrapper;


/**
 * general format of the command is:
 * <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
 * 
 * "add" means "store this data, but only if the server *doesn't* already
 *  hold data for this key".
 * 
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
