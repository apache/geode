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
package org.apache.geode.internal.memcached.commands;

import java.nio.ByteBuffer;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.memcached.Reply;
import org.apache.geode.internal.memcached.RequestReader;
import org.apache.geode.internal.memcached.ResponseStatus;
import org.apache.geode.internal.memcached.ValueWrapper;

/**
 * "append" means "add this data to an existing key after existing data".
 *
 *
 */
public class AppendCommand extends StorageCommand {

  @Override
  public ByteBuffer processStorageCommand(String key, byte[] value, int flags, Cache cache) {
    Region<Object, ValueWrapper> r = getMemcachedRegion(cache);
    ValueWrapper oldValWrapper = r.get(key);
    String retVal = Reply.NOT_FOUND.toString();
    if (oldValWrapper != null) {
      byte[] appendVal = value;
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
  public ByteBuffer processBinaryStorageCommand(Object key, byte[] value, long cas, int flags,
      Cache cache, RequestReader request) {
    ByteBuffer response = request.getResponse();
    Region<Object, ValueWrapper> r = getMemcachedRegion(cache);
    try {
      ValueWrapper oldValWrapper = r.get(key);
      if (oldValWrapper != null) {
        byte[] appendVal = value;
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
