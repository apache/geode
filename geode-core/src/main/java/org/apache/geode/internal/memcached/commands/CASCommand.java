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
import java.nio.CharBuffer;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.memcached.Reply;
import org.apache.geode.internal.memcached.RequestReader;
import org.apache.geode.internal.memcached.ValueWrapper;
import org.apache.geode.memcached.GemFireMemcachedServer.Protocol;

/**
 * "cas" is a check and set operation which means "store this data but
 * only if no one else has updated since I last fetched it."
 * 
 *
 */
public class CASCommand extends AbstractCommand {

  @Override
  public ByteBuffer processCommand(RequestReader request, Protocol protocol, Cache cache) {
    if (protocol == Protocol.ASCII) {
      return processAsciiCommand(request.getRequest(), cache);
    }
    // binary protocol has cas in regular add/put commands
    throw new IllegalStateException();
  }

  private ByteBuffer processAsciiCommand(ByteBuffer buffer, Cache cache) {
    CharBuffer flb = getFirstLineBuffer();
    getAsciiDecoder().reset();
    getAsciiDecoder().decode(buffer, flb, false);
    flb.flip();
    String firstLine = getFirstLine();
    String[] firstLineElements = firstLine.split(" ");
    
    String key = firstLineElements[1];
    int flags = Integer.parseInt(firstLineElements[2]);
    long expTime = Long.parseLong(firstLineElements[3]);
    int numBytes = Integer.parseInt(firstLineElements[4]);
    long casVersion = Long.parseLong(stripNewline(firstLineElements[5]));
    
    byte[] value = new byte[numBytes];
    buffer.position(firstLine.length());
    for (int i=0; i<numBytes; i++) {
      value[i] = buffer.get();
    }
    
    String reply = Reply.EXISTS.toString();
    Region<Object, ValueWrapper> r = getMemcachedRegion(cache);
    ValueWrapper expected = ValueWrapper.getDummyValue(casVersion);
    if (r.replace(key, expected, ValueWrapper.getWrappedValue(value, flags))) {
      reply = Reply.STORED.toString();
    }
    
    return asciiCharset.encode(reply);
  }
}
