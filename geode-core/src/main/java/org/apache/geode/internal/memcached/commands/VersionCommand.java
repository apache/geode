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
import org.apache.geode.internal.memcached.RequestReader;
import org.apache.geode.memcached.GemFireMemcachedServer;
import org.apache.geode.memcached.GemFireMemcachedServer.Protocol;

/**
 * Sends current version of gemcached server to the client
 * 
 *
 */
public class VersionCommand extends AbstractCommand {

  @Override
  public ByteBuffer processCommand(RequestReader request, Protocol protocol, Cache cache) {
    String version = GemFireMemcachedServer.version;
    if (protocol == Protocol.ASCII) {
      return asciiCharset.encode("VERSION "+version+"\r\n");
    }
    ByteBuffer response = request.getResponse(HEADER_LENGTH + version.length());
    response.putInt(TOTAL_BODY_LENGTH_INDEX, version.length());
    response.position(HEADER_LENGTH);
    response.put(asciiCharset.encode(version));
    response.rewind();
    request.getRequest().position(HEADER_LENGTH);
    return response;
  }

}
