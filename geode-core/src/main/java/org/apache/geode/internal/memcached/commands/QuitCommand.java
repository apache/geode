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
import org.apache.geode.internal.memcached.Reply;
import org.apache.geode.internal.memcached.RequestReader;
import org.apache.geode.memcached.GemFireMemcachedServer.Protocol;

/**
 * This command is currently no-op
 * 
 * "quit" is a command with no arguments:
 * quit\r\n<br/><br/>
 * 
 * Upon receiving this command, the server closes the
 * connection. However, the client may also simply close the connection
 * when it no longer needs it, without issuing this command.
 * 
 *
 */
public class QuitCommand extends AbstractCommand {

  @Override
  public ByteBuffer processCommand(RequestReader request, Protocol protocol, Cache cache) {
    if (protocol == Protocol.ASCII) {
      return asciiCharset.encode(Reply.OK.toString());
    }
    request.getRequest().position(HEADER_LENGTH);
    return isQuiet() ? null : request.getResponse();
  }

  /**
   * Overridden by Q command
   */
  protected boolean isQuiet() {
    return false;
  }
}
