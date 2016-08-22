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

package com.gemstone.gemfire.internal.cache.tier.sockets;

import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.cache.Conflatable;

import java.io.IOException;

/**
 * Interface <code>ClientMessage</code> is a message representing a cache
 * operation that is sent from a server to an interested client. 
 *
 *
 * @since GemFire 5.5
 */
public interface ClientMessage extends Conflatable, DataSerializableFixedID {

  /**
   * Returns a <code>Message</code> generated from the fields of this
   * <code>ClientMessage</code>.
   *
   * @param proxy the proxy that is dispatching this message
   * @return a <code>Message</code> generated from the fields of this
   *         <code>ClientUpdateMessage</code>
   * @throws IOException
   * @see com.gemstone.gemfire.internal.cache.tier.sockets.Message
   */
  public Message getMessage(CacheClientProxy proxy, boolean notify) throws IOException;
}
