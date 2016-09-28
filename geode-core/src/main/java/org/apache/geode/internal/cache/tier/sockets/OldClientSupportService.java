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

package org.apache.geode.internal.cache.tier.sockets;

import java.io.DataInput;
import java.io.DataOutput;

import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.CacheService;

/**
 * Support for old GemFire clients
 */
public interface OldClientSupportService extends CacheService {

  /**
   * translates the given throwable into one that can be sent to an old GemFire client
   * @param theThrowable the throwable to convert
   * @param clientVersion the version of the client
   * @return the exception to give the client
   */
  public Throwable getThrowable(Throwable theThrowable, Version clientVersion);

  /**
   * Process a class name read from a serialized object of unknown origin
   * @param name
   * @return the class name to use
   */
  public String processIncomingClassName(String name);
    
  /**
   * Process a class name read from a serialized object
   * @param name the fully qualified class name
   * @param in the source of the class name
   * @return the adjusted class name
   */
  public String processIncomingClassName(String name, DataInput in);

  /**
   * Process a class name being written to a serialized form
   * @param name the fully qualified class name
   * @param out the consumer of the class name
   * @return the adjusted class name
   */
  public String processOutgoingClassName(String name, DataOutput out);
}
