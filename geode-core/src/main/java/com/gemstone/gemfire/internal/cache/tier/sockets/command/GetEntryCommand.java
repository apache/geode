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
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Region.Entry;
import com.gemstone.gemfire.internal.cache.EntrySnapshot;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.NonLocalRegionEntry;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;

/**
 * getEntry(key) operation performed on server.
 * Extends Request, and overrides getValueAndIsObject() in Request
 * so as to not invoke loader.
 * @since GemFire 6.6
 */
public class GetEntryCommand extends Request {

  private final static GetEntryCommand singleton = new GetEntryCommand();

  public static Command getCommand() {
    return singleton;
  }

  protected GetEntryCommand() {
  }
  
  @Override
  public void getValueAndIsObject(Region p_region, Object key,
      Object callbackArg, ServerConnection servConn, Object[] result) {
    Object data = null;
    LocalRegion region = (LocalRegion) p_region;
    Entry entry = region.getEntry(key);
    if (logger.isDebugEnabled()) {
      logger.debug("GetEntryCommand: for key: {} returning entry: {}", key, entry);
    }
    if (entry != null) {
      EntrySnapshot snap = new EntrySnapshot();
      NonLocalRegionEntry re = new NonLocalRegionEntry(entry, region);
      snap.setRegionEntry(re);
      snap.setRegion(region);
      data = snap;
    }
    result[0] = data;
    result[1] = true; // isObject is true
  }
}
