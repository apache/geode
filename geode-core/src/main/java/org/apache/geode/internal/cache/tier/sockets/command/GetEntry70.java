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
package org.apache.geode.internal.cache.tier.sockets.command;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.NonLocalRegionEntry;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.cache.versions.VersionTag;

/**
 * getEntry(key) operation performed on server. Extends Request, and overrides getValueAndIsObject()
 * in Request so as to not invoke loader.
 * 
 * @since GemFire 6.6
 */
public class GetEntry70 extends Get70 {

  private final static GetEntry70 singleton = new GetEntry70();

  public static Command getCommand() {
    return singleton;
  }

  protected GetEntry70() {}

  @Override
  protected Get70.Entry getEntry(Region region, Object key, Object callbackArg,
      ServerConnection servConn) {
    return getValueAndIsObject(region, key, callbackArg, servConn);
  }

  @Override
  public Get70.Entry getValueAndIsObject(Region region, Object key, Object callbackArg,
      ServerConnection servConn) {
    LocalRegion lregion = (LocalRegion) region;
    Object data = null;
    Region.Entry entry = region.getEntry(key);
    if (logger.isDebugEnabled()) {
      logger.debug("GetEntryCommand: for key: {} returning entry: {}", key, entry);
    }
    VersionTag tag = null;
    if (entry != null) {
      EntrySnapshot snap = new EntrySnapshot();
      NonLocalRegionEntry re = new NonLocalRegionEntry(entry, lregion);
      snap.setRegionEntry(re);
      snap.setRegion(lregion);
      data = snap;
      tag = snap.getVersionTag();
    }
    Get70.Entry result = new Get70.Entry();
    result.value = data;
    result.isObject = true;
    result.keyNotPresent = false;
    result.versionTag = tag;
    return result;
  }
}
