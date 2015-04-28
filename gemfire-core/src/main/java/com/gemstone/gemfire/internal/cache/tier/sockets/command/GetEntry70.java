/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Region.Entry;
import com.gemstone.gemfire.internal.cache.EntrySnapshot;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.NonLocalRegionEntry;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;

/**
 * getEntry(key) operation performed on server.
 * Extends Request, and overrides getValueAndIsObject() in Request
 * so as to not invoke loader.
 * @author sbawaska
 * @since 6.6
 */
public class GetEntry70 extends Get70 {

  private final static GetEntry70 singleton = new GetEntry70();

  public static Command getCommand() {
    return singleton;
  }

  protected GetEntry70() {
  }
  
  @Override
  public Get70.Entry getValueAndIsObject(Region region, Object key,
      Object callbackArg, ServerConnection servConn) {
    LocalRegion lregion = (LocalRegion)region;
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
