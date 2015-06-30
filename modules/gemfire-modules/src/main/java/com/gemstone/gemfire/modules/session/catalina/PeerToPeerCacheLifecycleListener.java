package com.gemstone.gemfire.modules.session.catalina;

import com.gemstone.gemfire.modules.session.bootstrap.PeerToPeerCache;

/**
 * This is a thin wrapper around a peer-to-peer cache.
 */
public class PeerToPeerCacheLifecycleListener 
    extends AbstractCacheLifecycleListener {

  public PeerToPeerCacheLifecycleListener() {
    cache = PeerToPeerCache.getInstance();
  }
}
