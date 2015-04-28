package com.gemstone.gemfire.internal.admin;

import com.gemstone.gemfire.cache.server.CacheServer;

/**
 * A representation of <code>CacheServer</code> that is used for
 * administration. 
 *
 * @author David Whitlock
 * @since 4.0
 */
public interface AdminBridgeServer extends CacheServer {
  /**
   * Returns the VM-unique id of this cache server
   */
  public int getId();
}
