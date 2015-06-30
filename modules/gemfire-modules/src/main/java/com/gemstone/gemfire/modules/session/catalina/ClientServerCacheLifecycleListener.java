package com.gemstone.gemfire.modules.session.catalina;

import com.gemstone.gemfire.modules.session.bootstrap.ClientServerCache;

public class ClientServerCacheLifecycleListener extends AbstractCacheLifecycleListener {

  public ClientServerCacheLifecycleListener() {
    cache = ClientServerCache.getInstance();
  }
}
