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

package org.apache.geode.internal.cache;

import org.apache.geode.cache.*;

/**
 * EntryExpiryTask already implements the algorithm for figuring out expiration. This class has it
 * use the remote expiration attributes provided by the netsearch, rather than this region's
 * attributes.
 */
public class NetSearchExpirationCalculator extends EntryExpiryTask {

  private final ExpirationAttributes idleAttr;
  private final ExpirationAttributes ttlAttr;

  /** Creates a new instance of NetSearchExpirationCalculator */
  public NetSearchExpirationCalculator(LocalRegion region, Object key, int ttl, int idleTime) {
    super(region, null);
    idleAttr = new ExpirationAttributes(idleTime, ExpirationAction.INVALIDATE);
    ttlAttr = new ExpirationAttributes(ttl, ExpirationAction.INVALIDATE);
  }

  @Override
  protected ExpirationAttributes getTTLAttributes() {
    return this.ttlAttr;
  }

  @Override
  protected ExpirationAttributes getIdleAttributes() {
    return this.idleAttr;
  }

}
