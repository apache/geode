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
package org.apache.geode.modules.session.catalina.callback;

import javax.servlet.http.HttpSession;

import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.Region;

public class LocalSessionCacheLoader implements CacheLoader<String, HttpSession>, Declarable {

  private final Region<String, HttpSession> backingRegion;

  public LocalSessionCacheLoader(Region<String, HttpSession> backingRegion) {
    this.backingRegion = backingRegion;
  }

  @Override
  public HttpSession load(LoaderHelper<String, HttpSession> helper) throws CacheLoaderException {
    return backingRegion.get(helper.getKey());
  }

  @Override
  public void close() {}
}
