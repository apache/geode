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

package org.apache.geode.management.internal.configuration.mutators;

import java.util.Collections;
import java.util.List;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.PdxType;

public class PdxManager implements ConfigurationManager<PdxType, PdxType> {
  @Override
  public void add(PdxType config, CacheConfig existing) {
    existing.setPdx(config);
  }

  @Override
  public void update(PdxType config, CacheConfig existing) {
    existing.setPdx(config);
  }

  @Override
  public void delete(PdxType config, CacheConfig existing) {
    existing.setPdx(null);
  }

  @Override
  public List<PdxType> list(PdxType filterConfig, CacheConfig existing) {
    return Collections.singletonList(existing.getPdx());
  }

  @Override
  public PdxType get(String id, CacheConfig existing) {
    return existing.getPdx();
  }
}
