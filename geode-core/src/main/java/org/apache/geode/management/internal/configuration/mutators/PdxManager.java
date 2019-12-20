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
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.management.configuration.Pdx;
import org.apache.geode.management.internal.configuration.converters.PdxConverter;

public class PdxManager extends CacheConfigurationManager<Pdx> {
  private final PdxConverter pdxConverter = new PdxConverter();

  public PdxManager(ConfigurationPersistenceService service) {
    super(service);
  }

  @Override
  public void add(Pdx config, CacheConfig existing) {
    existing.setPdx(pdxConverter.fromConfigObject((config)));
  }

  @Override
  public void update(Pdx config, CacheConfig existing) {
    existing.setPdx(pdxConverter.fromConfigObject(config));
  }

  @Override
  public void delete(Pdx config, CacheConfig existing) {
    existing.setPdx(null);
  }

  @Override
  public List<Pdx> list(Pdx filterConfig, CacheConfig existing) {
    Pdx configuration = pdxConverter.fromXmlObject(existing.getPdx());
    if (configuration == null) {
      return Collections.emptyList();
    }
    return Collections.singletonList(configuration);
  }

  @Override
  public Pdx get(Pdx config, CacheConfig existing) {
    return pdxConverter.fromXmlObject(existing.getPdx());
  }
}
