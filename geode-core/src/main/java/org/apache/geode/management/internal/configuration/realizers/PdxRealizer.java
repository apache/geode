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

package org.apache.geode.management.internal.configuration.realizers;

import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.configuration.Pdx;
import org.apache.geode.management.runtime.PdxInfo;
import org.apache.geode.pdx.PdxSerializer;

public class PdxRealizer implements ConfigurationRealizer<Pdx, PdxInfo> {
  @Override
  public PdxInfo get(Pdx config, InternalCache cache) {
    PdxInfo info = new PdxInfo();
    info.setReadSerialized(cache.getPdxReadSerialized());
    info.setDiskStore(cache.getPdxDiskStore());
    info.setIgnoreUnreadFields(cache.getPdxIgnoreUnreadFields());
    info.setPersistent(cache.getPdxPersistent());
    PdxSerializer pdxSerializer = cache.getPdxSerializer();
    if (pdxSerializer != null) {
      info.setSerializerClassName(pdxSerializer.getClass().getName());
    }
    return info;
  }

  @Override
  public boolean isReadyOnly() {
    return true;
  }
}
