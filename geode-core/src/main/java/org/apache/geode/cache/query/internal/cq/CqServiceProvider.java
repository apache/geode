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
package org.apache.geode.cache.query.internal.cq;

import java.io.DataInput;
import java.io.IOException;
import java.util.Set;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.cache.query.internal.cq.spi.CqServiceFactory;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.services.module.ModuleService;
import org.apache.geode.services.result.ModuleServiceResult;
import org.apache.geode.util.internal.GeodeGlossary;

public class CqServiceProvider {

  /**
   * A debug flag used for testing vMotion during CQ registration
   */
  public static final boolean VMOTION_DURING_CQ_REGISTRATION_FLAG = false;
  /**
   * System property to maintain the CQ event references for optimizing the updates. This will allow
   * running the CQ query only once during update events.
   */
  @MutableForTesting
  public static boolean MAINTAIN_KEYS = Boolean
      .parseBoolean(System.getProperty(GeodeGlossary.GEMFIRE_PREFIX + "cq.MAINTAIN_KEYS", "true"));
  @Immutable
  private static CqServiceFactory factory;

  private CqServiceProvider() {}

  private static void setup(ModuleService moduleService) {
    if (factory == null) {
      ModuleServiceResult<Set<CqServiceFactory>> loadServiceResult =
          moduleService.loadService(CqServiceFactory.class);
      if (loadServiceResult.isSuccessful()) {
        for (CqServiceFactory cqServiceFactory : loadServiceResult.getMessage()) {
          factory = cqServiceFactory;
          factory.initialize();
          break;
        }
      } else {
        factory = null;
      }
    }
  }

  public static synchronized CqService create(InternalCache cache, ModuleService moduleService) {
    setup(moduleService);
    if (factory == null) {
      return new MissingCqService();
    }

    return factory.create(cache);
  }

  public static ServerCQ readCq(DataInput in) throws ClassNotFoundException, IOException {
    if (factory == null) {
      throw new IllegalStateException("CqService is not available.");
    } else {
      return factory.readCqQuery(in);
    }
  }
}
