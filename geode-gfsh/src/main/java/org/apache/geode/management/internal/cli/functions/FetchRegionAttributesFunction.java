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
package org.apache.geode.management.internal.cli.functions;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.services.module.ModuleService;

/**
 *
 * @since GemFire 7.0
 */
public class FetchRegionAttributesFunction extends CliFunction<String> {
  private static final Logger logger = LogService.getLogger();

  private static final long serialVersionUID = 4366812590788342070L;

  private static final String ID = FetchRegionAttributesFunction.class.getName();

  @Override
  public boolean isHA() {
    return false;
  }

  /**
   * this returns the region xml definition back to the caller
   */
  @Override
  public CliFunctionResult executeFunction(FunctionContext<String> context) throws Exception {
    ModuleService moduleService =
        ((InternalCache) context.getCache()).getInternalDistributedSystem().getModuleService();
    String regionPath = context.getArguments();
    if (regionPath == null) {
      throw new IllegalArgumentException(
          CliStrings.CREATE_REGION__MSG__SPECIFY_VALID_REGION_PATH);
    }
    XmlEntity xmlEntity =
        new XmlEntity(CacheXml.REGION, "name", regionPath.substring(1), moduleService);
    return new CliFunctionResult(context.getMemberName(), xmlEntity.getXmlDefinition());
  }

  @Override
  public String getId() {
    return ID;
  }
}
