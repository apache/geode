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

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.cli.commands.RegionCommandsUtils;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.configuration.realizers.RegionConfigRealizer;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.util.RegionPath;

/**
 *
 * @since GemFire 7.0
 */
@SuppressWarnings("rawtypes")
public class RegionCreateFunction implements InternalFunction {

  private static final Logger logger = LogService.getLogger();

  private static final long serialVersionUID = 8746830191680509335L;

  private static final String ID = RegionCreateFunction.class.getName();

  @Immutable
  public static final RegionCreateFunction INSTANCE = new RegionCreateFunction();

  @Immutable
  private static final RegionConfigRealizer realizer = new RegionConfigRealizer();

  @Override
  public boolean isHA() {
    return false;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void execute(FunctionContext context) {
    ResultSender<Object> resultSender = context.getResultSender();

    InternalCache cache =
        ((InternalCache) context.getCache()).getCacheForProcessingClientRequests();
    String memberNameOrId = context.getMemberName();

    CreateRegionFunctionArgs regionCreateArgs = (CreateRegionFunctionArgs) context.getArguments();

    try {
      RegionPath regionPath = new RegionPath(regionCreateArgs.getRegionPath());
      getRealizer().create(regionCreateArgs.getConfig(), regionCreateArgs.getRegionPath(), cache);
      XmlEntity xmlEntity = new XmlEntity(CacheXml.REGION, "name", regionPath.getRootRegionName());
      resultSender.lastResult(new CliFunctionResult(memberNameOrId, xmlEntity.getXmlDefinition(),
          CliStrings.format(CliStrings.CREATE_REGION__MSG__REGION_0_CREATED_ON_1,
              regionCreateArgs.getRegionPath(), memberNameOrId)));
    } catch (IllegalStateException e) {
      String exceptionMsg = e.getMessage();
      String localizedString =
          "Only regions with persistence or overflow to disk can specify DiskStore";
      if (localizedString.equals(e.getMessage())) {
        exceptionMsg = exceptionMsg + " "
            + CliStrings.format(CliStrings.CREATE_REGION__MSG__USE_ONE_OF_THESE_SHORTCUTS_0,
                new Object[] {String.valueOf(RegionCommandsUtils.PERSISTENT_OVERFLOW_SHORTCUTS)});
      }
      resultSender.lastResult(handleException(memberNameOrId, exceptionMsg, null/* do not log */));
    } catch (IllegalArgumentException e) {
      resultSender.lastResult(handleException(memberNameOrId, e.getMessage(), e));
    } catch (RegionExistsException e) {
      if (regionCreateArgs.isIfNotExists()) {
        resultSender
            .lastResult(new CliFunctionResult(memberNameOrId, CliFunctionResult.StatusState.OK,
                CliStrings.format(
                    CliStrings.CREATE_REGION__MSG__SKIPPING_0_REGION_PATH_1_ALREADY_EXISTS,
                    memberNameOrId, regionCreateArgs.getRegionPath())));
      } else {
        String exceptionMsg =
            CliStrings.format(CliStrings.CREATE_REGION__MSG__REGION_PATH_0_ALREADY_EXISTS_ON_1,
                regionCreateArgs.getRegionPath(), memberNameOrId);
        resultSender.lastResult(handleException(memberNameOrId, exceptionMsg, e));
      }
    } catch (Exception e) {
      String exceptionMsg = e.getMessage();
      if (exceptionMsg == null) {
        exceptionMsg = ExceptionUtils.getStackTrace(e);
      }
      resultSender.lastResult(handleException(memberNameOrId, exceptionMsg, e));
    }
  }

  private CliFunctionResult handleException(final String memberNameOrId, final String exceptionMsg,
      final Exception e) {
    if (e != null && logger.isDebugEnabled()) {
      logger.debug(e.getMessage(), e);
    }

    if (exceptionMsg != null) {
      return new CliFunctionResult(memberNameOrId, CliFunctionResult.StatusState.ERROR,
          exceptionMsg);
    }

    return new CliFunctionResult(memberNameOrId, CliFunctionResult.StatusState.ERROR);
  }

  @Override
  public String getId() {
    return ID;
  }

  @VisibleForTesting
  protected RegionConfigRealizer getRealizer() {
    return realizer;
  }
}
