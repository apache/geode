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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.ConfigSource;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.internal.cache.xmlcache.CacheXmlGenerator;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.functions.CliFunctionResult;

public class ExportConfigFunction implements InternalFunction<Object> {
  private static final Logger logger = LogService.getLogger();

  public static final String ID = ExportConfigFunction.class.getName();

  private static final long serialVersionUID = 1L;

  @Override
  @SuppressWarnings("deprecation")
  public void execute(FunctionContext<Object> context) {
    // Declared here so that it's available when returning a Throwable
    String memberId = "";

    try {
      Cache cache = context.getCache();
      DistributedMember member = cache.getDistributedSystem().getDistributedMember();

      memberId = member.getId();
      // If they set a name use it instead
      if (!member.getName().equals("")) {
        memberId = member.getName();
      }

      // Generate the cache XML
      StringWriter xmlWriter = new StringWriter();
      PrintWriter printWriter = new PrintWriter(xmlWriter);
      CacheXmlGenerator.generate(cache, printWriter, false, false);
      printWriter.close();

      // Generate the properties file
      DistributionConfigImpl config =
          (DistributionConfigImpl) ((InternalDistributedSystem) cache.getDistributedSystem())
              .getConfig();
      StringBuilder propStringBuf = new StringBuilder();
      String lineSeparator = System.getProperty("line.separator");
      for (Map.Entry entry : config.getConfigPropsFromSource(ConfigSource.runtime()).entrySet()) {
        if (entry.getValue() != null && !entry.getValue().equals("")) {
          propStringBuf.append(entry.getKey()).append("=").append(entry.getValue())
              .append(lineSeparator);
        }
      }
      for (Map.Entry entry : config.getConfigPropsFromSource(ConfigSource.api()).entrySet()) {
        if (entry.getValue() != null && !entry.getValue().equals("")) {
          propStringBuf.append(entry.getKey()).append("=").append(entry.getValue())
              .append(lineSeparator);
        }
      }
      for (Map.Entry entry : config.getConfigPropsDefinedUsingFiles().entrySet()) {
        if (entry.getValue() != null && !entry.getValue().equals("")) {
          propStringBuf.append(entry.getKey()).append("=").append(entry.getValue())
              .append(lineSeparator);
        }
      }
      // fix for bug 46653
      for (Map.Entry entry : config.getConfigPropsFromSource(ConfigSource.launcher()).entrySet()) {
        if (entry.getValue() != null && !entry.getValue().equals("")) {
          propStringBuf.append(entry.getKey()).append("=").append(entry.getValue())
              .append(lineSeparator);
        }
      }

      CliFunctionResult result = new CliFunctionResult(memberId,
          new String[] {xmlWriter.toString(), propStringBuf.toString()});

      context.getResultSender().lastResult(result);

    } catch (CacheClosedException cce) {
      CliFunctionResult result = new CliFunctionResult(memberId, false, null);
      context.getResultSender().lastResult(result);

    } catch (VirtualMachineError e) {
      org.apache.geode.SystemFailure.initiateFailure(e);
      throw e;

    } catch (Throwable th) {
      org.apache.geode.SystemFailure.checkFailure();
      logger.error("Could not export config {}", th.getMessage(), th);
      CliFunctionResult result = new CliFunctionResult(memberId, th, null);
      context.getResultSender().lastResult(result);
    }
  }

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public boolean optimizeForWrite() {
    return false;
  }

  @Override
  public boolean isHA() {
    return false;
  }
}
