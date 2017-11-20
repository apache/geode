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
package org.apache.geode.connectors.jdbc.internal.cli;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfigExistsException;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfiguration;
import org.apache.geode.connectors.jdbc.internal.InternalJdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.xml.ElementType;
import org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlGenerator;
import org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;

public class CreateConnectionFunction implements Function<ConnectionConfiguration>, InternalEntity {
  private static final Logger logger = LogService.getLogger();

  private static final String ID = CreateConnectionFunction.class.getName();

  @Override
  public boolean isHA() {
    return false;
  }

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public void execute(FunctionContext<ConnectionConfiguration> context) {
    ResultSender<Object> resultSender = context.getResultSender();

    InternalCache cache = (InternalCache) context.getCache();
    String memberNameOrId =
        CliUtil.getMemberNameOrId(cache.getDistributedSystem().getDistributedMember());

    ConnectionConfiguration configuration = context.getArguments();

    try {
      InternalJdbcConnectorService service = cache.getService(InternalJdbcConnectorService.class);
      service.createConnectionConfig(configuration);

      XmlEntity xmlEntity = new XmlEntity(CacheXml.CACHE, JdbcConnectorServiceXmlGenerator.PREFIX,
          JdbcConnectorServiceXmlParser.NAMESPACE, ElementType.CONNECTION_SERVICE.getTypeName());

      resultSender.lastResult(new CliFunctionResult(memberNameOrId, xmlEntity,
          "Created JDBC connection " + configuration.getName() + " on " + memberNameOrId));

    } catch (Exception e) {
      String exceptionMsg = e.getMessage();
      if (exceptionMsg == null) {
        exceptionMsg = CliUtil.stackTraceAsString(e);
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
      return new CliFunctionResult(memberNameOrId, false, exceptionMsg);
    }

    return new CliFunctionResult(memberNameOrId);
  }
}
