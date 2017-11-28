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

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.connectors.jdbc.internal.InternalJdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.xml.ElementType;
import org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlGenerator;
import org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;

public class DestroyConnectionFunction implements Function<String>, InternalEntity {

  private static final String ID = DestroyConnectionFunction.class.getName();

  private final transient ExceptionHandler exceptionHandler;

  public DestroyConnectionFunction() {
    this(new ExceptionHandler());
  }

  private DestroyConnectionFunction(ExceptionHandler exceptionHandler) {
    this.exceptionHandler = exceptionHandler;
  }

  @Override
  public boolean isHA() {
    return false;
  }

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public void execute(FunctionContext<String> context) {
    ResultSender<Object> resultSender = context.getResultSender();

    InternalCache cache = (InternalCache) context.getCache();
    String memberNameOrId =
        CliUtil.getMemberNameOrId(cache.getDistributedSystem().getDistributedMember());

    String connectionName = context.getArguments();

    try {
      InternalJdbcConnectorService service = cache.getService(InternalJdbcConnectorService.class);

      if (service.getConnectionConfig(connectionName) == null) {
        resultSender.lastResult(new CliFunctionResult(memberNameOrId, false,
            CliStrings.format("Connection named \"{0}\" not found", connectionName)));

      } else {
        service.destroyConnectionConfig(connectionName);

        XmlEntity xmlEntity = new XmlEntity(CacheXml.CACHE, JdbcConnectorServiceXmlGenerator.PREFIX,
            JdbcConnectorServiceXmlParser.NAMESPACE, ElementType.CONNECTION_SERVICE.getTypeName());

        resultSender.lastResult(new CliFunctionResult(memberNameOrId, xmlEntity,
            "Destroyed JDBC connection " + connectionName + " on " + memberNameOrId));
      }

    } catch (Exception e) {
      String exceptionMsg = e.getMessage();
      if (exceptionMsg == null) {
        exceptionMsg = CliUtil.stackTraceAsString(e);
      }
      resultSender.lastResult(exceptionHandler.handleException(memberNameOrId, exceptionMsg, e));
    }
  }
}
