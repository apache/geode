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

import static org.apache.geode.connectors.jdbc.internal.xml.ElementType.CONNECTION_SERVICE;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlGenerator.PREFIX;
import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.NAMESPACE;
import static org.apache.geode.internal.cache.xmlcache.CacheXml.CACHE;

import java.io.Serializable;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.connectors.jdbc.internal.InternalJdbcConnectorService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;

/**
 * Provides JDBC command dependencies provided in the FunctionContext
 */
class FunctionContextArgumentProvider implements Serializable {

  static InternalCache getCacheFromContext(FunctionContext<?> context) {
    return (InternalCache) context.getCache();
  }

  static String getMemberFromContext(FunctionContext<?> context) {
    InternalCache cache = getCacheFromContext(context);
    return CliUtil.getMemberNameOrId(cache.getDistributedSystem().getDistributedMember());
  }

  /**
   * Returns the JdbcConnectorService
   */
  InternalJdbcConnectorService getJdbcConnectorService(FunctionContext<?> context) {
    return getCacheFromContext(context).getService(InternalJdbcConnectorService.class);
  }

  /**
   * Returns the name of the distributed member or its id if it has no name
   */
  String getMember(FunctionContext<?> context) {
    return getMemberFromContext(context);
  }

  /**
   * Returns XmlEntity for JdbcConnectorServiceXmlGenerator snippet of cache xml
   */
  XmlEntity createXmlEntity(FunctionContext<?> context) {
    return new XmlEntity(createCacheProvider(context), CACHE, PREFIX, NAMESPACE,
        CONNECTION_SERVICE.getTypeName());
  }

  private XmlEntity.CacheProvider createCacheProvider(FunctionContext<?> context) {
    return () -> getCacheFromContext(context);
  }
}
