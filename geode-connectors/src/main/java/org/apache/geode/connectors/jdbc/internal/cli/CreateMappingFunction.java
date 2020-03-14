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

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.asyncqueue.AsyncEventQueueFactory;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.connectors.jdbc.JdbcAsyncWriter;
import org.apache.geode.connectors.jdbc.JdbcLoader;
import org.apache.geode.connectors.jdbc.JdbcWriter;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.RegionMappingExistsException;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.functions.CliFunctionResult;

/**
 * The Object[] must always be of size two.
 * The first element must be a RegionMapping.
 * The second element must be a Boolean that is true if synchronous.
 */
@Experimental
public class CreateMappingFunction extends CliFunction<Object[]> {

  CreateMappingFunction() {
    super();
  }

  @Override
  public CliFunctionResult executeFunction(FunctionContext<Object[]> context)
      throws Exception {
    JdbcConnectorService service = FunctionContextArgumentProvider.getJdbcConnectorService(context);
    // input
    Object[] arguments = context.getArguments();
    RegionMapping regionMapping = (RegionMapping) arguments[0];
    boolean synchronous = (boolean) arguments[1];

    String regionName = regionMapping.getRegionName();
    Region<?, ?> region = verifyRegionExists(context.getCache(), regionName);

    // action
    String member = context.getMemberName();
    String queueName = MappingCommandUtils.createAsyncEventQueueName(regionName);
    alterRegion(region, queueName, synchronous);
    if (isAccessor(region)) {
      return new CliFunctionResult(member, true,
          MappingConstants.THERE_IS_NO_JDBC_MAPPING_ON_PROXY_REGION);
    } else if (!synchronous) {
      createAsyncEventQueue(context.getCache(), queueName,
          region.getAttributes().getDataPolicy().withPartitioning());
    }
    createRegionMapping(service, regionMapping);

    // output
    String message =
        "Created JDBC mapping for region " + regionMapping.getRegionName() + " on " + member;
    return new CliFunctionResult(member, true, message);
  }

  /**
   * Change the existing region to have
   * the JdbcLoader as its cache-loader
   * and the given async-event-queue as one of its queues.
   */
  private void alterRegion(Region<?, ?> region, String queueName, boolean synchronous) {
    if (!isAccessor(region)) {
      region.getAttributesMutator().setCacheLoader(new JdbcLoader<>());
      if (synchronous) {
        region.getAttributesMutator().setCacheWriter(new JdbcWriter<>());
      } else {
        region.getAttributesMutator().addAsyncEventQueueId(queueName);
      }
    } else {
      if (!synchronous) {
        region.getAttributesMutator().addAsyncEventQueueId(queueName);
      }
    }
  }

  boolean isAccessor(Region<?, ?> region) {
    if (!region.getAttributes().getDataPolicy().withStorage()) {
      return true;
    }
    return region.getAttributes().getPartitionAttributes() != null
        && region.getAttributes().getPartitionAttributes().getLocalMaxMemory() == 0;
  }

  /**
   * Create an async-event-queue with the given name.
   * For a partitioned region a parallel queue is created.
   * Otherwise a serial queue is created.
   */
  private void createAsyncEventQueue(Cache cache, String queueName, boolean isPartitioned) {
    AsyncEventQueueFactory asyncEventQueueFactory = cache.createAsyncEventQueueFactory();
    asyncEventQueueFactory.setParallel(isPartitioned);
    asyncEventQueueFactory.create(queueName, new JdbcAsyncWriter());
  }

  private Region<?, ?> verifyRegionExists(Cache cache, String regionName) {
    Region<?, ?> result = cache.getRegion(regionName);
    if (result == null) {
      throw new IllegalStateException(
          "create jdbc-mapping requires that the region \"" + regionName + "\" exists.");
    }
    return result;
  }

  /**
   * Creates the named connection configuration
   */
  void createRegionMapping(JdbcConnectorService service,
      RegionMapping regionMapping) throws RegionMappingExistsException {
    service.createRegionMapping(regionMapping);
  }
}
