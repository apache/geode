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

package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.management.internal.i18n.CliStrings.CREATE_ASYNC_EVENT_QUEUE__BATCHTIMEINTERVAL__HELP;
import static org.apache.geode.management.internal.i18n.CliStrings.CREATE_ASYNC_EVENT_QUEUE__BATCH_SIZE__HELP;
import static org.apache.geode.management.internal.i18n.CliStrings.CREATE_ASYNC_EVENT_QUEUE__MAXIMUM_QUEUE_MEMORY__HELP;
import static org.apache.geode.management.internal.i18n.CliStrings.IFEXISTS;
import static org.apache.geode.management.internal.i18n.CliStrings.IFEXISTS_HELP;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.apache.commons.lang3.StringUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.xml.sax.SAXException;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.lang.Identifiable;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.cli.UpdateAllConfigurationGroupsMarker;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.exceptions.EntityNotFoundException;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

/**
 * this command currently only updates the cluster configuration. Requires server restart to pick up
 * the changes.
 */
public class AlterAsyncEventQueueCommand extends SingleGfshCommand implements
    UpdateAllConfigurationGroupsMarker {

  public static final String GROUP_STATUS_SECTION = "group-status";
  static final String COMMAND_NAME = "alter async-event-queue";
  static final String ID = "id";
  static final String BATCH_SIZE = "batch-size";
  static final String BATCH_TIME_INTERVAL = "batch-time-interval";
  static final String MAX_QUEUE_MEMORY = "max-queue-memory";
  static final String MAXIMUM_QUEUE_MEMORY = "maximum-queue-memory";

  static final String COMMAND_HELP =
      "alter attributes of async-event-queue, needs rolling restart for new attributes to take effect. ";
  static final String ID_HELP = "Id of the async event queue to be changed.";
  static final String BATCH_SIZE_HELP = CREATE_ASYNC_EVENT_QUEUE__BATCH_SIZE__HELP;
  static final String BATCH_TIME_INTERVAL_HELP = CREATE_ASYNC_EVENT_QUEUE__BATCHTIMEINTERVAL__HELP;
  static final String MAXIMUM_QUEUE_MEMORY_HELP =
      CREATE_ASYNC_EVENT_QUEUE__MAXIMUM_QUEUE_MEMORY__HELP;
  static final String PAUSE_EVENT_PROCESSING = "pause-event-processing";
  static final String PAUSE_EVENT_PROCESSING_HELP =
      "Pause event processing when the async event queue is created";

  @CliCommand(value = COMMAND_NAME, help = COMMAND_HELP)
  @CliMetaData(
      interceptor = "org.apache.geode.management.internal.cli.commands.AlterAsyncEventQueueCommand$Interceptor")
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.DEPLOY)
  public ResultModel execute(@CliOption(key = ID, mandatory = true, help = ID_HELP) String id,
      @CliOption(key = BATCH_SIZE, help = BATCH_SIZE_HELP) Integer batchSize,
      @CliOption(key = BATCH_TIME_INTERVAL,
          help = BATCH_TIME_INTERVAL_HELP) Integer batchTimeInterval,
      @CliOption(key = MAX_QUEUE_MEMORY, help = MAXIMUM_QUEUE_MEMORY_HELP) Integer maxQueueMemory,
      @CliOption(key = IFEXISTS, help = IFEXISTS_HELP, specifiedDefaultValue = "true",
          unspecifiedDefaultValue = "false") boolean ifExists,
      @CliOption(key = PAUSE_EVENT_PROCESSING, help = PAUSE_EVENT_PROCESSING_HELP,
          specifiedDefaultValue = "true",
          unspecifiedDefaultValue = "false") boolean pauseEventProcessing)
      throws IOException, SAXException, ParserConfigurationException, TransformerException,
      EntityNotFoundException {

    // need not check if any running servers has this async-event-queue. A server with this queue id
    // may be shutdown, but we still need to update Cluster Configuration.
    if (getConfigurationPersistenceService() == null) {
      return ResultModel.createError("Cluster Configuration Service is not available. "
          + "Please connect to a locator with running Cluster Configuration Service.");
    }

    if (findAEQ(id) == null) {
      String message = String.format("Can not find an async event queue with id '%s'.", id);
      throw new EntityNotFoundException(message, ifExists);
    }

    CacheConfig.AsyncEventQueue aeqConfiguration = new CacheConfig.AsyncEventQueue();
    aeqConfiguration.setId(id);
    aeqConfiguration.setPauseEventProcessing(pauseEventProcessing);

    if (batchSize != null) {
      aeqConfiguration.setBatchSize(batchSize + "");
    }
    if (batchTimeInterval != null) {
      aeqConfiguration.setBatchTimeInterval(batchTimeInterval + "");
    }
    if (maxQueueMemory != null) {
      aeqConfiguration.setMaximumQueueMemory(maxQueueMemory + "");
    }

    ResultModel result = new ResultModel();
    result.addInfo().addLine("Please restart the servers to apply any changed configuration");
    result.setConfigObject(aeqConfiguration);

    return result;
  }

  CacheConfig.AsyncEventQueue findAEQ(String aeqId) {
    CacheConfig.AsyncEventQueue queue = null;
    InternalConfigurationPersistenceService ccService =
        getConfigurationPersistenceService();
    if (ccService == null) {
      return null;
    }

    Set<String> groups = ccService.getGroups();
    for (String group : groups) {
      queue =
          Identifiable.find(ccService.getCacheConfig(group).getAsyncEventQueues(), aeqId);
      if (queue != null) {
        return queue;
      }
    }
    return queue;
  }

  @Override
  public boolean updateConfigForGroup(String group, CacheConfig config, Object configObject) {

    boolean aeqConfigsHaveBeenUpdated = false;

    List<CacheConfig.AsyncEventQueue> queues = config.getAsyncEventQueues();
    if (queues.isEmpty()) {
      return false;
    }

    CacheConfig.AsyncEventQueue aeqConfiguration =
        ((CacheConfig.AsyncEventQueue) configObject);

    String aeqId = aeqConfiguration.getId();

    for (CacheConfig.AsyncEventQueue queue : queues) {
      if (aeqId.equals(queue.getId())) {
        if (StringUtils.isNotBlank(aeqConfiguration.getBatchSize())) {
          queue.setBatchSize(aeqConfiguration.getBatchSize());
        }

        if (StringUtils.isNotBlank(aeqConfiguration.getBatchTimeInterval())) {
          queue.setBatchTimeInterval(aeqConfiguration.getBatchTimeInterval());
        }

        if (StringUtils.isNotBlank(aeqConfiguration.getMaximumQueueMemory())) {
          queue.setMaximumQueueMemory(aeqConfiguration.getMaximumQueueMemory());
        }
        if (aeqConfiguration.isPauseEventProcessing() != null) {
          queue.setPauseEventProcessing(aeqConfiguration.isPauseEventProcessing());
        }
        aeqConfigsHaveBeenUpdated = true;
      }

    }
    return aeqConfigsHaveBeenUpdated;

  }

  public static class Interceptor extends AbstractCliAroundInterceptor {
    @Override
    public ResultModel preExecution(GfshParseResult parseResult) {
      Object batchSize = parseResult.getParamValue(BATCH_SIZE);
      Object batchTimeInterval = parseResult.getParamValue(BATCH_TIME_INTERVAL);
      Object maxQueueMemory = parseResult.getParamValue(MAX_QUEUE_MEMORY);

      if (batchSize == null && batchTimeInterval == null && maxQueueMemory == null) {
        return ResultModel.createError("need to specify at least one option to modify.");
      }
      return new ResultModel();
    }
  }
}
