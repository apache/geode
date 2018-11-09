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

import static org.apache.geode.management.internal.cli.i18n.CliStrings.CREATE_ASYNC_EVENT_QUEUE__BATCHTIMEINTERVAL__HELP;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.CREATE_ASYNC_EVENT_QUEUE__BATCH_SIZE__HELP;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.CREATE_ASYNC_EVENT_QUEUE__MAXIMUM_QUEUE_MEMORY__HELP;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.IFEXISTS;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.IFEXISTS_HELP;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.apache.commons.lang3.StringUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.xml.sax.SAXException;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.exceptions.EntityNotFoundException;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

/**
 * this command currently only updates the cluster configuration. Requires server restart to pick up
 * the changes.
 */
public class AlterAsyncEventQueueCommand extends SingleGfshCommand {

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
          unspecifiedDefaultValue = "false") boolean ifExists)
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
    result.setConfigObject(aeqConfiguration);

    return result;
  }

  CacheConfig.AsyncEventQueue findAEQ(String aeqId) {
    CacheConfig.AsyncEventQueue queue = null;
    InternalConfigurationPersistenceService ccService =
        (InternalConfigurationPersistenceService) this.getConfigurationPersistenceService();
    if (ccService == null) {
      return null;
    }

    Set<String> groups = ccService.getGroups();
    for (String group : groups) {
      queue =
          CacheElement.findElement(ccService.getCacheConfig(group).getAsyncEventQueues(), aeqId);
      if (queue != null) {
        return queue;
      }
    }
    return queue;
  }

  @Override
  public boolean updateAllConfigs(Map<String, CacheConfig> configs, ResultModel resultModel) {
    CacheConfig.AsyncEventQueue aeqConfiguration =
        (CacheConfig.AsyncEventQueue) resultModel.getConfigObject();
    List<GroupQueuePair> queuesToAlter = configs.keySet()
        .stream()
        .map(group -> GroupQueuePair.of(group, CacheElement.findElement(
            configs.get(group).getAsyncEventQueues(),
            aeqConfiguration.getId())))
        .filter(p -> p.snd != null)
        .collect(Collectors.toList());

    if (queuesToAlter.isEmpty()) {
      return false;
    }

    TabularResultModel tableData = resultModel.addTable(GROUP_STATUS_SECTION);
    tableData.setColumnHeader("Group", "Status");
    queuesToAlter.forEach(p -> {
      String group = p.fst;
      CacheConfig.AsyncEventQueue queue = p.snd;

      if (StringUtils.isNotBlank(aeqConfiguration.getBatchSize())) {
        queue.setBatchSize(aeqConfiguration.getBatchSize());
      }

      if (StringUtils.isNotBlank(aeqConfiguration.getBatchTimeInterval())) {
        queue.setBatchTimeInterval(aeqConfiguration.getBatchTimeInterval());
      }

      if (StringUtils.isNotBlank(aeqConfiguration.getMaximumQueueMemory())) {
        queue.setMaximumQueueMemory(aeqConfiguration.getMaximumQueueMemory());
      }

      tableData.addRow(group, "Cluster Configuration Updated");
    });

    tableData.setFooter(
        System.lineSeparator()
            + "These changes won't take effect on the running servers. "
            + System.lineSeparator()
            + "Please restart the servers in these groups for the changes to take effect.");

    return true;
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

  static class GroupQueuePair {
    public GroupQueuePair(String fst, CacheConfig.AsyncEventQueue snd) {
      this.fst = fst;
      this.snd = snd;
    }

    final String fst;
    final CacheConfig.AsyncEventQueue snd;

    static GroupQueuePair of(String group, CacheConfig.AsyncEventQueue queue) {
      return new GroupQueuePair(group, queue);
    }
  }
}
