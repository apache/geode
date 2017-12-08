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

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import org.apache.geode.cache.Region;
import org.apache.geode.distributed.internal.ClusterConfigurationService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.exceptions.EntityNotFoundException;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.management.internal.configuration.utils.XmlUtils;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

/**
 * this command currently only updates the cluster configuration. Requires server restart to pick up
 * the changes.
 */
public class AlterAsyncEventQueueCommand implements GfshCommand {

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
  public Result execute(@CliOption(key = ID, mandatory = true, help = ID_HELP) String id,
      @CliOption(key = BATCH_SIZE, help = BATCH_SIZE_HELP) Integer batchSize,
      @CliOption(key = BATCH_TIME_INTERVAL,
          help = BATCH_TIME_INTERVAL_HELP) Integer batchTimeInterval,
      @CliOption(key = MAX_QUEUE_MEMORY, help = MAXIMUM_QUEUE_MEMORY_HELP) Integer maxQueueMemory,
      @CliOption(key = IFEXISTS, help = IFEXISTS_HELP, specifiedDefaultValue = "true",
          unspecifiedDefaultValue = "false") boolean ifExists)
      throws IOException, SAXException, ParserConfigurationException, TransformerException {

    // need not check if any running servers has this async-event-queue. A server with this queue id
    // may be shutdown, but we still need to update Cluster Configuration.
    ClusterConfigurationService service = getSharedConfiguration();

    if (service == null) {
      return ResultBuilder.createUserErrorResult("Cluster Configuration Service is not available. "
          + "Please connect to a locator with running Cluster Configuration Service.");
    }

    boolean locked = service.lockSharedConfiguration();
    if (!locked) {
      return ResultBuilder.createGemFireErrorResult("Unable to lock the cluster configuration.");
    }

    TabularResultData tableData = ResultBuilder.createTabularResultData();
    try {
      Region<String, Configuration> configRegion = service.getConfigurationRegion();
      for (String group : configRegion.keySet()) {
        Configuration config = configRegion.get(group);
        if (config.getCacheXmlContent() == null) {
          // skip to the next group
          continue;
        }

        boolean xmlUpdated = false;
        Document document = XmlUtils.createDocumentFromXml(config.getCacheXmlContent());
        NodeList nodeList = document.getElementsByTagName("async-event-queue");
        for (int i = 0; i < nodeList.getLength(); i++) {
          Element item = (Element) nodeList.item(i);
          String queueId = item.getAttribute("id");
          if (!id.equals(queueId)) {
            // skip to the next async-event-queue found in this xml
            continue;
          }
          // this node is the async-event-queue with the correct id
          if (batchSize != null) {
            item.setAttribute(BATCH_SIZE, batchSize + "");
          }
          if (batchTimeInterval != null) {
            item.setAttribute(BATCH_TIME_INTERVAL, batchTimeInterval + "");
          }
          if (maxQueueMemory != null) {
            item.setAttribute(MAXIMUM_QUEUE_MEMORY, maxQueueMemory + "");
          }
          // each group should have only one queue with this id defined
          tableData.accumulate("Group", group);
          tableData.accumulate("Status", "Cluster Configuration Updated");
          xmlUpdated = true;
          break;
        }

        if (xmlUpdated) {
          String newXml = XmlUtils.prettyXml(document.getFirstChild());
          config.setCacheXmlContent(newXml);
          configRegion.put(group, config);
        }
      }
    } finally {
      service.unlockSharedConfiguration();
    }

    if (tableData.rowSize("Group") == 0) {
      String message = String.format("Can not find an async event queue with id '%s'.", id);
      throw new EntityNotFoundException(message, ifExists);
    }

    // some configurations are changed, print out the warning message as well.
    tableData.setFooter(System.lineSeparator()
        + "These changes won't take effect on the running servers. " + System.lineSeparator()
        + "Please restart the servers in these groups for the changes to take effect.");
    return ResultBuilder.buildResult(tableData);
  }

  public static class Interceptor extends AbstractCliAroundInterceptor {
    @Override
    public Result preExecution(GfshParseResult parseResult) {
      Object batchSize = parseResult.getParamValue(BATCH_SIZE);
      Object batchTimeInterval = parseResult.getParamValue(BATCH_TIME_INTERVAL);
      Object maxQueueMemory = parseResult.getParamValue(MAX_QUEUE_MEMORY);

      if (batchSize == null && batchTimeInterval == null && maxQueueMemory == null) {
        return ResultBuilder
            .createUserErrorResult("need to specify at least one option to modify.");
      }
      return ResultBuilder.createInfoResult("");
    }
  }
}
