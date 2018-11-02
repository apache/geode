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

import org.springframework.shell.core.annotation.CliAvailabilityIndicator;

import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.i18n.CliStrings;

public class CommandAvailabilityIndicator extends GfshCommand {

  @CliAvailabilityIndicator({CliStrings.LIST_CLIENTS, CliStrings.DESCRIBE_CLIENT,
      CliStrings.DESCRIBE_CONFIG, CliStrings.EXPORT_CONFIG, CliStrings.ALTER_RUNTIME_CONFIG,
      CliStrings.ALTER_REGION, CliStrings.CREATE_REGION, CliStrings.DESTROY_REGION,
      CliStrings.REBALANCE, CliStrings.GET, CliStrings.PUT, CliStrings.REMOVE,
      CliStrings.LOCATE_ENTRY, CliStrings.QUERY, CliStrings.IMPORT_DATA, CliStrings.EXPORT_DATA,
      CliStrings.DEPLOY, CliStrings.UNDEPLOY, CliStrings.LIST_DEPLOYED,
      CliStrings.BACKUP_DISK_STORE, CliStrings.COMPACT_DISK_STORE, CliStrings.DESCRIBE_DISK_STORE,
      CliStrings.LIST_DISK_STORE, CliStrings.REVOKE_MISSING_DISK_STORE,
      CliStrings.SHOW_MISSING_DISK_STORE, CliStrings.CREATE_DISK_STORE,
      CliStrings.DESTROY_DISK_STORE, CliStrings.LIST_DURABLE_CQS, CliStrings.CLOSE_DURABLE_CLIENTS,
      CliStrings.CLOSE_DURABLE_CQS, CliStrings.COUNT_DURABLE_CQ_EVENTS,
      CliStrings.EXPORT_SHARED_CONFIG, CliStrings.IMPORT_SHARED_CONFIG, CliStrings.EXECUTE_FUNCTION,
      CliStrings.DESTROY_FUNCTION, CliStrings.LIST_FUNCTION, CliStrings.LIST_INDEX,
      CliStrings.CREATE_INDEX, CliStrings.DESTROY_INDEX, CliStrings.CREATE_DEFINED_INDEXES,
      CliStrings.CLEAR_DEFINED_INDEXES, CliStrings.DEFINE_INDEX, CliStrings.LIST_MEMBER,
      CliStrings.DESCRIBE_MEMBER, CliStrings.SHUTDOWN, CliStrings.GC, CliStrings.SHOW_DEADLOCK,
      CliStrings.SHOW_METRICS, CliStrings.SHOW_LOG, CliStrings.EXPORT_STACKTRACE,
      CliStrings.NETSTAT, CliStrings.EXPORT_LOGS, CliStrings.CHANGE_LOGLEVEL,
      CliStrings.CONFIGURE_PDX, CliStrings.CREATE_ASYNC_EVENT_QUEUE,
      CliStrings.LIST_ASYNC_EVENT_QUEUES, CliStrings.LIST_REGION, CliStrings.DESCRIBE_REGION,
      CliStrings.STATUS_SHARED_CONFIG, CliStrings.CREATE_GATEWAYSENDER,
      CliStrings.START_GATEWAYSENDER, CliStrings.PAUSE_GATEWAYSENDER,
      CliStrings.RESUME_GATEWAYSENDER, CliStrings.STOP_GATEWAYSENDER,
      CliStrings.CREATE_GATEWAYRECEIVER, CliStrings.START_GATEWAYRECEIVER,
      CliStrings.STOP_GATEWAYRECEIVER, CliStrings.LIST_GATEWAY, CliStrings.STATUS_GATEWAYSENDER,
      CliStrings.STATUS_GATEWAYRECEIVER, CliStrings.LOAD_BALANCE_GATEWAYSENDER,
      CliStrings.DESTROY_GATEWAYSENDER, AlterAsyncEventQueueCommand.COMMAND_NAME,
      DestroyAsyncEventQueueCommand.DESTROY_ASYNC_EVENT_QUEUE,
      DestroyGatewayReceiverCommand.DESTROY_GATEWAYRECEIVER,
      CreateDataSourceCommand.CREATE_DATA_SOURCE,
      CreateJndiBindingCommand.CREATE_JNDIBINDING, DestroyJndiBindingCommand.DESTROY_JNDIBINDING,
      DescribeJndiBindingCommand.DESCRIBE_JNDI_BINDING, ListJndiBindingCommand.LIST_JNDIBINDING})
  public boolean available() {
    return isOnlineCommandAvailable();
  }
}
