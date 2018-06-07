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

import java.util.List;
import java.util.Set;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.connectors.jdbc.internal.configuration.ConnectorService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

@Experimental
public class DestroyConnectionCommand extends SingleGfshCommand {
  static final String DESTROY_CONNECTION = "destroy jdbc-connection";
  static final String DESTROY_CONNECTION__HELP =
      EXPERIMENTAL + "Destroy/Remove the specified jdbc connection.";
  static final String DESTROY_CONNECTION__NAME = "name";
  static final String DESTROY_CONNECTION__NAME__HELP =
      "Name of the jdbc connection to be destroyed.";

  @CliCommand(value = DESTROY_CONNECTION, help = DESTROY_CONNECTION__HELP)
  @CliMetaData(relatedTopic = CliStrings.DEFAULT_TOPIC_GEODE)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)
  public ResultModel destroyConnection(@CliOption(key = DESTROY_CONNECTION__NAME, mandatory = true,
      help = DESTROY_CONNECTION__NAME__HELP) String name) {
    // input
    Set<DistributedMember> targetMembers = getMembers(null, null);

    // action
    List<CliFunctionResult> results =
        executeAndGetFunctionResult(new DestroyConnectionFunction(), name, targetMembers);
    ResultModel result =
        ResultModel.createMemberStatusResult(results, EXPERIMENTAL, null, false, true);
    result.setConfigObject(name);
    return result;
  }

  @Override
  public void updateClusterConfig(String group, CacheConfig config, Object element) {
    ConnectorService service =
        config.findCustomCacheElement("connector-service", ConnectorService.class);
    if (service != null) {
      CacheElement.removeElement(service.getConnection(), (String) element);
    }
  }
}
