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

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import org.springframework.shell.core.annotation.CliCommand;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.JndiBindingsType;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.functions.ListJndiBindingFunction;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class ListJndiBindingCommand extends GfshCommand {

  public static final String LIST_JNDIBINDING = "list jndi-binding";
  private static final String LIST_JNDIBINDING__HELP =
      "List all jndi bindings, active and configured. An active binding is one that is bound to the server's jndi context (and also listed in the cluster config). A configured binding is one that is listed in the cluster config, but may not be active on the servers.";
  @Immutable
  private static final ListJndiBindingFunction LIST_BINDING_FUNCTION =
      new ListJndiBindingFunction();

  @CliCommand(value = LIST_JNDIBINDING, help = LIST_JNDIBINDING__HELP)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public ResultModel listJndiBinding() {
    ResultModel resultModel = new ResultModel();
    TabularResultModel configTable = null;

    InternalConfigurationPersistenceService ccService =
        getConfigurationPersistenceService();
    if (ccService != null) {
      configTable = resultModel.addTable("clusterConfiguration");
      // we don't support creating jndi binding with random group name yet
      CacheConfig cacheConfig = ccService.getCacheConfig("cluster");
      List<JndiBindingsType.JndiBinding> jndiBindings = cacheConfig.getJndiBindings();
      if (jndiBindings.size() == 0) {
        configTable.setHeader("No JNDI-bindings found in cluster configuration");
      } else {
        configTable.setHeader("Configured JNDI bindings: ");
        for (JndiBindingsType.JndiBinding jndiBinding : jndiBindings) {
          configTable.accumulate("Group Name", "cluster");
          configTable.accumulate("JNDI Name", jndiBinding.getJndiName());
          configTable.accumulate("JDBC Driver Class", jndiBinding.getJdbcDriverClass());
        }
      }
    }

    Set<DistributedMember> members = findMembers(null, null);

    if (members.size() == 0) {
      if (configTable == null) {
        return ResultModel.createError("No members found");
      }
      configTable.setFooter("No members found");
      return resultModel;
    }

    TabularResultModel memberTable = resultModel.addTable("memberConfiguration");
    List<CliFunctionResult> rc = executeAndGetFunctionResult(LIST_BINDING_FUNCTION, null, members);

    memberTable.setHeader("Active JNDI bindings found on each member: ");
    for (CliFunctionResult oneResult : rc) {
      Serializable[] serializables = getSerializables(oneResult);
      for (int i = 0; i < serializables.length; i += 2) {
        memberTable.accumulate("Member", oneResult.getMemberIdOrName());
        memberTable.accumulate("JNDI Name", (String) serializables[i]);
        memberTable.accumulate("JDBC Driver Class", (String) serializables[i + 1]);
      }
    }
    return resultModel;
  }

  @SuppressWarnings("deprecation")
  private Serializable[] getSerializables(CliFunctionResult oneResult) {
    return oneResult.getSerializables();
  }
}
