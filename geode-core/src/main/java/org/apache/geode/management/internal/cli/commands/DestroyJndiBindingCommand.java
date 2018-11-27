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


import java.util.List;
import java.util.Set;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.JndiBindingsType;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.internal.cli.exceptions.EntityNotFoundException;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.DestroyJndiBindingFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class DestroyJndiBindingCommand extends SingleGfshCommand {
  static final String DESTROY_JNDIBINDING = "destroy jndi-binding";
  static final String DESTROY_JNDIBINDING__HELP =
      "Destroy a JNDI binding that holds the configuration for an XA datasource.";
  static final String JNDI_NAME = "name";
  static final String JNDI_NAME__HELP = "Name of the binding to be destroyed.";
  static final String IFEXISTS_HELP =
      "Skip the destroy operation when the specified JNDI binding does "
          + "not exist. Without this option, an error results from the specification "
          + "of a JNDI binding that does not exist.";

  @CliCommand(value = DESTROY_JNDIBINDING, help = DESTROY_JNDIBINDING__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_REGION)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)
  public ResultModel destroyJDNIBinding(
      @CliOption(key = JNDI_NAME, mandatory = true, help = JNDI_NAME__HELP) String jndiName,
      @CliOption(key = CliStrings.IFEXISTS, help = IFEXISTS_HELP, specifiedDefaultValue = "true",
          unspecifiedDefaultValue = "false") boolean ifExists) {

    InternalConfigurationPersistenceService service =
        (InternalConfigurationPersistenceService) getConfigurationPersistenceService();
    if (service != null) {
      List<JndiBindingsType.JndiBinding> bindings =
          service.getCacheConfig("cluster").getJndiBindings();
      JndiBindingsType.JndiBinding binding = CacheElement.findElement(bindings, jndiName);
      // fail fast when CC is running and if required binding not found assuming that
      // when CC is running then every configuration goes through CC
      if (binding == null) {
        throw new EntityNotFoundException(
            CliStrings.format("Jndi binding with jndi-name \"{0}\" does not exist.", jndiName),
            ifExists);
      }
    }

    Set<DistributedMember> targetMembers = findMembers(null, null);
    if (targetMembers.size() > 0) {
      List<CliFunctionResult> jndiCreationResult =
          executeAndGetFunctionResult(new DestroyJndiBindingFunction(), jndiName, targetMembers);
      ResultModel result = ResultModel.createMemberStatusResult(jndiCreationResult);
      result.setConfigObject(jndiName);
      return result;
    } else {
      return ResultModel.createInfo("No members found.");
    }
  }

  @Override
  public boolean updateConfigForGroup(String group, CacheConfig config, Object element) {
    CacheElement.removeElement(config.getJndiBindings(), (String) element);
    return true;
  }
}
