/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.management.cli;

import java.util.Map;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.management.internal.cli.result.model.ResultModel;

/**
 * Command class that extends this class can only have one single command method,
 * i.e only one method that is annotated with @CliCommand.
 */
@Experimental
public abstract class SingleGfshCommand extends GfshCommand {

  /**
   * implement this method for updating the configuration of a given group
   *
   * the implementation should update the passed in config object with appropriate changes
   * if for any reason config can't be updated. throw a RuntimeException stating the reason.
   *
   * @param group the name of the group to update cluster config for
   * @param config the configuration object, never null
   * @param configObject the return value of CommandResult.getConfigObject. CommandResult is the
   *        return value of your command method.
   * @return a boolean indicating whether a change to the cluster configuration was persisted.
   */
  public boolean updateConfigForGroup(String group, CacheConfig config, Object configObject) {
    return false;
  }

  /**
   * implement this method for updating a configuration of a given group or groups, when the group
   * is not specified by the command (and hence is unavailable to be passed in as a parameter)
   *
   * @param groupConfigs map of group name -> cache config object representing configs for groups.
   * @param resultModel the return value of your command method
   * @return a boolean indicating whether a change to the cluster configuration was persisted.
   */
  public boolean updateAllConfigs(Map<String, CacheConfig> groupConfigs, ResultModel resultModel) {
    return false;
  }
}
