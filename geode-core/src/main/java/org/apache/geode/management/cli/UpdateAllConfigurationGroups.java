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
package org.apache.geode.management.cli;

/**
 * This interface is implemented by gfsh commands that can potentially update the configuration for
 * all groups (including the cluster-wide group, "cluster").
 * </p>
 * CommandExecutor will builds the list of groups for configuration updating to include all groups
 * if the command implements UpdateAllConfigurationGroups. Otherwise, the list of groups are those
 * specified with a command --group option, or "cluster" if there is no --group option.
 */
public interface UpdateAllConfigurationGroups {
}
