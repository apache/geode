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
package org.apache.geode.management.internal.cli.converters;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.internal.cli.shell.Gfsh;

/**
 *
 *
 * @since GemFire 8.0
 */
public class ClusterMemberIdNameConverter extends BaseStringConverter {

  @Override
  public String getConverterHint() {
    return ConverterHint.ALL_MEMBER_IDNAME;
  }

  @Override
  public Set<String> getCompletionValues() {
    final Set<String> memberIdsAndNames = new TreeSet<>();

    final Gfsh gfsh = Gfsh.getCurrentInstance();

    if (gfsh != null && gfsh.isConnectedAndReady()) {
      final String[] memberIds =
          gfsh.getOperationInvoker().getDistributedSystemMXBean().listMembers();

      if (memberIds != null && memberIds.length != 0) {
        memberIdsAndNames.addAll(Arrays.asList(memberIds));
      }
    }

    return memberIdsAndNames;
  }

}
