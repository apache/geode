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

import static org.apache.geode.management.internal.cli.commands.StopGatewaySenderCommandDelegateParallelImpl.StopGatewaySenderOnMember;

import java.util.ArrayList;

import javax.management.ObjectName;

import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.GatewaySenderMXBean;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.i18n.CliStrings;

class StopGatewaySenderOnMemberWithBeanImpl implements StopGatewaySenderOnMember {

  public ArrayList<String> executeStopGatewaySenderOnMember(String id, Cache cache,
      SystemManagementService managementService, DistributedMember member) {
    GatewaySenderMXBean bean;
    ArrayList<String> statusList = new ArrayList<>();
    if (cache.getDistributedSystem().getDistributedMember().getId().equals(member.getId())) {
      bean = managementService.getLocalGatewaySenderMXBean(id);
    } else {
      ObjectName objectName = managementService.getGatewaySenderMBeanName(member, id);
      bean = managementService.getMBeanProxy(objectName, GatewaySenderMXBean.class);
    }

    if (bean == null) {
      statusList.add(member.getId());
      statusList.add(CliStrings.GATEWAY_ERROR);
      statusList.add(CliStrings.format(CliStrings.GATEWAY_SENDER_0_IS_NOT_AVAILABLE_ON_MEMBER_1,
          id, member.getId()));
      return statusList;
    }

    if (!bean.isRunning()) {
      statusList.add(member.getId());
      statusList.add(CliStrings.GATEWAY_ERROR);
      statusList.add(CliStrings.format(
          CliStrings.GATEWAY_SENDER_0_IS_NOT_RUNNING_ON_MEMBER_1, id, member.getId()));
      return statusList;
    }

    bean.stop();
    statusList.add(member.getId());
    statusList.add(CliStrings.GATEWAY_OK);
    statusList.add(CliStrings.format(CliStrings.GATEWAY_SENDER_0_IS_STOPPED_ON_MEMBER_1, id,
        member.getId()));
    return statusList;
  }
}
