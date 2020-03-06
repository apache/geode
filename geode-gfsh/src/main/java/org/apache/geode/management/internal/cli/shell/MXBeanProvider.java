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
package org.apache.geode.management.internal.cli.shell;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.geode.internal.Assert.assertState;

import java.io.IOException;
import java.util.Set;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.Query;
import javax.management.QueryExp;

import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.MemberMXBean;
import org.apache.geode.management.internal.ManagementConstants;

public class MXBeanProvider {

  /**
   * Gets a proxy to the DistributedSystemMXBean from the GemFire Manager's MBeanServer, or null if
   * unable to find the DistributedSystemMXBean.
   * </p>
   *
   * @return a proxy to the DistributedSystemMXBean from the GemFire Manager's MBeanServer, or null
   *         if unable to find the DistributedSystemMXBean.
   */
  public static DistributedSystemMXBean getDistributedSystemMXBean()
      throws IOException, MalformedObjectNameException {
    assertState(Gfsh.isCurrentInstanceConnectedAndReady(),
        "Gfsh must be connected in order to get proxy to a GemFire DistributedSystemMXBean.");
    return Gfsh.getCurrentInstance().getOperationInvoker().getDistributedSystemMXBean();
  }

  /**
   * Gets a proxy to the MemberMXBean for the GemFire member specified by member name or ID from the
   * GemFire Manager's MBeanServer.
   * </p>
   *
   * @param member a String indicating the GemFire member's name or ID.
   * @return a proxy to the MemberMXBean having the specified GemFire member's name or ID from the
   *         GemFire Manager's MBeanServer, or null if no GemFire member could be found with the
   *         specified member name or ID.
   * @see #getMemberMXBean(String, String)
   */
  public static MemberMXBean getMemberMXBean(final String member) throws IOException {
    return getMemberMXBean(null, member);
  }

  public static MemberMXBean getMemberMXBean(final String serviceName, final String member)
      throws IOException {
    assertState(Gfsh.isCurrentInstanceConnectedAndReady(),
        "Gfsh must be connected in order to get proxy to a GemFire Member MBean.");

    MemberMXBean memberBean = null;

    try {
      String objectNamePattern = ManagementConstants.OBJECTNAME__PREFIX;

      objectNamePattern += (isBlank(serviceName)
          ? ""
          : "service=" + serviceName + ",");
      objectNamePattern += "type=Member,*";

      // NOTE throws a MalformedObjectNameException, however, this should not happen since the
      // ObjectName is constructed
      // here in a conforming pattern
      final ObjectName objectName = ObjectName.getInstance(objectNamePattern);

      final QueryExp query = Query.or(Query.eq(Query.attr("Name"), Query.value(member)),
          Query.eq(Query.attr("Id"), Query.value(member)));

      final Set<ObjectName> memberObjectNames =
          Gfsh.getCurrentInstance().getOperationInvoker().queryNames(objectName, query);

      if (!memberObjectNames.isEmpty()) {
        memberBean = Gfsh.getCurrentInstance().getOperationInvoker()
            .getMBeanProxy(memberObjectNames.iterator().next(), MemberMXBean.class);
      }
    } catch (MalformedObjectNameException e) {
      Gfsh.getCurrentInstance().logSevere(e.getMessage(), e);
    }

    return memberBean;
  }


}
