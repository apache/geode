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
package org.apache.geode.management.internal.cli.functions;

import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.functions.CliFunctionResult;

/**
 * Function used by the 'destroy disk-store' gfsh command to destroy a disk store on each member.
 *
 * @since GemFire 8.0
 */
public class DestroyDiskStoreFunction implements InternalFunction<DestroyDiskStoreFunctionArgs> {
  private static final long serialVersionUID = 1L;

  @Override
  @SuppressWarnings("deprecation")
  public void execute(FunctionContext<DestroyDiskStoreFunctionArgs> context) {
    // Declared here so that it's available when returning a Throwable
    String memberId;

    final DestroyDiskStoreFunctionArgs args = context.getArguments();

    InternalCache cache = (InternalCache) context.getCache();

    DistributedMember member = cache.getDistributedSystem().getDistributedMember();

    memberId = member.getId();
    // If they set a name use it instead
    if (!member.getName().equals("")) {
      memberId = member.getName();
    }

    DiskStore diskStore = cache.findDiskStore(args.getId());

    CliFunctionResult result;
    try {
      if (diskStore != null) {
        XmlEntity xmlEntity = new XmlEntity(CacheXml.DISK_STORE, "name", args.getId());
        diskStore.destroy();
        result = new CliFunctionResult(memberId, xmlEntity, "Success");
      } else {
        if (args.isIfExists()) {
          result = new CliFunctionResult(memberId, true,
              "Skipping: Disk store not found on this member");
        } else {
          result = new CliFunctionResult(memberId, false, "Disk store not found on this member");
        }
      }
    } catch (IllegalStateException ex) {
      result = new CliFunctionResult(memberId, false, ex.getMessage());
    }
    final ResultSender<CliFunctionResult> resultSender = context.getResultSender();
    resultSender.lastResult(result);
  }
}
