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

/**
 * Function used by the 'create disk-store' gfsh command to create a disk store on each member.
 *
 * @since GemFire 8.0
 */

import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.cache.DiskStoreAttributes;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.cli.Result;

public class CreateDiskStoreFunction extends CliFunction {

  private static final long serialVersionUID = 1L;

  @Override
  public CliFunctionResult executeFunction(FunctionContext context) {
    final Object[] args = (Object[]) context.getArguments();
    final String diskStoreName = (String) args[0];
    final DiskStoreAttributes diskStoreAttrs = (DiskStoreAttributes) args[1];

    InternalCache cache = (InternalCache) context.getCache();
    DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory(diskStoreAttrs);
    diskStoreFactory.create(diskStoreName);

    return new CliFunctionResult(context.getMemberName(), Result.Status.OK,
        "Created disk store " + diskStoreName);
  }

  @Override
  public String getId() {
    return CreateDiskStoreFunction.class.getName();
  }
}
