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
package org.apache.geode.cache.execute.internal;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.execute.FunctionExecutionServiceImpl;
import org.apache.geode.internal.cache.execute.InternalFunctionService;

/**
 * Provides the entry point into execution of user defined {@linkplain Function}s.
 * <p>
 * Function execution provides a means to route application behaviour to {@linkplain Region data} or
 * more generically to peers in a {@link DistributedSystem} or servers in a {@link Pool}.
 * </p>
 *
 * While {@link FunctionService} is a customer facing interface to this functionality, all of the
 * work is done here. In addition, internal only functionality is exposed in this class.
 *
 * @since GemFire 7.0
 * @deprecated Please use an instance of {@link FunctionExecutionServiceImpl} or invoke static
 *             operations on {@link FunctionService} or {@link InternalFunctionService} instead.
 */
@Deprecated
public class FunctionServiceManager extends FunctionExecutionServiceImpl {
}
