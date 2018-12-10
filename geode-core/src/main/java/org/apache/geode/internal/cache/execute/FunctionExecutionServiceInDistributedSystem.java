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
package org.apache.geode.internal.cache.execute;

import java.util.Set;

import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;

public interface FunctionExecutionServiceInDistributedSystem {

  /**
   * Returns an {@link Execution} object that can be used to execute a data independent function on
   * a {@link DistributedMember} of the {@link DistributedSystem}. If the member is not found in the
   * system, the function execution will throw an Exception. If the member goes down while
   * dispatching or executing the function on the member, an Exception will be thrown.
   *
   * @param system defines the distributed system
   * @param distributedMember defines a member in the distributed system
   * @throws FunctionException if either input parameter is null
   * @since GemFire 6.0
   *
   */
  Execution onMember(DistributedSystem system, DistributedMember distributedMember);

  /**
   * Returns an {@link Execution} object that can be used to execute a data independent function on
   * all members of the {@link DistributedSystem}. If one of the members goes down while dispatching
   * or executing the function on the member, an Exception will be thrown.
   *
   * @param system defines the distributed system
   *
   * @throws FunctionException if DistributedSystem instance passed is null
   * @since GemFire 6.0
   */
  Execution onMembers(DistributedSystem system, String... groups);

  /**
   * Uses {@code RANDOM_onMember} for tests.
   *
   * <p>
   * TODO: maybe merge with {@link #onMembers(DistributedSystem, String...)}
   */
  Execution onMember(DistributedSystem system, String... groups);

  /**
   * Returns an {@link Execution} object that can be used to execute a data independent function on
   * the set of {@link DistributedMember}s of the {@link DistributedSystem}. If one of the members
   * goes down while dispatching or executing the function, an Exception will be thrown.
   *
   * @param system defines the distributed system
   * @param distributedMembers set of distributed members on which {@link Function} to be executed
   * @throws FunctionException if DistributedSystem instance passed is null
   * @since GemFire 6.0
   */
  Execution onMembers(DistributedSystem system, Set<DistributedMember> distributedMembers);
}
