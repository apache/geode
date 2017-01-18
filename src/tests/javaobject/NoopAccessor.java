/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package javaobject;

import org.apache.geode.cache.Cache;
import org.apache.geode.security.AccessControl;
import org.apache.geode.cache.operations.OperationContext;
import org.apache.geode.distributed.DistributedMember;

import java.security.Principal;

public class NoopAccessor implements AccessControl {

  public static AccessControl create() {
    return new NoopAccessor();
  }

  public void init(Principal principal, DistributedMember remoteMember,
      Cache cache) {
  }

  public boolean authorizeOperation(String regionName,
                                    OperationContext context) {
    return true;
  }

  public void close() {
  }
}
