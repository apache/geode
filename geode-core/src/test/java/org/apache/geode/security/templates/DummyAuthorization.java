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
package org.apache.geode.security.templates;

import java.security.Principal;
import java.util.HashSet;
import java.util.Set;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.operations.OperationContext;
import org.apache.geode.cache.operations.OperationContext.OperationCode;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.security.AccessControl;
import org.apache.geode.security.NotAuthorizedException;

/**
 * A dummy implementation of the {@code AccessControl} interface that allows authorization depending
 * on the format of the {@code Principal} string.
 *
 * @since GemFire 5.5
 */
public class DummyAuthorization implements AccessControl {

  private Set allowedOps;
  private DistributedMember remoteMember;
  private LogWriter securityLogWriter;

  public static final OperationCode[] READER_OPS =
      {OperationCode.GET, OperationCode.QUERY, OperationCode.EXECUTE_CQ, OperationCode.CLOSE_CQ,
          OperationCode.STOP_CQ, OperationCode.REGISTER_INTEREST, OperationCode.UNREGISTER_INTEREST,
          OperationCode.KEY_SET, OperationCode.CONTAINS_KEY, OperationCode.EXECUTE_FUNCTION};

  public static final OperationCode[] WRITER_OPS = {OperationCode.PUT, OperationCode.PUTALL,
      OperationCode.DESTROY, OperationCode.INVALIDATE, OperationCode.REGION_CLEAR};

  public static AccessControl create() {
    return new DummyAuthorization();
  }

  public DummyAuthorization() {
    this.allowedOps = new HashSet(20);
  }

  @Override
  public void init(final Principal principal, final DistributedMember remoteMember,
      final Cache cache) throws NotAuthorizedException {
    if (principal != null) {

      final String name = principal.getName().toLowerCase();

      if (name != null) {

        if (name.equals("root") || name.equals("admin") || name.equals("administrator")) {
          addReaderOps();
          addWriterOps();
          this.allowedOps.add(OperationCode.REGION_CREATE);
          this.allowedOps.add(OperationCode.REGION_DESTROY);

        } else if (name.startsWith("writer")) {
          addWriterOps();

        } else if (name.startsWith("reader")) {
          addReaderOps();
        }

      }
    }

    this.remoteMember = remoteMember;
    this.securityLogWriter = cache.getSecurityLogger();
  }

  @Override
  public boolean authorizeOperation(String regionName, OperationContext context) {
    final OperationCode opCode = context.getOperationCode();
    this.securityLogWriter.fine("Invoked authorize operation for [" + opCode + "] in region ["
        + regionName + "] for client: " + remoteMember);
    return this.allowedOps.contains(opCode);
  }

  @Override
  public void close() {
    this.allowedOps.clear();
  }

  private void addReaderOps() {
    for (int index = 0; index < READER_OPS.length; index++) {
      this.allowedOps.add(READER_OPS[index]);
    }
  }

  private void addWriterOps() {
    for (int index = 0; index < WRITER_OPS.length; index++) {
      this.allowedOps.add(WRITER_OPS[index]);
    }
  }
}
