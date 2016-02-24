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
package com.gemstone.gemfire.internal.cache.versions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Version;

/**
 * The version tag class for version tags for non-persistent regions. The
 * VersionSource held in these tags is an InternalDistributedMember.
 * 
 * 
 */
public class VMVersionTag extends VersionTag<InternalDistributedMember> {
  
  public VMVersionTag() {
    super();
  }

  @Override
  public void writeMember(InternalDistributedMember member, DataOutput out) throws IOException {
    member.writeEssentialData(out);
    
  }
  
  @Override
  public void setCanonicalIDs(DM dm) {
    InternalDistributedMember id = getMemberID();
    if (id != null) {
      setMemberID(dm.getCanonicalId(getMemberID()));
      id = getPreviousMemberID();
      if (id != null) {
        setPreviousMemberID(dm.getCanonicalId(id));
      }
    }
  }

  @Override
  public InternalDistributedMember readMember(DataInput in) throws IOException, ClassNotFoundException {
    return InternalDistributedMember.readEssentialData(in);
  }

  @Override
  public int getDSFID() {
    return VERSION_TAG;
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }


}
