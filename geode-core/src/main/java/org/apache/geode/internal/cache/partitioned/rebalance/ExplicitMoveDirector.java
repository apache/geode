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
package org.apache.geode.internal.cache.partitioned.rebalance;

import java.util.List;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.partitioned.rebalance.model.Bucket;
import org.apache.geode.internal.cache.partitioned.rebalance.model.Member;
import org.apache.geode.internal.cache.partitioned.rebalance.model.Move;
import org.apache.geode.internal.cache.partitioned.rebalance.model.PartitionedRegionLoadModel;
import org.apache.geode.internal.cache.partitioned.rebalance.model.RefusalReason;

public class ExplicitMoveDirector extends RebalanceDirectorAdapter {

  private PartitionedRegionLoadModel model;
  private final int bucketId;
  private final InternalDistributedMember source;
  private final InternalDistributedMember target;
  private final Object key;
  private InternalDistributedSystem ds;


  public ExplicitMoveDirector(Object key, int bucketId, DistributedMember source,
      DistributedMember target, DistributedSystem distributedSystem) {
    this.key = key;
    this.bucketId = bucketId;
    this.source = (InternalDistributedMember) source;
    this.target = (InternalDistributedMember) target;
    this.ds = (InternalDistributedSystem) distributedSystem;
  }

  @Override
  public void initialize(PartitionedRegionLoadModel model) {
    this.model = model;
  }

  @Override
  public void membershipChanged(PartitionedRegionLoadModel model) {
    initialize(model);
  }

  @Override
  public boolean nextStep() {
    Bucket bucket = model.getBuckets()[bucketId];
    Member sourceMember = model.getMember(source);
    Member targetMember = model.getMember(target);
    if (sourceMember == null) {
      throw new IllegalStateException(
          String.format(
              "Source member does not exist or is not a data store for the partitioned region %s: %s",
              model.getName(), source));
    }
    if (targetMember == null) {
      throw new IllegalStateException(
          String.format(
              "Target member does not exist or is not a data store for the partitioned region %s: %s",
              model.getName(), target));
    }

    if (bucket == null) {
      throw new IllegalStateException("The bucket for key " + key + ", bucket " + bucketId
          + ", region " + model.getName() + " does not exist");
    }

    if (!bucket.getMembersHosting().contains(sourceMember)) {
      throw new IllegalStateException(
          "The bucket for key " + key + ", bucket " + bucketId + ", region " + model.getName()
              + " is not hosted by " + source + ". Members hosting: " + bucket.getMembersHosting());
    }

    RefusalReason reason =
        targetMember.willAcceptBucket(bucket, sourceMember, model.enforceUniqueZones());
    if (reason.willAccept()) {
      if (!model.moveBucket(new Move(sourceMember, targetMember, bucket))) {
        // Double check to see if the source or destination have left the DS
        List allMembers = ds.getDistributionManager().getDistributionManagerIdsIncludingAdmin();
        if (!allMembers.contains(sourceMember)) {
          throw new IllegalStateException(
              String.format(
                  "Source member does not exist or is not a data store for the partitioned region %s: %s",
                  model.getName(), source));
        }
        if (!allMembers.contains(targetMember)) {
          throw new IllegalStateException(
              String.format(
                  "Target member does not exist or is not a data store for the partitioned region %s: %s",
                  model.getName(), target));
        }
        throw new IllegalStateException(
            "Unable to move bucket " + bucket + " from " + sourceMember + " to " + targetMember);
      }
    } else {
      throw new IllegalStateException("Unable to move bucket for " + model.getName() + ". "
          + reason.formatMessage(targetMember, bucket));
    }

    return false;
  }
}
