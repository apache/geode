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

import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.partitioned.rebalance.model.Bucket;
import org.apache.geode.internal.cache.partitioned.rebalance.model.Member;
import org.apache.geode.internal.cache.partitioned.rebalance.model.Move;
import org.apache.geode.internal.cache.partitioned.rebalance.model.PartitionedRegionLoadModel;

/**
 * Move buckets from one node to another, up to a certain percentage of the source node. Each call
 * to nextStep attempts to move a single bucket.
 *
 * This uses a first fit decreasing strategy to choose which buckets to move. It sorts the buckets
 * by size, and then moves the largest bucket that is below the load we are trying to move.
 *
 * An improvement would be find the bucket that can be moved with the least cost for the most load
 * change, but because the load probe currently use the same value for load and cost, there's no
 * need to complicate things now.
 *
 *
 */
public class PercentageMoveDirector extends RebalanceDirectorAdapter {

  private PartitionedRegionLoadModel model;
  private final InternalDistributedMember source;
  private final InternalDistributedMember target;
  private final float percentage;

  private float loadToMove;
  private NavigableSet<Bucket> orderedBuckets;


  public PercentageMoveDirector(DistributedMember source, DistributedMember target,
      float percentage) {
    this.source = (InternalDistributedMember) source;
    this.target = (InternalDistributedMember) target;
    this.percentage = percentage;
  }

  @Override
  public void initialize(PartitionedRegionLoadModel model) {
    Member sourceMember = model.getMember(source);
    if (sourceMember == null) {
      throw new IllegalStateException(
          String.format(
              "Source member does not exist or is not a data store for the partitioned region %s: %s",
              model.getName(), source));
    }

    // Figure out how much load we are moving, based on the percentage.
    float sourceLoad = sourceMember.getTotalLoad();
    loadToMove = sourceLoad * percentage / 100;

    membershipChanged(model);
  }

  @Override
  public void membershipChanged(PartitionedRegionLoadModel model) {

    // We don't reset the total load to move after a membership change
    this.model = model;
    Member sourceMember = model.getMember(source);
    if (sourceMember == null) {
      throw new IllegalStateException(
          String.format(
              "Source member does not exist or is not a data store for the partitioned region %s: %s",
              model.getName(), source));
    }


    // Build the set of of buckets
    orderedBuckets = new TreeSet<Bucket>(new LoadComparator());
    for (Bucket bucket : sourceMember.getBuckets()) {
      float bucketLoad = bucket.getLoad();
      if (bucketLoad <= loadToMove) {
        orderedBuckets.add(bucket);
      }
    }
  }

  @Override
  public boolean nextStep() {
    Member targetMember = model.getMember(target);
    Member sourceMember = model.getMember(source);
    if (targetMember == null) {
      throw new IllegalStateException(
          String.format(
              "Target member does not exist or is not a data store for the partitioned region %s: %s",
              model.getName(), target));
    }

    if (targetMember.equals(sourceMember)) {
      throw new IllegalStateException(String.format(
          "Target member is the same as source member for the partitioned region %s: %s",
          model.getName(), target));
    }

    // if there is no largest bucket that we can move, we are done.
    if (orderedBuckets.isEmpty()) {
      return false;
    }
    // Take the largest bucket, and try to move that.
    Bucket bucket = orderedBuckets.last();

    float load = bucket.getLoad();

    // See if we can move this bucket to the taret node.
    if (targetMember.willAcceptBucket(bucket, sourceMember, model.enforceUniqueZones())
        .willAccept()) {

      if (model.moveBucket(new Move(sourceMember, targetMember, bucket))) {
        // If we had a successful move, decrement the load we should move.
        loadToMove -= load;

        // Remove all of the remaining buckets that are to big to move.
        // TODO - this could be O(log(n)), rather an O(n)
        Iterator<Bucket> itr = orderedBuckets.descendingIterator();
        while (itr.hasNext()) {
          Bucket next = itr.next();
          if (next.getLoad() > loadToMove) {
            itr.remove();
          } else {
            break;
          }
        }
      }
    }

    // In any case, remove the bucket from the list of buckets we'll try to move.
    orderedBuckets.remove(bucket);

    return true;
  }

  /**
   * A comparator that compares buckets by load, and then by bucket id.
   */
  private static class LoadComparator implements Comparator<Bucket> {

    @Override
    public int compare(Bucket o1, Bucket o2) {
      int result = Float.compare(o1.getLoad(), o2.getLoad());
      if (result == 0) {
        result = o2.getId() - o1.getId();
      }
      return result;
    }

  }
}
