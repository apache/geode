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
package org.apache.geode.internal.cache.partitioned.rebalance.model;

import java.util.Objects;

/**
 * Represents a move from one node to another. Used to keep track of moves that we have already
 * attempted that have failed.
 */
public class Move {
  private final Member source;
  private final Member target;
  private final Bucket bucket;

  public Move(Member source, Member target, Bucket bucket) {
    super();
    this.source = source;
    this.target = target;
    this.bucket = bucket;
  }

  /**
   * @return the source
   */
  public Member getSource() {
    return source;
  }

  /**
   * @return the target
   */
  public Member getTarget() {
    return target;
  }

  /**
   * @return the bucket
   */
  public Bucket getBucket() {
    return bucket;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Move move = (Move) o;
    return Objects.equals(source, move.source) && Objects.equals(target, move.target)
        && Objects.equals(bucket, move.bucket);
  }

  @Override
  public int hashCode() {

    return Objects.hash(source, target, bucket);
  }
}
