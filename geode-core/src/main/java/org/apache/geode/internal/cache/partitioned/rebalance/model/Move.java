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
    return this.source;
  }

  /**
   * @return the target
   */
  public Member getTarget() {
    return this.target;
  }

  /**
   * @return the bucket
   */
  public Bucket getBucket() {
    return this.bucket;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((this.bucket == null) ? 0 : this.bucket.hashCode());
    result = prime * result + ((this.source == null) ? 0 : this.source.hashCode());
    result = prime * result + ((this.target == null) ? 0 : this.target.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Move other = (Move) obj;
    if (this.bucket == null) {
      if (other.bucket != null)
        return false;
    } else if (!this.bucket.equals(other.bucket))
      return false;
    if (this.source == null) {
      if (other.source != null)
        return false;
    } else if (!this.source.equals(other.source))
      return false;
    if (this.target == null) {
      if (other.target != null)
        return false;
    } else if (!this.target.equals(other.target))
      return false;
    return true;
  }
}
