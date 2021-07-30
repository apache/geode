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

package org.apache.geode.cache.partition;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.partitioned.BucketId;

/**
 * <p>
 * Utility class that implements all methods in <code>PartitionListener</code> with empty
 * implementations. Applications can subclass this class and only override the methods of interest.
 * <p>
 *
 * <p>
 * Subclasses declared in a Cache XML file, it must also implement
 * {@link org.apache.geode.cache.Declarable}
 * </p>
 *
 * Note : Please request help on the Geode developer mailing list (dev@geode.apache.org) before
 * using these APIs.
 *
 *
 * @since GemFire 6.6.2
 */
public class PartitionListenerAdapter implements PartitionListener {

  @Override
  @Deprecated
  public void afterPrimary(int bucketId) {}

  @Override
  @Deprecated
  public void afterSecondary(final int bucketId) {}

  @Override
  public void afterRegionCreate(Region<?, ?> region) {}

  @Override
  @Deprecated
  public void afterBucketRemoved(int bucketId, Iterable<?> keys) {}

  @Override
  @Deprecated
  public void afterBucketCreated(int bucketId, Iterable<?> keys) {}

  @Override
  public void afterPrimary(final BucketId bucketId) {
    afterPrimary(bucketId.intValue());
  }

  @Override
  public void afterSecondary(final BucketId bucketId) {
    afterSecondary(bucketId.intValue());
  }

  @Override
  public void afterBucketRemoved(final BucketId bucketId, final Iterable<?> keys) {
    afterBucketRemoved(bucketId.intValue(), keys);
  }

  @Override
  public void afterBucketCreated(final BucketId bucketId, final Iterable<?> keys) {
    afterBucketCreated(bucketId.intValue(), keys);
  }
}
