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
package org.apache.geode.internal.cache.wan.parallel;

import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.internal.cache.EventID;

/**
 * ShadowPartitionedRegion for replicated region uses this PartitionResolver. In
 * shadowPR for RR, we are storing eventID as key into the buckets calculated
 * from "key" of the original events. It means unlike to normal scenario, in
 * this case bucket id (in which EventIDs are added as key) is different from
 * the bucket id calculated using EventID's hashcode. To not to break the
 * contract of key and its bucket id, we are providing an internal resolver
 * which will return a correct bucketId when EventID will be used as the key in
 * RR with PArallelGatewaySender
 * 
 * We are assuming here, before calling getRoutingObejct in this Resolver, key
 * of EntryOperation i.e. EventID as already been processed by
 * ParallelGatewaySenderImpl#setModifiedEvent where we are calculating bucketId
 * from original event's key and storing it in EventID.
 * 
 */
public class RREventIDResolver implements PartitionResolver {

  public void close() {

  }

  public Object getRoutingObject(EntryOperation opDetails) {
    EventID eventID = (EventID)opDetails.getKey();
    return eventID.getBucketID();
  }

  public String getName() {
    return getClass().getName();
  }

}
