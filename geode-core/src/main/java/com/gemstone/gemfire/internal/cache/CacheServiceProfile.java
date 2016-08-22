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
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.Region;

/**
 * CacheServiceProfiles track additions to a {@link Region} made by a {@link CacheService}.
 * They are added to the {@link CacheDistributionAdvisor.CacheProfile} during {@link Region}
 * creation and are exchanged by the {@link CreateRegionProcessor}.
 */
public interface CacheServiceProfile {

  /**
   * Return the id of this profile
   * @return the id of this profile
   */
  String getId();

  /**
   * @param regionPath The path of the region to which this profile is associated
   * @param profile The CacheServiceProfile to check compatibility against
   * @return A string message of incompatibility or null if the profiles are compatible
   */
  String checkCompatibility(String regionPath, CacheServiceProfile profile);
}
