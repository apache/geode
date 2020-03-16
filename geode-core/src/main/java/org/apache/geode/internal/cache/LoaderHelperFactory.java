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
package org.apache.geode.internal.cache;

import org.apache.geode.cache.LoaderHelper;

/**
 * The LoaderHelperFactory creates a LoaderHelper class which is used by a
 * {@link org.apache.geode.cache.CacheLoader}.
 * The LoaderHelperFactory inspiration came from a need to allow Partitioned Regions to generate a
 * LoaderHelper that was outside the context of the Region the loader invoked from.
 *
 * @since GemFire 5.0
 */
public interface LoaderHelperFactory {
  LoaderHelper createLoaderHelper(Object key, Object callbackArgument, boolean netSearchAllowed,
      boolean netLoadAllowed, SearchLoadAndWriteProcessor searcher);

}
