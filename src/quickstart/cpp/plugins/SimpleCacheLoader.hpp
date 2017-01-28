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

/*
 * SimpleCacheLoader QuickStart Example.
 *
 * This is a simple implementation of a Cache Loader
 * It merely prints the events captured from the Geode Native Client.
 *
 */

// Include the Geode library.
#include <gfcpp/GeodeCppCache.hpp>
#include <gfcpp/CacheLoader.hpp>

// Use the "geode" namespace.
using namespace apache::geode::client;

// The SimpleCacheLoader class.
class SimpleCacheLoader : public CacheLoader {
 public:
  virtual CacheablePtr load(const RegionPtr& region, const CacheableKeyPtr& key,
                            const UserDataPtr& aCallbackArgument);

  virtual void close(const RegionPtr& region);
};
