#pragma once

#ifndef GEODE_POOLEDBASE_H_
#define GEODE_POOLEDBASE_H_

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

#include <gfcpp/gfcpp_globals.hpp>

namespace apache {
namespace geode {
namespace client {

class PooledBasePool;

/**
 * @class PooledBase PooledBase.hpp
 *
 * This abstract base class is the base class of all user objects
 * that have the shared capability of reference counting.
 */
class CPPCACHE_EXPORT PooledBase {
 public:
  PooledBase(PooledBasePool* pool);

  void preserveSB() const;
  void releaseSB() const;

  inline int32_t refCount() { return m_refCount; }

  virtual ~PooledBase();

  /** called just prior to inserting an object back into the pool. */
  virtual void prePool();

  /** called just after removing an object from the pool. */
  virtual void postPool();

 private:
  volatile int32_t m_refCount;
  PooledBasePool* m_pool;

  void operator=(const PooledBase& rhs);
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_POOLEDBASE_H_
