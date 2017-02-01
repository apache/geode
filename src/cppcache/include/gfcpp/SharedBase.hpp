#pragma once

#ifndef GEODE_GFCPP_SHAREDBASE_H_
#define GEODE_GFCPP_SHAREDBASE_H_

// SharedBase.hpp     -*- mode: c++ -*-

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

#include "gfcpp_globals.hpp"

/** @file
*/

namespace apache {
namespace geode {
namespace client {

/**
 * @class SharedBase SharedBase.hpp
 *
 * This abstract base class is the base class of all user objects
 * that have the shared capability of reference counting.
 */
class CPPCACHE_EXPORT SharedBase {
 public:
  /** Constructor. */
  inline SharedBase() : m_refCount(0) {}

  /** Atomically increment reference count */
  void preserveSB() const;

  /**
   * Atomically decrement reference count, the SharedBase object is
   * automatically deleted when its reference count goes to zero
   */
  void releaseSB() const;

  /** @return the reference count */
  inline int32_t refCount() { return m_refCount; }

 protected:
  inline SharedBase(bool noInit) {}

  virtual ~SharedBase() {}

 private:
  mutable volatile int32_t m_refCount;

  void operator=(const SharedBase& rhs);
};

/**
 * Class encapsulating a NULL SharedBase smart pointer. This is for passing
 * NULL pointers implicitly to copy constructor of <code>SharedPtr</code>
 * class.
 */
class CPPCACHE_EXPORT NullSharedBase : public SharedBase {
 public:
  static const NullSharedBase* const s_instancePtr;

 private:
  NullSharedBase() {}
  // never defined
  NullSharedBase(const NullSharedBase&);
  NullSharedBase& operator=(const NullSharedBase&);

  friend class SharedBase;  // just to get rid of warning with gcc3.x
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#define NULLPTR ::apache::geode::client::NullSharedBase::s_instancePtr

#endif // GEODE_GFCPP_SHAREDBASE_H_
