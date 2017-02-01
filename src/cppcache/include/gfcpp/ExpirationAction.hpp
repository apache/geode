#pragma once

#ifndef GEODE_GFCPP_EXPIRATIONACTION_H_
#define GEODE_GFCPP_EXPIRATIONACTION_H_

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

/**
 * @file
 */

namespace apache {
namespace geode {
namespace client {
/**
 * @class ExpirationAction ExpirationAction.hpp
 * Enumerated type for expiration actions.
 *
 * @see ExpirationAttributes
 */
class CPPCACHE_EXPORT ExpirationAction {
  // public static methods
 public:
  // types of action

  typedef enum {
    /** When the region or cached object expires, it is invalidated. */
    INVALIDATE = 0,
    /** When expired, invalidated locally only. */
    LOCAL_INVALIDATE,

    /** When the region or cached object expires, it is destroyed. */
    DESTROY,
    /** When expired, destroyed locally only. */
    LOCAL_DESTROY,

    /** invalid type. */
    INVALID_ACTION
  } Action;

  /**
  * @param name the name of the expiration action
  */
  static Action fromName(const char* name);

  /**
   * Returns whether this is the action for distributed invalidate.
   * @return true if this in INVALIDATE
   */
  inline static bool isInvalidate(const Action type) {
    return (type == INVALIDATE);
  }

  /**
   * Returns whether this is the action for local invalidate.
   * @return true if this is LOCAL_INVALIDATE
   */
  inline static bool isLocalInvalidate(const Action type) {
    return (type == LOCAL_INVALIDATE);
  }

  /** Returns whether this is the action for distributed destroy.
   * @return true if this is DESTROY
   */
  inline static bool isDestroy(const Action type) { return (type == DESTROY); }

  /** Returns whether this is the action for local destroy.
   * @return true if thisis LOCAL_DESTROY
   */
  inline static bool isLocalDestroy(const Action type) {
    return (type == LOCAL_DESTROY);
  }

  /** Returns whether this action is local.
   * @return true if this is LOCAL_INVALIDATE or LOCAL_DESTROY
   */
  inline static bool isLocal(const Action type) {
    return (type == LOCAL_INVALIDATE) || (type == LOCAL_DESTROY);
  }

  /** Returns whether this action is distributed.
   * @return true if this is INVALIDATE or DESTROY
   */
  inline static bool isDistributed(const Action type) {
    return (type == INVALIDATE) || (type == DESTROY);
  }

  /** Return the ExpirationAction represented by the specified ordinal */
  static const char* fromOrdinal(const int ordinal);

 private:
  ExpirationAction();
  ~ExpirationAction();
  static char* names[];
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_GFCPP_EXPIRATIONACTION_H_
