#pragma once

#ifndef GEODE_GFCPP_EXPIRATIONATTRIBUTES_H_
#define GEODE_GFCPP_EXPIRATIONATTRIBUTES_H_

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
#include "ExpirationAction.hpp"

/**
 * @file
 */

namespace apache {
namespace geode {
namespace client {
/**
 * @class ExpirationAttributes ExpirationAttributes.hpp
 *
 * Immutable parameter object for accessing and setting the attributes
 * associated with
 * <code>timeToLive</code> and <code>idleTimeout</code>. If the expiration
 * action is not specified, it defaults to
 * <code>ExpirationAction.INVALIDATE</code>.
 * If the timeout is not specified, it defaults to zero (which means to never
 * time out).
 *
 * @see AttributesFactory
 * @see RegionAttributes
 * @see AttributesMutator
 */
class CPPCACHE_EXPORT ExpirationAttributes {
  /**
    * @brief public methods
    */
 public:
  /**
   *@brief  constructors
   */

  /** Constructs a default <code>ExpirationAttributes</code>, which indicates no
   * expiration
   * will take place.
   */
  ExpirationAttributes();

  /** Constructs an <code>ExpirationAttributes</code> with the specified
   * expiration time and
   * expiration action.
   * @param expirationTime The number of seconds for a value to live before it
   * expires
   * @param expirationAction the action to take when the value expires
   * @throws IllegalArgumentException if expirationTime is nonpositive
   */
  ExpirationAttributes(const int expirationTime,
                       const ExpirationAction::Action expirationAction =
                           ExpirationAction::INVALIDATE);

  /** Returns the number of seconds before a region or value expires.
   *
   * @return the relative number of seconds before a region or value expires
   * or zero if it will never expire
   */
  int getTimeout() const;
  void setTimeout(int timeout);

  /** Returns the action that should take place when this value or region
   * expires.
   *
   * @return the action to take when expiring
   */
  ExpirationAction::Action getAction() const;
  void setAction(ExpirationAction::Action& action);

 private:
  /** The action that should take place when this object or region expires.
   */
  ExpirationAction::Action m_action;
  /** The number of seconds since this value or region was created before it
   * expires. */
  int m_timeout;
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_GFCPP_EXPIRATIONATTRIBUTES_H_
