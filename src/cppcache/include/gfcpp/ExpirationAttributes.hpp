#ifndef __GEMFIRE_EXPIRATIONATTRIBUTES_H__
#define __GEMFIRE_EXPIRATIONATTRIBUTES_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "gfcpp_globals.hpp"
#include "ExpirationAction.hpp"

/**
 * @file
 */

namespace gemfire {
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
};      // namespace gemfire
#endif  // ifndef __GEMFIRE_EXPIRATIONATTRIBUTES_H__
