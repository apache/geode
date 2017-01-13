#ifndef __GEMFIRE_AUTHINITIALIZE_H__
#define __GEMFIRE_AUTHINITIALIZE_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

/**
 * @file
 */

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"

namespace gemfire {

/**
 * @class AuthInitialize AuthInitialize.hpp
 * Specifies the mechanism to obtain credentials for a client.
 * It is mandantory for clients when the server is running in secure
 * mode having a <code>security-client-authenticator</code> module specified.
 * Implementations should register the library path as
 * <code>security-client-auth-library</code> system property and factory
 * function (a zero argument function returning pointer to an
 * AuthInitialize object) as the <code>security-client-auth-factory</code>
 * system property.
 */
class CPPCACHE_EXPORT AuthInitialize : public SharedBase {
 public:
  /**@brief initialize with the given set of security properties
   * and return the credentials for the client as properties.
   * @param props the set of security properties provided to the
   * <code>DistributedSystem.connect</code> method
   * @param server it is the ID of the current endpoint.
   * The format expected is "host:port".
   * @returns the credentials to be used for the given <code>server</code>
   * @remarks This method can modify the given set of properties. For
   * example it may invoke external agents or even interact with the user.
   */
  virtual PropertiesPtr getCredentials(PropertiesPtr& securityprops,
                                       const char* server) = 0;

  /**@brief Invoked before the cache goes down. */
  virtual void close() = 0;
};

}  // namespace gemfire

#endif  // ifndef __GEMFIRE_AUTHINITIALIZE_H__
