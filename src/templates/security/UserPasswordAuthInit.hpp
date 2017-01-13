/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#ifndef __USERPASSWORDAUTHINIT__
#define __USERPASSWORDAUTHINIT__
#include "gfcpp/AuthInitialize.hpp"

/**
 * @file
 */

namespace gemfire {

/**
 * @class UserPasswordAuthInit Implementation UserPasswordAuthInit.hpp
 * UserPasswordAuthInit API for getCredentials.
 *
 * The UserPasswordAuthInit class derives from AuthInitialize base class and
 * implements the getCredential API to get the username and password.
 *
 * To use this class the <c>security-client-auth-library</c>
 * property should be set to the name of the shared library (viz.
 * <code>securityImpl</code>) and the <c>security-client-auth-factory</c>
 * property should be set to the name of the global creation function viz.
 * <code>createUserPasswordAuthInitInstance</code>.
 */
class UserPasswordAuthInit : public AuthInitialize {
  /**
   * @brief public methods
   */
 public:
  /**
   * @brief constructor
   */
  UserPasswordAuthInit() {}

  /**
   * @brief destructor
   */
  ~UserPasswordAuthInit() {}

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
  PropertiesPtr getCredentials(PropertiesPtr& securityprops,
                               const char* server);

  /**
   * @brief Invoked before the cache goes down.
   */
  void close() { return; }

  /**
   * @brief private members
   */

 private:
};
};
#endif  //__USERPASSWORDAUTHINIT__
