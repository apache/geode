#ifndef __GEMFIRE_DISTRIBUTEDSYSTEM_H__
#define __GEMFIRE_DISTRIBUTEDSYSTEM_H__
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
#include "ExceptionTypes.hpp"
#include "Properties.hpp"
#include "VectorT.hpp"

namespace gemfire {
/**
 * @class DistributedSystem DistributedSystem.hpp
 * DistributedSystem encapsulates this applications "connection" into the
 * GemFire Java servers distributed system. In order to participate in the
 * GemFire Java servers distributed system, each application needs to connect to
 * the DistributedSystem.
 * Each application can only be connected to one DistributedSystem.
 */
class SystemProperties;
class DistributedSystemImpl;
class CacheRegionHelper;
class DiffieHellman;
class TcrConnection;

class CPPCACHE_EXPORT DistributedSystem : public SharedBase {
  /**
   * @brief public methods
   */
 public:
  /**
   * Initializes the Native Client system to be able to connect to the
   * GemFire Java servers. If the name string is empty, then the default
   * "NativeDS" is used as the name of distributed system.
   * @throws LicenseException if no valid license is found.
   * @throws IllegalStateException if GFCPP variable is not set and
   *   product installation directory cannot be determined
   * @throws IllegalArgument exception if DS name is NULL
   * @throws AlreadyConnectedException if this call has succeeded once before
   *for this process
   **/
  static DistributedSystemPtr connect(const char* name,
                                      const PropertiesPtr& configPtr = NULLPTR);

  /**
   *@brief disconnect from the distributed system
   *@throws IllegalStateException if not connected
   */
  static void disconnect();

  /** Returns the SystemProperties that were used to create this instance of the
   *  DistributedSystem
   * @return  SystemProperties
   */
  static SystemProperties* getSystemProperties();

  /** Returns the name that identifies the distributed system instance
   * @return  name
   */
  virtual const char* getName() const;

  /** Returns  true if connected, false otherwise
   *
   * @return  true if connected, false otherwise
   */
  static bool isConnected();

  /** Returns a pointer to the DistributedSystem instance
   *
   * @return  instance
   */
  static DistributedSystemPtr getInstance();

  /**
   * @brief destructor
   */
  virtual ~DistributedSystem();

 protected:
  /**
   * @brief constructors
   */
  DistributedSystem(const char* name);

 private:
  char* m_name;
  static bool m_connected;
  static DistributedSystemPtr* m_instance_ptr;
  // static DistributedSystemImpl *m_impl;

 public:
  static DistributedSystemImpl* m_impl;
  friend class CacheRegionHelper;
  friend class DistributedSystemImpl;
  friend class TcrConnection;

 private:
  DistributedSystem(const DistributedSystem&);
  const DistributedSystem& operator=(const DistributedSystem&);
};

}  // namespace gemfire

#endif  // ifndef __GEMFIRE_DISTRIBUTEDSYSTEM_H__
