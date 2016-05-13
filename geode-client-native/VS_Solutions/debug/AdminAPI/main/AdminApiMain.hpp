/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef _ADMINTEST_HPP_
#define _ADMINTEST_HPP_

#include <string>
#include <admin/AdministratedSystem.hpp>
#include <admin/CacheRegion.hpp>

using namespace gemfire_admin;

class AdminTest
{
  public:
    AdministratedSystem* sys;

    void begin();
    void discover( int iteration_num );
    void regionize( int level, CacheRegionPtr reg );

};

#endif

