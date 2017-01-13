#ifndef __GEMFIRE_USERDATA_H__
#define __GEMFIRE_USERDATA_H__
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
#include "Serializable.hpp"

namespace gemfire {

typedef Serializable UserData;
typedef SharedPtr<UserData> UserDataPtr;
}

#endif  // ifndef __GEMFIRE_USERDATA_H__
