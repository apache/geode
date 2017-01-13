#ifndef __GEMFIRE_CQRESULTS_H__
#define __GEMFIRE_CQRESULTS_H__
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
#include "Serializable.hpp"
#include "CacheableBuiltins.hpp"
#include "SelectResults.hpp"
namespace gemfire {

/**
 * @class CqResults CqResults.hpp
 *
 * A CqResults is obtained by executing a Query on the server.
 * This will be a StructSet.
 */
class CPPCACHE_EXPORT CqResults : public SelectResults {};

}  // namespace gemfire

#endif  // ifndef __GEMFIRE_CQRESULTS_H__
