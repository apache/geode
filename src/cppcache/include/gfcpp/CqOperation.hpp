#ifndef __GEMFIRE_CQ_OPERATION_H__
#define __GEMFIRE_CQ_OPERATION_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "gfcpp_globals.hpp"

/**
 * @file
 */

namespace gemfire {
/**
 * @class Operation CqOperation.hpp
 * Enumerated type for Operation actions.
 */
class CPPCACHE_EXPORT CqOperation {
  // public static methods
 public:
  // types of operation CORESPONDING TO THE ONES in gemfire.cache.Operation

  typedef enum {
    OP_TYPE_INVALID = -1,
    OP_TYPE_CREATE = 1,
    OP_TYPE_UPDATE = 2,
    OP_TYPE_INVALIDATE = 4,
    OP_TYPE_REGION_CLEAR = 8,
    OP_TYPE_DESTROY = 16,
    OP_TYPE_MARKER = 32
  } CqOperationType;
};
}  // namespace gemfire
#endif  // ifndef __GEMFIRE_CQ_OPERATION_H__
