#ifndef __GEMFIRE_PDXTYPES_H__
#define __GEMFIRE_PDXTYPES_H__

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <gfcpp/gfcpp_globals.hpp>

class PdxTypes {
 public:
  static const int8_t BYTE_SIZE = 1;

  static const int8_t BOOLEAN_SIZE = 1;

  static const int8_t CHAR_SIZE = 2;

  static const int8_t SHORT_SIZE = 2;

  static const int8_t INTEGER_SIZE = 4;

  static const int8_t FLOAT_SIZE = 4;

  static const int8_t LONG_SIZE = 8;

  static const int8_t DOUBLE_SIZE = 8;

  static const int8_t DATE_SIZE = 8;
};

#endif  // __GEMFIRE_PDXTYPES_H__
