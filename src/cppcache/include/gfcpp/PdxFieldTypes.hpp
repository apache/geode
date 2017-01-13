#ifndef __GEMFIRE_PDXFIELDTYPES_HPP_
#define __GEMFIRE_PDXFIELDTYPES_HPP_

/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
* This product is protected by U.S. and international copyright
* and intellectual property laws. Pivotal products are covered by
* more patents listed at http://www.pivotal.io/patents.
*========================================================================
*/

namespace gemfire {

class CPPCACHE_EXPORT PdxFieldTypes {
 public:
  enum PdxFieldType {
    BOOLEAN,
    BYTE,
    CHAR,
    SHORT,
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    DATE,
    STRING,
    OBJECT,
    BOOLEAN_ARRAY,
    CHAR_ARRAY,
    BYTE_ARRAY,
    SHORT_ARRAY,
    INT_ARRAY,
    LONG_ARRAY,
    FLOAT_ARRAY,
    DOUBLE_ARRAY,
    STRING_ARRAY,
    OBJECT_ARRAY,
    ARRAY_OF_BYTE_ARRAYS
  };
};
}
#endif /* __GEMFIRE_PDXFIELDTYPES_HPP_ */
