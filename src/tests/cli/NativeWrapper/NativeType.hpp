/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#ifndef _GF_NATIVETYPE_HPP
#define _GF_NATIVETYPE_HPP

/**
* This class is to test GC invocation for managed wrapper objects
* when a method on the underlying native object is still in progress.
* See bug #309 for detailed scenario.
*/
class NativeType
{
public:
  /** default constructor */
  NativeType();
  /** destructor */
  ~NativeType();
  /** test method that allocated large amounts of memory in steps */
  bool doOp(int size, int numOps, int numGCOps);
};

#endif // _GF_NATIVETYPE_HPP

