/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * Delta.cpp
 *
 *  Created on: Nov 9, 2009
 *      Author: abhaware
 */

#include <gfcpp/Delta.hpp>

using namespace gemfire;

DeltaPtr Delta::clone() {
  DataOutput out;
  Cacheable* ptr = dynamic_cast<Cacheable*>(this);
  out.writeObject(ptr);
  DataInput in(out.getBuffer(), out.getBufferLength());
  CacheablePtr theClonePtr;
  in.readObject(theClonePtr);
  return DeltaPtr(theClonePtr);
}
