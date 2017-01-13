/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * GFSsl.hpp
 *
 *  Created on: 28-Apr-2010
 *      Author: ankurs
 */

#ifndef GFSSL_HPP_
#define GFSSL_HPP_

#include <ace/INET_Addr.h>
#include <ace/OS.h>

class GFSsl {
 public:
  virtual ~GFSsl(){};
  virtual int setOption(int, int, void*, int) = 0;
  virtual int listen(ACE_INET_Addr, unsigned) = 0;
  virtual int connect(ACE_INET_Addr, unsigned) = 0;
  virtual ssize_t recv(void*, size_t, const ACE_Time_Value*, size_t*) = 0;
  virtual ssize_t send(const void*, size_t, const ACE_Time_Value*, size_t*) = 0;
  virtual int getLocalAddr(ACE_Addr&) = 0;
  virtual void close() = 0;
};

#endif /* GFSSL_HPP_ */
