/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef NON_COPYABLE_H_
#define NON_COPYABLE_H_

namespace gemfire {
class CPPCACHE_EXPORT NonCopyable {
 protected:
  NonCopyable() {}
  ~NonCopyable() {}

 private:
  NonCopyable(const NonCopyable&);
};
class CPPCACHE_EXPORT NonAssignable {
 protected:
  NonAssignable() {}
  ~NonAssignable() {}

 private:
  const NonAssignable& operator=(const NonAssignable&);
};
}  // namespace gemfire

#endif
