#ifndef _GEMFIRE_AUTODELETE_HPP_
#define _GEMFIRE_AUTODELETE_HPP_

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include <gfcpp/gfcpp_globals.hpp>

namespace gemfire {
template <typename T>
class DeleteObject {
 private:
  T*& m_p;
  bool m_cond;

 public:
  DeleteObject(T*& p) : m_p(p), m_cond(true) {}

  inline void noDelete() { m_cond = false; }

  inline T*& ptr() { return m_p; }

  ~DeleteObject() {
    if (m_cond) {
      GF_SAFE_DELETE(m_p);
    }
  }
};

template <typename T>
class DeleteArray {
 private:
  T*& m_p;
  bool m_cond;

 public:
  DeleteArray(T*& p) : m_p(p), m_cond(true) {}

  inline T operator[](int32_t index) { return m_p[index]; }

  inline void noDelete() { m_cond = false; }

  inline T*& ptr() { return m_p; }

  ~DeleteArray() {
    if (m_cond) {
      GF_SAFE_DELETE_ARRAY(m_p);
    }
  }
};
}

#endif  // #ifndef _GEMFIRE_AUTODELETE_HPP_
