/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/**
  * @file    FwkException.hpp
  * @since   1.0
  * @version 1.0
  * @see
  *
  */

// ----------------------------------------------------------------------------

#ifndef __FWKEXCEPTION_HPP__
#define __FWKEXCEPTION_HPP__

#include <string>

// ----------------------------------------------------------------------------

namespace gemfire {
namespace testframework {
// ----------------------------------------------------------------------------

/** @class FwkException
  * @brief Framework exception handler
  */
class FwkException {
 public:
  /** @brief exception message to handle */
  inline FwkException(const std::string& sMessage) : m_sMessage(sMessage) {}

  /** @brief exception message to handle */
  inline FwkException(const char* pszMessage) : m_sMessage(pszMessage) {}

  /** @brief get message */
  inline const char* getMessage() const { return m_sMessage.c_str(); }

  /** @brief get message */
  inline const char* what() const { return m_sMessage.c_str(); }

 private:
  std::string m_sMessage;
};

// ----------------------------------------------------------------------------

}  // namespace  testframework
}  // namepace gemfire

#endif  // __FWKEXCEPTION_HPP__
