/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef USER_FUNCTION_EXECUTION_EXCEPTION
#define USER_FUNCTION_EXECUTION_EXCEPTION

#include "Serializable.hpp"
#include "CacheableString.hpp"

namespace gemfire {
class UserFunctionExecutionException;
typedef SharedPtr<UserFunctionExecutionException>
    UserFunctionExecutionExceptionPtr;

/**
* @brief UserFunctionExecutionException class is used to encapsulate gemfire
*sendException in case of Function execution.
**/
class UserFunctionExecutionException : public Serializable {
  /**
  * @brief public methods
  */

 public:
  /**
  * @brief destructor
  */
  virtual ~UserFunctionExecutionException() {}

  /**
  * @brief constructors
  */
  UserFunctionExecutionException(CacheableStringPtr msg);

  /**
  *@brief serialize this object
  * @throws IllegalStateException If this api is called from User code.
  **/
  virtual void toData(DataOutput& output) const;

  /**
  *@brief deserialize this object, typical implementation should return
  * the 'this' pointer.
  * @throws IllegalStateException If this api is called from User code.
  **/
  virtual Serializable* fromData(DataInput& input);

  /**
  *@brief Return the classId of the instance being serialized.
  * This is used by deserialization to determine what instance
  * type to create and deserialize into.
  *
  * The classId must be unique within an application suite.
  * Using a negative value may result in undefined behavior.
  * @throws IllegalStateException If this api is called from User code.
  */
  virtual int32_t classId() const;

  /**
   *@brief return the size in bytes of the instance being serialized.
   * This is used to determine whether the cache is using up more
   * physical memory than it has been configured to use. The method can
   * return zero if the user does not require the ability to control
   * cache memory utilization.
   * Note that you must implement this only if you use the HeapLRU feature.
   * @throws IllegalStateException If this api is called from User code.
 */
  virtual uint32_t objectSize() const;

  /**
   *@brief return the typeId byte of the instance being serialized.
   * This is used by deserialization to determine what instance
   * type to create and deserialize into.
   *
   * Note that this should not be overridden by custom implementations
   * and is reserved only for builtin types.
   */
  virtual int8_t typeId() const;

  /**
  *@brief return as CacheableStringPtr the Exception message returned from
  *gemfire sendException api.
  **/
  CacheableStringPtr getMessage() { return m_message; }

  /**
  *@brief return as CacheableStringPtr the Exception name returned from gemfire
  *sendException api.
  **/
  CacheableStringPtr getName() {
    const char* msg = "UserFunctionExecutionException";
    CacheableStringPtr str = CacheableString::create(msg);
    return str;
  }

 private:
  // never implemented.
  UserFunctionExecutionException(const UserFunctionExecutionException& other);
  void operator=(const UserFunctionExecutionException& other);

  CacheableStringPtr m_message;  // error message
};
}
#endif
