/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef PUTALL_PARTIALRESULT_SERVER_EXCEPTION
#define PUTALL_PARTIALRESULT_SERVER_EXCEPTION

#include <gfcpp/Serializable.hpp>
#include <gfcpp/CacheableString.hpp>
#include "VersionedCacheableObjectPartList.hpp"
#include "PutAllPartialResult.hpp"

namespace gemfire {

class PutAllPartialResultServerException;
typedef SharedPtr<PutAllPartialResultServerException>
    PutAllPartialResultServerExceptionPtr;

/**
 * @brief PutAllPartialResultServerException class is used to encapsulate
 *gemfire PutAllPartialResultServerException in case of PutAll execution.
 **/
class CPPCACHE_EXPORT PutAllPartialResultServerException : public Serializable {
  /**
   * @brief public methods
   */
 public:
  PutAllPartialResultServerException(PutAllPartialResultPtr result);

  PutAllPartialResultServerException();

  // Consolidate exceptions
  void consolidate(PutAllPartialResultServerExceptionPtr pre);

  void consolidate(PutAllPartialResultPtr otherResult);

  PutAllPartialResultPtr getResult();

  /**
   * Returns the key set in exception
   */
  VersionedCacheableObjectPartListPtr getSucceededKeysAndVersions();

  ExceptionPtr getFailure();

  // Returns there's failedKeys
  bool hasFailure();

  CacheableKeyPtr getFirstFailedKey();

  CacheableStringPtr getMessage();

  /**
  * @brief destructor
  */
  virtual ~PutAllPartialResultServerException() {}

  /**
  * @brief constructors
  */
  PutAllPartialResultServerException(CacheableStringPtr msg);

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
  *@brief return as CacheableStringPtr the Exception name returned from gemfire
  *sendException api.
  **/
  CacheableStringPtr getName() {
    const char* msg = "PutAllPartialResultServerException";
    CacheableStringPtr str = CacheableString::create(msg);
    return str;
  }

 private:
  // never implemented.
  PutAllPartialResultServerException(
      const PutAllPartialResultServerException& other);
  void operator=(const PutAllPartialResultServerException& other);

  CacheableStringPtr m_message;  // error message
  PutAllPartialResultPtr m_result;
};
}
#endif
