#ifndef PDXSERIALIZER_HPP_
#define PDXSERIALIZER_HPP_

/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*========================================================================
*/

#include "Serializable.hpp"
#include "PdxReader.hpp"
#include "PdxWriter.hpp"
namespace gemfire {

/**
 * Function pointer type which takes a void pointer to an instance of a user
 * object to delete and class name.
 */
typedef void (*UserDeallocator)(void*, const char*);

/**
 * Function pointer type which takes a void pointer to an instance of a user
 * object and class name to return the size of the user object.
 */
typedef uint32_t (*UserObjectSizer)(void*, const char*);

class CPPCACHE_EXPORT PdxSerializer : public SharedBase {
  /**
   * The PdxSerializer class allows domain classes to be
   * serialized and deserialized as PDXs without modification
   * of the domain class.
   * A domain class should register function {@link
   * Serializable::registerPdxSerializer} to create new
   * instance of type for de-serilization.
   */

 public:
  PdxSerializer() {}

  virtual ~PdxSerializer() {}

  /**
   * Deserialize this object.
   *
   * @param className the class name whose object need to de-serialize
   * @param pr the PdxReader stream to use for reading the object data
   */
  virtual void* fromData(const char* className, PdxReaderPtr pr) = 0;

  /**
   * Serializes this object in gemfire PDX format.
   * @param userObject the object which need to serialize
   * @param pw the PdxWriter object to use for serializing the object
   */
  virtual bool toData(void* userObject, const char* className,
                      PdxWriterPtr pw) = 0;

  /**
   * Get the function pointer to the user deallocator
   * @param className to help select a deallocator for the correct class
   */
  virtual UserDeallocator getDeallocator(const char* className) = 0;

  /**
   * Get the function pointer to the user function that returns the size of an
   * instance of a user domain object
   * @param className to help select an object sizer for the correct class
   */
  virtual UserObjectSizer getObjectSizer(const char* className) {
    return NULL;
  };
};

} /* namespace gemfire */
#endif /* PDXSERIALIZER_HPP_ */
