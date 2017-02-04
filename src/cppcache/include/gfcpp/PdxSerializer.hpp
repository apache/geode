#pragma once

#ifndef GEODE_GFCPP_PDXSERIALIZER_H_
#define GEODE_GFCPP_PDXSERIALIZER_H_


/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "Serializable.hpp"
#include "PdxReader.hpp"
#include "PdxWriter.hpp"
namespace apache {
namespace geode {
namespace client {

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
   * Serializes this object in geode PDX format.
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
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // GEODE_GFCPP_PDXSERIALIZER_H_
