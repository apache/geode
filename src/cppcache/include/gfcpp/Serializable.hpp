#ifndef __GEMFIRE_SERIALIZABLE_H__
#define __GEMFIRE_SERIALIZABLE_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

/**
 * @file
 */

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"

namespace gemfire {

class DataOutput;
class DataInput;

typedef void (*CliCallbackMethod)();

/** @brief signature of functions passed to registerType. Such functions
 * should return an empty instance of the type they represent. The instance
 * will typically be initialized immediately after creation by a call to
 * fromData().
 */
typedef Serializable* (*TypeFactoryMethod)();

typedef PdxSerializable* (*TypeFactoryMethodPdx)();
/**
 * @class Serializable Serializable.hpp
 * This abstract base class is the superclass of all user objects
 * in the cache that can be serialized.
 */

class CPPCACHE_EXPORT Serializable : public SharedBase {
 public:
  /**
   *@brief serialize this object
   **/
  virtual void toData(DataOutput& output) const = 0;

  /**
   *@brief deserialize this object, typical implementation should return
   * the 'this' pointer.
   **/
  virtual Serializable* fromData(DataInput& input) = 0;

  /**
   *@brief Return the classId of the instance being serialized.
   * This is used by deserialization to determine what instance
   * type to create and deserialize into.
   *
   * The classId must be unique within an application suite.
   * Using a negative value may result in undefined behavior.
   */
  virtual int32_t classId() const = 0;

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
   * @brief return the Data Serialization Fixed ID type.
   * This is used to determine what instance type to create and deserialize
   * into.
   *
   * Note that this should not be overridden by custom implementations
   * and is reserved only for builtin types.
   */
  virtual int8_t DSFID() const;

  /**
   *@brief return the size in bytes of the instance being serialized.
   * This is used to determine whether the cache is using up more
   * physical memory than it has been configured to use. The method can
   * return zero if the user does not require the ability to control
   * cache memory utilization.
   * Note that you must implement this only if you use the HeapLRU feature.
   */
  virtual uint32_t objectSize() const;

  /**
   * @brief register an instance factory method for a given type.
   * During registration the factory will be invoked to extract the typeId
   * to associate with this function.
   * @throws IllegalStateException if the typeId has already been registered,
   *         or there is an error in registering the type; check errno for
   *         more information in the latter case.
   */
  static void registerType(TypeFactoryMethod creationFunction);

  /**
   * @brief register an Pdx instance factory method for a given type.
   * @throws IllegalStateException if the typeName has already been registered,
   *         or there is an error in registering the type; check errno for
   *         more information in the latter case.
   */
  static void registerPdxType(TypeFactoryMethodPdx creationFunction);

  /**
   * Register the PDX serializer which can handle serialization for instances of
   * user domain classes.
   * @see PdxSerializer
   */
  static void registerPdxSerializer(PdxSerializerPtr pdxSerializer);

  /**
   * Display this object as 'string', which depends on the implementation in
   * the subclasses.
   * The default implementation renders the classname.
   *
   * The return value may be a temporary, so the caller has to ensure that
   * the SharedPtr count does not go down to zero by storing the result
   * in a variable or otherwise.
   */
  virtual CacheableStringPtr toString() const;

  /** Factory method that creates the Serializable object that matches the type
   * of value.
   * For customer defined derivations of Serializable, the method
   * gemfire::createValue
   * may be overloaded. For pointer types (e.g. char*) the method
   * gemfire::createValueArr may be overloaded.
   */
  template <class PRIM>
  inline static SerializablePtr create(const PRIM value);

  /**
   * @brief destructor
   */
  virtual ~Serializable() {}

 protected:
  /**
   * @brief constructors
   */
  Serializable() : SharedBase() {}

 private:
  // Never defined.
  Serializable(const Serializable& other);
  void operator=(const Serializable& other);
};

}  // namespace gemfire

#endif  // ifndef __GEMFIRE_SERIALIZABLE_H__
