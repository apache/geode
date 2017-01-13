#ifndef __PDXINSTANCE_FACTORY_HPP_
#define __PDXINSTANCE_FACTORY_HPP_

/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*========================================================================
*/

#include "PdxInstance.hpp"
#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
#include "CacheableBuiltins.hpp"
#include "CacheableDate.hpp"
#include "CacheableObjectArray.hpp"

namespace gemfire {

/**
* PdxInstanceFactory gives you a way to create PdxInstances.
* Call the write methods to populate the field data and then call {@link
* #create}
* to produce an actual instance that contains the data.
* To create a factory call {@link Cache#createPdxInstanceFactory}
* A factory can only create a single instance. To create multiple instances
* create
* multiple factories or use {@link PdxInstance#createWriter} to create
* subsequent instances.
*/
class CPPCACHE_EXPORT PdxInstanceFactory : public SharedBase {
 public:
  /**
  * @brief destructor
  */
  virtual ~PdxInstanceFactory() {}

 protected:
  /**
  * @brief constructors
  */
  PdxInstanceFactory() {}

 private:
  // never implemented.
  PdxInstanceFactory(const PdxInstanceFactory& other);
  void operator=(const PdxInstanceFactory& other);

 public:
  /**
  * Create a {@link PdxInstance}. The instance
  * will contain any data written to this factory
  * using the write methods.
  * @return the created Pdxinstance
  * @throws IllegalStateException if called more than once
  */
  virtual PdxInstancePtr create() = 0;

  /**
  * Writes the named field with the given value to the serialized form.
  * The fields type is <code>wchar_t</code>.
  * <p>Java char is mapped to C++ wchar_t.</p>
  * @param fieldName the name of the field to write
  * @param value the value of the field to write
  * @return this PdxInstanceFactory
  * @throws IllegalStateException if the named field has already been written or
  * fieldName is NULL or empty.
  */
  virtual PdxInstanceFactoryPtr writeWideChar(const char* fieldName,
                                              wchar_t value) = 0;

  /**
  * Writes the named field with the given value to the serialized form.
  * The fields type is <code>char</code>.
  * <p>Java char is mapped to C++ char.</p>
  * @param fieldName the name of the field to write
  * @param value the value of the field to write
  * @return this PdxInstanceFactory
  * @throws IllegalStateException if the named field has already been written or
  * fieldName is NULL or empty.
  */
  virtual PdxInstanceFactoryPtr writeChar(const char* fieldName,
                                          char value) = 0;

  /**
  * Writes the named field with the given value to the serialized form.
  * The fields type is <code>bool</code>.
  * <p>Java boolean is mapped to C++ bool.</p>
  * @param fieldName the name of the field to write
  * @param value the value of the field to write
  * @return this PdxInstanceFactory
  * @throws IllegalStateException if the named field has already been written or
  * fieldName is NULL or empty.
  */
  virtual PdxInstanceFactoryPtr writeBoolean(const char* fieldName,
                                             bool value) = 0;

  /**
  * Writes the named field with the given value to the serialized form.
  * The fields type is <code>int8_t</code>.
  * <p>Java byte is mapped to C++ int8_t</p>
  * @param fieldName the name of the field to write
  * @param value the value of the field to write
  * @return this PdxInstanceFactory
  * @throws IllegalStateException if the named field has already been written or
  * fieldName is NULL or empty.
  */
  virtual PdxInstanceFactoryPtr writeByte(const char* fieldName,
                                          int8_t value) = 0;

  /**
  * Writes the named field with the given value to the serialized form.
  * The fields type is <code>int16_t</code>.
  * <p>Java short is mapped to C++ int16_t.</p>
  * @param fieldName the name of the field to write
  * @param value the value of the field to write
  * @return this PdxInstanceFactory
  * @throws IllegalStateException if the named field has already been written or
  * fieldName is NULL or empty.
  */
  virtual PdxInstanceFactoryPtr writeShort(const char* fieldName,
                                           int16_t value) = 0;

  /**
  * Writes the named field with the given value to the serialized form.
  * The fields type is <code>int32_t</code>.
  * <p>Java int is mapped to C++ int32_t.</p>
  * @param fieldName the name of the field to write
  * @param value the value of the field to write
  * @return this PdxInstanceFactory
  * @throws IllegalStateException if the named field has already been written or
  * fieldName is NULL or empty.
  */
  virtual PdxInstanceFactoryPtr writeInt(const char* fieldName,
                                         int32_t value) = 0;

  /**
  * Writes the named field with the given value to the serialized form.
  * The fields type is <code>int64_t</code>.
  * <p>Java long is mapped to C++ int64_t.</p>
  * @param fieldName the name of the field to write
  * @param value the value of the field to write
  * @return this PdxInstanceFactory
  * @throws IllegalStateException if the named field has already been written or
  * fieldName is NULL or empty.
  */
  virtual PdxInstanceFactoryPtr writeLong(const char* fieldName,
                                          int64_t value) = 0;

  /**
  * Writes the named field with the given value to the serialized form.
  * The fields type is <code>float</code>.
  * <p>Java float is mapped to C++ float.</p>
  * @param fieldName the name of the field to write
  * @param value the value of the field to write
  * @return this PdxInstanceFactory
  * @throws IllegalStateException if the named field has already been written or
  * fieldName is NULL or empty.
  */
  virtual PdxInstanceFactoryPtr writeFloat(const char* fieldName,
                                           float value) = 0;

  /**
  * Writes the named field with the given value to the serialized form.
  * The fields type is <code>double</code>.
  * <p>Java double is mapped to C++ double.</p>
  * @param fieldName the name of the field to write
  * @param value the value of the field to write
  * @return this PdxInstanceFactory
  * @throws IllegalStateException if the named field has already been written or
  * fieldName is NULL or empty.
  */
  virtual PdxInstanceFactoryPtr writeDouble(const char* fieldName,
                                            double value) = 0;

  /**
  * Writes the named field with the given value to the serialized form.
  * The fields type is <code>CacheableDatePtr</code>.
  * <p>Java Date is mapped to C++ CacheableDatePtr.</p>
  * @param fieldName the name of the field to write
  * @param value the value of the field to write
  * @return this PdxInstanceFactory
  * @throws IllegalStateException if the named field has already been written or
  * fieldName is NULL or empty.
  */
  virtual PdxInstanceFactoryPtr writeDate(const char* fieldName,
                                          CacheableDatePtr value) = 0;

  /**
  * Writes the named field with the given value to the serialized form.
  * The fields type is <code>wchar_t*</code>.
  * <p>Java String is mapped to C++ wchar_t*.</p>
  * @param fieldName the name of the field to write
  * @param value the value of the field to write
  * @return this PdxInstanceFactory
  * @throws IllegalStateException if the named field has already been written or
  * fieldName is NULL or empty.
  */
  virtual PdxInstanceFactoryPtr writeWideString(const char* fieldName,
                                                const wchar_t* value) = 0;

  /**
  * Writes the named field with the given value to the serialized form.
  * The fields type is <code>char*</code>.
  * <p>Java String is mapped to C++ char*.</p>
  * @param fieldName the name of the field to write
  * @param value the value of the field to write
  * @return this PdxInstanceFactory
  * @throws IllegalStateException if the named field has already been written or
  * fieldName is NULL or empty.
  */
  virtual PdxInstanceFactoryPtr writeString(const char* fieldName,
                                            const char* value) = 0;

  /**
  * Writes the named field with the given value to the serialized form.
  * The fields type is <code>CacheablePtr</code>.
  * <p>Java object is mapped to C++ CacheablePtr.</p>
  * It is best to use one of the other writeXXX methods if your field type
  * will always be XXX. This method allows the field value to be anything
  * that is an instance of Object. This gives you more flexibility but more
  * space is used to store the serialized field.
  *
  * Note that some Java objects serialized with this method may not be
  * compatible with non-java languages.
  * @param fieldName the name of the field to write
  * @param value the value of the field to write
  * @return this PdxInstanceFactory
  * @throws IllegalStateException if the named field has already been written or
  * fieldName is NULL or empty.
  */
  virtual PdxInstanceFactoryPtr writeObject(const char* fieldName,
                                            CacheablePtr value) = 0;

  /**
  * Writes the named field with the given value to the serialized form.
  * The fields type is <code>bool*</code>.
  * <p>Java boolean[] is mapped to C++ bool*.</p>
  * @param fieldName the name of the field to write
  * @param value the value of the field to write
  * @param length the length of the array field to write
  * @return this PdxInstanceFactory
  * @throws IllegalStateException if the named field has already been written or
  * fieldName is NULL or empty.
  */
  virtual PdxInstanceFactoryPtr writeBooleanArray(const char* fieldName,
                                                  bool* value,
                                                  int32_t length) = 0;

  /**
  * Writes the named field with the given value to the serialized form.
  * The fields type is <code>wchar_t*</code>.
  * <p>Java char[] is mapped to C++ wchar_t*.</p>
  * @param fieldName the name of the field to write
  * @param value the value of the field to write
  * @param length the length of the array field to write
  * @return this PdxInstanceFactory
  * @throws IllegalStateException if the named field has already been written or
  * fieldName is NULL or empty.
  */
  virtual PdxInstanceFactoryPtr writeWideCharArray(const char* fieldName,
                                                   wchar_t* value,
                                                   int32_t length) = 0;

  /**
  * Writes the named field with the given value to the serialized form.
  * The fields type is <code>char*</code>.
  * <p>Java char[] is mapped to C++ char*.</p>
  * @param fieldName the name of the field to write
  * @param value the value of the field to write
  * @param length the length of the array field to write
  * @return this PdxInstanceFactory
  * @throws IllegalStateException if the named field has already been written or
  * fieldName is NULL or empty.
  */
  virtual PdxInstanceFactoryPtr writeCharArray(const char* fieldName,
                                               char* value, int32_t length) = 0;

  /**
  * Writes the named field with the given value to the serialized form.
  * The fields type is <code>int8_t*</code>.
  * <p>Java byte[] is mapped to C++ int8_t*.</p>
  * @param fieldName the name of the field to write
  * @param value the value of the field to write
  * @param length the length of the array field to write
  * @return this PdxInstanceFactory
  * @throws IllegalStateException if the named field has already been written or
  * fieldName is NULL or empty.
  */
  virtual PdxInstanceFactoryPtr writeByteArray(const char* fieldName,
                                               int8_t* value,
                                               int32_t length) = 0;

  /**
  * Writes the named field with the given value to the serialized form.
  * The fields type is <code>int16_t*</code>.
  * <p>Java short[] is mapped to C++ int16_t*.</p>
  * @param fieldName the name of the field to write
  * @param value the value of the field to write
  * @param length the length of the array field to write
  * @return this PdxInstanceFactory
  * @throws IllegalStateException if the named field has already been written or
  * fieldName is NULL or empty.
  */
  virtual PdxInstanceFactoryPtr writeShortArray(const char* fieldName,
                                                int16_t* value,
                                                int32_t length) = 0;

  /**
  * Writes the named field with the given value to the serialized form.
  * The fields type is <code>int32_t*</code>.
  * <p>Java int[] is mapped to C++ int32_t*.</p>
  * @param fieldName the name of the field to write
  * @param value the value of the field to write
  * @param length the length of the array field to write
  * @return this PdxInstanceFactory
  * @throws IllegalStateException if the named field has already been written or
  * fieldName is NULL or empty.
  */
  virtual PdxInstanceFactoryPtr writeIntArray(const char* fieldName,
                                              int32_t* value,
                                              int32_t length) = 0;

  /**
  * Writes the named field with the given value to the serialized form.
  * The fields type is <code>int64_t*</code>.
  * <p>Java long[] is mapped to C++ int64_t*.</p>
  * @param fieldName the name of the field to write
  * @param value the value of the field to write
  * @param length the length of the array field to write
  * @return this PdxInstanceFactory
  * @throws IllegalStateException if the named field has already been written or
  * fieldName is NULL or empty.
  */
  virtual PdxInstanceFactoryPtr writeLongArray(const char* fieldName,
                                               int64_t* value,
                                               int32_t length) = 0;

  /**
  * Writes the named field with the given value to the serialized form.
  * The fields type is <code>float*</code>.
  * <p>Java float[] is mapped to C++ float*.</p>
  * @param fieldName the name of the field to write
  * @param value the value of the field to write
  * @param length the length of the array field to write
  * @return this PdxInstanceFactory
  * @throws IllegalStateException if the named field has already been written or
  * fieldName is NULL or empty.
  */
  virtual PdxInstanceFactoryPtr writeFloatArray(const char* fieldName,
                                                float* value,
                                                int32_t length) = 0;

  /**
  * Writes the named field with the given value to the serialized form.
  * The fields type is <code>double*</code>.
  * <p>Java double[] is mapped to C++ double*.</p>
  * @param fieldName the name of the field to write
  * @param value the value of the field to write
  * @param length the length of the array field to write
  * @return this PdxInstanceFactory
  * @throws IllegalStateException if the named field has already been written or
  * fieldName is NULL or empty.
  */
  virtual PdxInstanceFactoryPtr writeDoubleArray(const char* fieldName,
                                                 double* value,
                                                 int32_t length) = 0;

  /**
  * Writes the named field with the given value to the serialized form.
  * The fields type is <code>char**</code>.
  * <p>Java String[] is mapped to C++ char**.</p>
  * @param fieldName the name of the field to write
  * @param value the value of the field to write
  * @param length the length of the array field to write
  * @return this PdxInstanceFactory
  * @throws IllegalStateException if the named field has already been written or
  * fieldName is NULL or empty.
  */
  virtual PdxInstanceFactoryPtr writeStringArray(const char* fieldName,
                                                 char** value,
                                                 int32_t length) = 0;

  /**
  * Writes the named field with the given value to the serialized form.
  * The fields type is <code>wchar_t**</code>.
  * <p>Java String[] is mapped to C++ wchar_t**.</p>
  * @param fieldName the name of the field to write
  * @param value the value of the field to write
  * @param length the length of the array field to write
  * @return this PdxInstanceFactory
  * @throws IllegalStateException if the named field has already been written or
  * fieldName is NULL or empty.
  */
  virtual PdxInstanceFactoryPtr writeWideStringArray(const char* fieldName,
                                                     wchar_t** value,
                                                     int32_t length) = 0;

  /**
  * Writes the named field with the given value to the serialized form.
  * The fields type is <code>CacheableObjectArrayPtr</code>.
  * Java Object[] is mapped to C++ CacheableObjectArrayPtr.
  * For how each element of the array is a mapped to C++ see {@link
  * #writeObject}.
  * Note that this call may serialize elements that are not compatible with
  * non-java languages.
  * @param fieldName the name of the field to write
  * @param value the value of the field to write
  * @return this PdxInstanceFactory
  * @throws IllegalStateException if the named field has already been written or
  * fieldName is NULL or empty.
  */
  virtual PdxInstanceFactoryPtr writeObjectArray(
      const char* fieldName, CacheableObjectArrayPtr value) = 0;

  /**
  * Writes the named field with the given value to the serialized form.
  * The fields type is <code>int8_t**</code>.
  * <p>Java byte[][] is mapped to C++ int8_t**.</p>
  * @param fieldName the name of the field to write
  * @param value the value of the field to write
  * @param arrayLength the length of the actual byte array field holding
  * individual byte arrays to write
  * @param elementLength the length of the individual byte arrays to write
  * @return this PdxInstanceFactory
  * @throws IllegalStateException if the named field has already been written or
  * fieldName is NULL or empty.
  */
  virtual PdxInstanceFactoryPtr writeArrayOfByteArrays(
      const char* fieldName, int8_t** value, int32_t arrayLength,
      int32_t* elementLength) = 0;

  /**
  * Indicate that the named field should be included in hashCode and equals
  * (operator==()) checks
  * of this object on a server that is accessing {@link PdxInstance}
  * or when a client executes a query on a server.
  *
  * The fields that are marked as identity fields are used to generate the
  * hashCode and
  * equals (operator==()) methods of {@link PdxInstance}. Because of this, the
  * identity fields should themselves
  * either be primitives, or implement hashCode and equals (operator==()).
  *
  * If no fields are set as identity fields, then all fields will be used in
  * hashCode and equals (operator==())
  * checks.
  *
  * The identity fields should make marked after they are written using a write*
  * method.
  *
  * @param fieldName the name of the field to mark as an identity field.
  * @return this PdxInstanceFactory
  * @throws IllegalStateException if the named field does not exist.
  */
  virtual PdxInstanceFactoryPtr markIdentityField(const char* fieldName) = 0;
};
}

#endif /* __PDXINSTANCE_FACTORY_HPP_ */
