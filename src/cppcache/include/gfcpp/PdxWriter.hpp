#ifndef __GEMFIRE_PDXWRITER_H__
#define __GEMFIRE_PDXWRITER_H__

/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*========================================================================
*/

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
#include "CacheableBuiltins.hpp"
#include "CacheableDate.hpp"

namespace gemfire {

class PdxWriter;
typedef SharedPtr<PdxWriter> PdxWriterPtr;

/**
 * A PdxWriter will be passed to PdxSerializable.toData
 * when it is serializing the domain class. The domain class needs to serialize
 * member
 * fields using this abstract class. This class is implemented by Native Client.
 */
class CPPCACHE_EXPORT PdxWriter : public SharedBase {
 public:
  /**
   * @brief constructors
   */
  PdxWriter() {}

  /**
   * @brief destructor
  */
  virtual ~PdxWriter() {}

  /**
   * Writes the named field with the given value to the serialized form.
   * The fields type is <code>char</code>.
   * <p>C++ char is mapped to Java char</p>.
   * @param fieldName The name of the field to write.
   * @param value The value of the field to write
   * @return this PdxWriter
   * @throws IllegalStateException if the named field has already been written
   * or fieldName is NULL or empty
   */
  virtual PdxWriterPtr writeChar(const char* fieldName, char value) = 0;

  /**
   * Writes the named field with the given value to the serialized form
   * The fields type is <code>wchar_t</code>
   * <p>C++ wchar_t is mapped to Java char</p>
   * @param fieldName The name of the field to write
   * @param value The value of the field to write
   * @return this PdxWriter
   * @throws IllegalStateException if the named field has already been written
   * or fieldName is NULL or empty
  */
  virtual PdxWriterPtr writeWideChar(const char* fieldName, wchar_t value) = 0;

  /**
   * Writes the named field with the given value to the serialized form.
   * The fields type is <code>bool</code>.
   * <p>C++ bool is mapped to Java boolean</p>
   * @param fieldName the name of the field to write
   * @param value the value of the field to write
   * @return this PdxWriter
   * @throws IllegalStateException if the named field has already been written
   * or fieldName is NULL or empty
   */
  virtual PdxWriterPtr writeBoolean(const char* fieldName, bool value) = 0;

  /**
   * Writes the named field with the given value to the serialized form.
   * The fields type is <code>int8_t</code>.
   * <p>C++ int8_t is mapped to Java byte</p>
   * @param fieldName the name of the field to write
   * @param value the value of the field to write
   * @return this PdxWriter
   * @throws IllegalStateException if the named field has already been written
   * or fieldName is NULL or empty
  */
  virtual PdxWriterPtr writeByte(const char* fieldName, int8_t value) = 0;

  /**
   * Writes the named field with the given value to the serialized form.
   * The fields type is <code>int16_t</code>.
   * <p>C++ int16_t is mapped to Java short</p>
   * @param fieldName the name of the field to write
   * @param value the value of the field to write
   * @return this PdxWriter
   * @throws IllegalStateException if the named field has already been written
   * or fieldName is NULL or empty
   */
  virtual PdxWriterPtr writeShort(const char* fieldName, int16_t value) = 0;

  /**
   * Writes the named field with the given value to the serialized form.
   * The fields type is <code>int32_t</code>.
   * <p>C++ int32_t is mapped to Java int</p>
   * @param fieldName the name of the field to write
   * @param value the value of the field to write
   * @return this PdxWriter
   * @throws IllegalStateException if the named field has already been written
   * or fieldName is NULL or empty
   */
  virtual PdxWriterPtr writeInt(const char* fieldName, int32_t value) = 0;

  /**
   * Writes the named field with the given value to the serialized form.
   * The fields type is <code>int64_t</code>.
   * <p>C++ int64_t is mapped to Java long</p>
   * @param fieldName the name of the field to write
   * @param value the value of the field to write
   * @return this PdxWriter
   * @throws IllegalStateException if the named field has already been written
   * or fieldName is NULL or empty
   */
  virtual PdxWriterPtr writeLong(const char* fieldName, int64_t value) = 0;

  /**
   * Writes the named field with the given value to the serialized form.
   * The fields type is <code>float</code>.
   * <p>C++ float is mapped to Java float</p>
   * @param fieldName the name of the field to write
   * @param value the value of the field to write
   * @return this PdxWriter
   * @throws IllegalStateException if the named field has already been written
   * or fieldName is NULL or empty
   */
  virtual PdxWriterPtr writeFloat(const char* fieldName, float value) = 0;

  /**
   * Writes the named field with the given value to the serialized form.
   * The fields type is <code>double</code>.
   * <p>C++ double is mapped to Java double</p>
   * @param fieldName the name of the field to write
   * @param value the value of the field to write
   * @return this PdxWriter
   * @throws IllegalStateException if the named field has already been written
   * or fieldName is NULL or empty
   */
  virtual PdxWriterPtr writeDouble(const char* fieldName, double value) = 0;

  /**
   * Writes the named field with the given value to the serialized form.
   * The fields type is <code>CacheableDatePtr</code>.
   * <p>C++ CacheableDatePtr is mapped to Java Date</p>
   * @param fieldName the name of the field to write
   * @param value the value of the field to write
   * @return this PdxWriter
   * @throws IllegalStateException if the named field has already been written
   * or fieldName is NULL or empty
   */
  virtual PdxWriterPtr writeDate(const char* fieldName,
                                 CacheableDatePtr date) = 0;

  /**
   * Writes the named field with the given value to the serialized form.
   * The fields type is <code>char*</code>.
   * <p>C++ char* is mapped to Java String</p>
   * @param fieldName the name of the field to write
   * @param value the value of the field to write
   * @return this PdxWriter
   * @throws IllegalStateException if the named field has already been written
   * or fieldName is NULL or empty
   */
  virtual PdxWriterPtr writeString(const char* fieldName,
                                   const char* value) = 0;

  /**
   * Writes the named field with the given value to the serialized form.
   * The fields type is <code>wchar_t*</code>.
   * <p>C++ wchar_t* is mapped to Java String</p>
   * @param fieldName the name of the field to write
   * @param value the value of the field to write
   * @return this PdxWriter
   * @throws IllegalStateException if the named field has already been written
   * or fieldName is NULL or empty
   */
  virtual PdxWriterPtr writeWideString(const char* fieldName,
                                       const wchar_t* value) = 0;

  /**
   * Writes the named field with the given value to the serialized form.
   * The fields type is <code>CacheablePtr</code>.
   * <p>C++ CacheablePtr is mapped to Java object.</p>
   * It is best to use one of the other writeXXX methods if your field type
   * will always be XXX. This method allows the field value to be anything
   * that is an instance of Object. This gives you more flexibility but more
   * space is used to store the serialized field.
   *
   * Note that some Java objects serialized with this method may not be
   * compatible with non-java languages.
   * @param fieldName the name of the field to write
   * @param value the value of the field to write
   * @return this PdxWriter
   * @throws IllegalStateException if the named field has already been written
   * or fieldName is NULL or empty.
   */
  virtual PdxWriterPtr writeObject(const char* fieldName,
                                   CacheablePtr value) = 0;

  /**
   * Writes the named field with the given value to the serialized form.
   * The fields type is <code>bool*</code>.
   * <p>C++ bool* is mapped to Java boolean[]</p>
   * @param fieldName the name of the field to write
   * @param array the value of the field to write
   * @param length the length of the array field to write
   * @return this PdxWriter
   * @throws IllegalStateException if the named field has already been written
   * or fieldName is NULL or empty.
   */
  virtual PdxWriterPtr writeBooleanArray(const char* fieldName, bool* array,
                                         int length) = 0;

  /**
   * Writes the named field with the given value to the serialized form.
   * The fields type is <code>wchar_t*</code>.
   * <p>C++ wchar_t* is mapped to Java char[].</p>
   * @param fieldName the name of the field to write
   * @param array the value of the field to write
   * @param length the length of the array field to write
   * @return this PdxWriter
   * @throws IllegalStateException if the named field has already been written
   * or fieldName is NULL or empty.
   */
  virtual PdxWriterPtr writeWideCharArray(const char* fieldName, wchar_t* array,
                                          int length) = 0;

  /**
   * Writes the named field with the given value to the serialized form.
   * The fields type is <code>char*</code>.
   * <p>C++ char* is mapped to Java char[].</p>
   * @param fieldName the name of the field to write
   * @param array the value of the field to write
   * @param length the length of the array field to write
   * @return this PdxWriter
   * @throws IllegalStateException if the named field has already been written
   * or fieldName is NULL or empty.
   */
  virtual PdxWriterPtr writeCharArray(const char* fieldName, char* array,
                                      int length) = 0;

  /**
   * Writes the named field with the given value to the serialized form.
   * The fields type is <code>int8_t*</code>.
   * <p>C++ int8_t* is mapped to Java byte[].</p>
   * @param fieldName the name of the field to write
   * @param array the value of the field to write
   * @param length the length of the array field to write
   * @return this PdxWriter
   * @throws IllegalStateException if the named field has already been written
   * or fieldName is NULL or empty.
   */
  virtual PdxWriterPtr writeByteArray(const char* fieldName, int8_t* array,
                                      int length) = 0;

  /**
   * Writes the named field with the given value to the serialized form.
   * The fields type is <code>int16_t*</code>.
   * <p>C++ int16_t* is mapped to Java short[].</p>
   * @param fieldName the name of the field to write
   * @param array the value of the field to write
   * @param length the length of the array field to write
   * @return this PdxWriter
   * @throws IllegalStateException if the named field has already been written
   * or fieldName is NULL or empty.
   */
  virtual PdxWriterPtr writeShortArray(const char* fieldName, int16_t* array,
                                       int length) = 0;

  /**
   * Writes the named field with the given value to the serialized form.
   * The fields type is <code>int32_t*</code>.
   * <p>C++ int32_t* is mapped to Java int[].</p>
   * @param fieldName the name of the field to write
   * @param array the value of the field to write
   * @param length the length of the array field to write
   * @return this PdxWriter
   * @throws IllegalStateException if the named field has already been written
   * or fieldName is NULL or empty.
   */
  virtual PdxWriterPtr writeIntArray(const char* fieldName, int32_t* array,
                                     int length) = 0;

  /**
   * Writes the named field with the given value to the serialized form.
   * The fields type is <code>int64_t*</code>.
   * <p>C++ int64_t* is mapped to Java long[].</p>
   * @param fieldName the name of the field to write
   * @param array the value of the field to write
   * @param length the length of the array field to write
   * @return this PdxWriter
   * @throws IllegalStateException if the named field has already been written
   * or fieldName is NULL or empty.
   */
  virtual PdxWriterPtr writeLongArray(const char* fieldName, int64_t* array,
                                      int length) = 0;

  /**
   * Writes the named field with the given value to the serialized form.
   * The fields type is <code>float*</code>.
   * <p>C++ float* is mapped to Java float[].</p>
   * @param fieldName the name of the field to write
   * @param array the value of the field to write
   * @param length the length of the array field to write
   * @return this PdxWriter
   * @throws IllegalStateException if the named field has already been written
   * or fieldName is NULL or empty.
   */
  virtual PdxWriterPtr writeFloatArray(const char* fieldName, float* array,
                                       int length) = 0;

  /**
   * Writes the named field with the given value to the serialized form.
   * The fields type is <code>double*</code>.
   * <p>C++ double* is mapped to Java double[].</p>
   * @param fieldName the name of the field to write
   * @param array the value of the field to write
   * @param length the length of the array field to write
   * @return this PdxWriter
   * @throws IllegalStateException if the named field has already been written
   * or fieldName is NULL or empty.
   */
  virtual PdxWriterPtr writeDoubleArray(const char* fieldName, double* array,
                                        int length) = 0;

  /**
   * Writes the named field with the given value to the serialized form.
   * The fields type is <code>char**</code>.
   * <p>C++ char** is mapped to Java String[].</p>
   * @param fieldName the name of the field to write
   * @param array the value of the field to write
   * @param length the length of the array field to write
   * @return this PdxWriter
   * @throws IllegalStateException if the named field has already been written
   * or fieldName is NULL or empty.
   */
  virtual PdxWriterPtr writeStringArray(const char* fieldName, char** array,
                                        int length) = 0;

  /**
   * Writes the named field with the given value to the serialized form.
   * The fields type is <code>wchar_t**</code>.
   * <p>C++ wchar_t** is mapped to Java String[].</p>
   * @param fieldName the name of the field to write
   * @param array the value of the field to write
   * @param length the length of the array field to write
   * @return this PdxWriter
   * @throws IllegalStateException if the named field has already been written
   * or fieldName is NULL or empty.
   */
  virtual PdxWriterPtr writeWideStringArray(const char* fieldName,
                                            wchar_t** array, int length) = 0;

  /**
   * Writes the named field with the given value to the serialized form.
   * The fields type is <code>CacheableObjectArrayPtr</code>.
   * C++ CacheableObjectArrayPtr is mapped to Java Object[].
   * For how each element of the array is a mapped to C++ see {@link
   * #writeObject}.
   * Note that this call may serialize elements that are not compatible with
   * non-java languages.
   * @param fieldName the name of the field to write
   * @param array the value of the field to write
   * @return this PdxWriter
   * @throws IllegalStateException if the named field has already been written
   * or fieldName is NULL or empty.
   */
  virtual PdxWriterPtr writeObjectArray(const char* fieldName,
                                        CacheableObjectArrayPtr array) = 0;

  /**
   * Writes the named field with the given value to the serialized form.
   * The fields type is <code>int8_t**</code>.
   * <p>C++ int8_t** is mapped to Java byte[][].</p>
   * @param fieldName the name of the field to write
   * @param array the value of the field to write
   * @param arrayLength the length of the actual byte array field holding
   * individual byte arrays to write
   * @param elementLength the length of the individual byte arrays to write
   * @return this PdxWriter
   * @throws IllegalStateException if the named field has already been written
   * or fieldName is NULL or empty.
   */
  virtual PdxWriterPtr writeArrayOfByteArrays(const char* fieldName,
                                              int8_t** array, int arrayLength,
                                              int* elementLength) = 0;

  /**
   * Indicate that the given field name should be included in hashCode and
   * equals checks
   * of this object on a server that is using {@link
   * CacheFactory#setPdxReadSerialized} or when a client executes a query on a
   * server.
   * The fields that are marked as identity fields are used to generate the
   * hashCode and
   * equals methods of {@link PdxInstance}. Because of this, the identity fields
   * should themselves
   * either be primitives, or implement hashCode and equals.
   *
   * If no fields are set as identity fields, then all fields will be used in
   * hashCode and equals
   * checks.
   *
   * The identity fields should make marked after they are written using a
   * write* method.
   *
   * @param fieldName the name of the field to mark as an identity field.
   * @returns this PdxWriterPtr
   * @throws IllegalStateException if the named field does not exist.
   */
  virtual PdxWriterPtr markIdentityField(const char* fieldName) = 0;

  /**
   * Writes the given unread fields to the serialized form.
   * The unread fields are obtained by calling {@link
   * PdxReader#readUnreadFields}.
   * <p>This method must be called first before any of the writeXXX methods is
   * called.
   * @param unread the object that was returned from {@link
   * PdxReader#readUnreadFields}.
   * @return this PdxWriter
   * @throws IllegalStateException if one of the writeXXX methods has already
   * been called.
   */
  virtual PdxWriterPtr writeUnreadFields(PdxUnreadFieldsPtr unread) = 0;
};
}
#endif /* PDXWRITER_HPP_ */
