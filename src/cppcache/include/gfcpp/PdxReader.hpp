#ifndef __GEMFIRE_PDXREADER_H__
#define __GEMFIRE_PDXREADER_H__

/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*========================================================================
*/

#include "CacheableBuiltins.hpp"
#include "PdxUnreadFields.hpp"

namespace gemfire {

class PdxReader;
typedef SharedPtr<PdxReader> PdxReaderPtr;

/**
 * A PdxReader will be passed to PdxSerializable.fromData or
 * during deserialization of a PDX. The domain class needs to deserialize field
 * members
 * using this abstract class. This class is implemented by Native Client.
 * Each readXXX call will return the field's value. If the serialized
 * PDX does not contain the named field then a default value will
 * be returned. Standard Java defaults are used. For Objects this is
 * null and for primitives it is 0 or 0.0.
 *
 * @note Implementations of PdxReader that are internal to the Native
 *       Client library may be returned to clients via instances of
 *       PdxReaderPtr. For those implementations, any
 *       non-<tt>NULL</tt>, non-empty strings returned from
 *       PdxReader::readString() or PdxReader::readWideString() must
 *       be freed with DataInput::freeUTFMemory(). Arrays returned
 *       from PdxReader::readStringArray() or
 *       PdxReader::readWideStringArray() must be freed with
 *       <tt>GF_SAFE_DELETE_ARRAY</tt> once their constituent strings
 *       have been freed with DataInput::freeUTFMemory().
 * @note Custom implementations of PdxReader are not subject
 *       to this restriction.
 */
class CPPCACHE_EXPORT PdxReader : public SharedBase {
 public:
  /**
   * @brief constructors
   */
  PdxReader() {}

  /**
   * @brief destructor
   */
  virtual ~PdxReader() {}

  /**
   * Read a char value from the <code>PdxReader</code>.
   * <p>C++ char is mapped to Java char</p>
   * @param fieldName name of the field to read.
   * @return value of type char.
   * @throws IllegalStateException if PdxReader doesn't has the named field.
   *
   * @see PdxReader#hasField
   */
  virtual char readChar(const char* fieldName) = 0;

  /**
   * Read a wide char value from the <code>PdxReader</code>.
   * <p>C++ wchar_t is mapped to Java char</p>
   * @param fieldName name of the field to read.
   * @return value of type wchar_t.
   * @throws IllegalStateException if PdxReader doesn't has the named field.
   *
   * @see PdxReader#hasField
   */
  virtual wchar_t readWideChar(const char* fieldName) = 0;

  /**
   * Read a bool value from the <code>PdxReader</code>.
   * <p>C++ bool is mapped to Java boolean</p>
   * @param fieldName name of the field to read
   * @return value of type bool.
   * @throws IllegalStateException if PdxReader doesn't has the named field.
   *
   * @see PdxReader#hasField
   */
  virtual bool readBoolean(const char* fieldName) = 0;

  /**
   * Read a int8_t value from the <code>PdxReader</code>.
   * <p>C++ int8_t is mapped to Java byte</p>
   * @param fieldName name of the field to read
   * @return value of type int8_t.
   * @throws IllegalStateException if PdxReader doesn't has the named field.
   *
   * @see PdxReader#hasField
   */
  virtual int8_t readByte(const char* fieldName) = 0;

  /**
   * Read a int16_t value from the <code>PdxReader</code>.
   * <p>C++ int16_t is mapped to Java short</p>
   * @param fieldName name of the field to read
   * @return value of type int16_t.
   * @throws IllegalStateException if PdxReader doesn't has the named field.
   *
   * @see PdxReader#hasField
   */
  virtual int16_t readShort(const char* fieldName) = 0;

  /**
   * Read a int32_t value from the <code>PdxReader</code>.
   * <p>C++ int32_t is mapped to Java int</p>
   * @param fieldName name of the field to read
   * @return value of type int32_t.
   * @throws IllegalStateException if PdxReader doesn't has the named field.
   *
   * @see PdxReader#hasField
   */
  virtual int32_t readInt(const char* fieldName) = 0;

  /**
   * Read a int64_t value from the <code>PdxReader</code>.
   * <p>C++ int64_t is mapped to Java long</p>
   * @param fieldName name of the field to read
   * @return value of type int64_t.
   * @throws IllegalStateException if PdxReader doesn't has the named field.
   *
   * @see PdxReader#hasField
   */
  virtual int64_t readLong(const char* fieldName) = 0;

  /**
   * Read a float value from the <code>PdxReader</code>.
   * <p>C++ float is mapped to Java float</p>
   * @param fieldName name of the field to read
   * @return value of type float.
   * @throws IllegalStateException if PdxReader doesn't has the named field.
   *
   * @see PdxReader#hasField
   */
  virtual float readFloat(const char* fieldName) = 0;

  /**
   * Read a double value from the <code>PdxReader</code>.
   * <p>C++ double is mapped to Java double</p>
   * @param fieldName name of the field to read
   * @return value of type double.
   * @throws IllegalStateException if PdxReader doesn't has the named field.
   *
   * @see PdxReader#hasField
   */
  virtual double readDouble(const char* fieldName) = 0;

  /**
   * Read a char* value from the <code>PdxReader</code>.
   * <p>C++ char* is mapped to Java String</p>
   * @param fieldName name of the field to read
   * @return value of type char*. Refer to the class description for
   *         how to free the return value.
   * @throws IllegalStateException if PdxReader doesn't has the named field.
   *
   * @see PdxReader#hasField
   */
  virtual char* readString(const char* fieldName) = 0;

  /**
   * Read a wchar_t* value from the <code>PdxReader</code>.
   * <p>C++ wchar_t* is mapped to Java String</p>
   * @param fieldName name of the field to read
   * @return value of type wchar_t*. Refer to the class description for
   *         how to free the return value.
   * @throws IllegalStateException if PdxReader doesn't has the named field.
   *
   * @see PdxReader#hasField
   */
  virtual wchar_t* readWideString(const char* fieldName) = 0;

  /**
   * Read a CacheablePtr value from the <code>PdxReader</code>.
   * <p>C++ CacheablePtr is mapped to Java object</p>
   * @param fieldName name of the field to read
   * @return value of type CacheablePtr.
   * @throws IllegalStateException if PdxReader doesn't has the named field.
   *
   * @see PdxReader#hasField
   */
  virtual CacheablePtr readObject(const char* fieldName) = 0;

  /**
   * Read a char* value from the <code>PdxReader</code> and sets array length.
   * <p>C++ char* is mapped to Java char[].</p>
   * @param fieldName name of the field to read
   * @param length length is set with number of wchar_t elements.
   * @return value of type char*.
   * @throws IllegalStateException if PdxReader doesn't has the named field.
   *
   * @see PdxReader#hasField
   */
  virtual char* readCharArray(const char* fieldName, int32_t& length) = 0;

  /**
   * Read a wchar_t* value from the <code>PdxReader</code> and sets array
   * length.
   * <p>C++ wchar_t* is mapped to Java char[].</p>
   * @param fieldName name of the field to read
   * @param length length is set with number of wchar_t elements.
   * @return value of type wchar_t*.
   * @throws IllegalStateException if PdxReader doesn't has the named field.
   *
   * @see PdxReader#hasField
   */
  virtual wchar_t* readWideCharArray(const char* fieldName,
                                     int32_t& length) = 0;

  /**
   * Read a bool* value from the <code>PdxReader</code> and sets array length.
   * <p>C++ bool* is mapped to Java boolean[]</p>
   * @param fieldName name of the field to read
   * @param length length is set with number of bool elements.
   * @throws IllegalStateException if PdxReader doesn't has the named field.
   *
   * @see PdxReader#hasField
   */
  virtual bool* readBooleanArray(const char* fieldName, int32_t& length) = 0;

  /**
   * Read a int8_t* value from the <code>PdxReader</code> and sets array length.
   * <p>C++ int8_t* is mapped to Java byte[].</p>
   * @param fieldName name of the field to read
   * @param length length is set with number of int8_t elements
   * @return value of type int8_t*.
   * @throws IllegalStateException if PdxReader doesn't has the named field.
   *
   * @see PdxReader#hasField
   */
  virtual int8_t* readByteArray(const char* fieldName, int32_t& length) = 0;

  /**
   * Read a int16_t* value from the <code>PdxReader</code> and sets array
   * length.
   * <p>C++ int16_t* is mapped to Java short[].</p>
   * @param fieldName name of the field to read
   * @param length length is set with number of int16_t elements
   * @return value of type int16_t*.
   * @throws IllegalStateException if PdxReader doesn't has the named field.
   *
   * @see PdxReader#hasField
   */
  virtual int16_t* readShortArray(const char* fieldName, int32_t& length) = 0;

  /**
   * Read a int32_t* value from the <code>PdxReader</code> and sets array
   * length.
   * <p>C++ int32_t* is mapped to Java int[].</p>
   * @param fieldName name of the field to read
   * @param length length is set with number of int32_t elements
   * @return value of type int32_t*.
   * @throws IllegalStateException if PdxReader doesn't has the named field.
   *
   * @see PdxReader#hasField
   */
  virtual int32_t* readIntArray(const char* fieldName, int32_t& length) = 0;

  /**
   * Read a int64_t* value from the <code>PdxReader</code> and sets array
   * length.
   * <p>C++ int64_t* is mapped to Java long[].</p>
   * @param fieldName name of the field to read
   * @param length length is set with number of int64_t elements
   * @return value of type int64_t*.
   * @throws IllegalStateException if PdxReader doesn't has the named field.
   *
   * @see PdxReader#hasField
   */
  virtual int64_t* readLongArray(const char* fieldName, int32_t& length) = 0;

  /**
   * Read a float* value from the <code>PdxReader</code> and sets array length.
   * <p>C++ float* is mapped to Java float[].</p>
   * @param fieldName name of the field to read
   * @param length length is set with number of float elements
   * @return value of type float*.
   * @throws IllegalStateException if PdxReader doesn't has the named field.
   *
   * @see PdxReader#hasField
   */
  virtual float* readFloatArray(const char* fieldName, int32_t& length) = 0;

  /**
   * Read a double* value from the <code>PdxReader</code> and sets array length.
   * <p>C++ double* is mapped to Java double[].</p>
   * @param fieldName name of the field to read
   * @param length length is set with number of double elements
   * @return value of type double*.
   * @throws IllegalStateException if PdxReader doesn't has the named field.
   *
   * @see PdxReader#hasField
   */
  virtual double* readDoubleArray(const char* fieldName, int32_t& length) = 0;

  /**
   * Read a char** value from the <code>PdxReader</code> and sets array length.
   * <p>C++ char** is mapped to Java String[].</p>
   * @param fieldName name of the field to read
   * @param length length is set with number of char* elements
   * @return value of type char**. Refer to the class description for
   *         how to free the return value.
   * @throws IllegalStateException if PdxReader doesn't has the named field.
   *
   * @see PdxReader#hasField
   */
  virtual char** readStringArray(const char* fieldName, int32_t& length) = 0;

  /**
   * Read a wchar_t** value from the <code>PdxReader</code> and sets array
   * length.
   * <p>C++ wchar_t** is mapped to Java String[].</p>
   * @param fieldName name of the field to read
   * @param length length is set with number of wchar_t* elements
   * @return value of type wchar_t**. Refer to the class description for
   *         how to free the return value.
   * @throws IllegalStateException if PdxReader doesn't has the named field.
   *
   * @see PdxReader#hasField
   */
  virtual wchar_t** readWideStringArray(const char* fieldName,
                                        int32_t& length) = 0;

  /**
   * Read a CacheableObjectArrayPtr value from the <code>PdxReader</code>.
   * C++ CacheableObjectArrayPtr is mapped to Java Object[].
   * @param fieldName name of the field to read
   * @return value of type CacheableObjectArrayPtr.
   * @throws IllegalStateException if PdxReader doesn't has the named field.
   *
   * @see PdxReader#hasField
   */
  virtual CacheableObjectArrayPtr readObjectArray(const char* fieldName) = 0;

  /**
   * Read a int8_t** value from the <code>PdxReader</code> and sets
   * ArrayOfByteArray's length and individual ByteArray's length.
   * <p>C++ int8_t** is mapped to Java byte[][].</p>
   * @param fieldName name of the field to read
   * @param arrayLength length is set with number of int8_t* elements
   * @param elementLength elementLength is set with the length value of
   * individual byte arrays.
   * @return value of type int8_t**.
   * @throws IllegalStateException if PdxReader doesn't has the named field.
   *
   * @see PdxReader#hasField
   */
  virtual int8_t** readArrayOfByteArrays(const char* fieldName,
                                         int32_t& arrayLength,
                                         int32_t** elementLength) = 0;

  /**
   * Read a CacheableDatePtr value from the <code>PdxReader</code>.
   * <p>C++ CacheableDatePtr is mapped to Java Date</p>
   * @param fieldName name of the field to read
   * @return value of type CacheableDatePtr.
   * @throws IllegalStateException if PdxReader doesn't has the named field.
   *
   * @see PdxReader#hasField
   */
  virtual CacheableDatePtr readDate(const char* fieldName) = 0;

  /**
   * Checks if the named field exists and returns the result.
   * This can be useful when writing code that handles more than one version of
   * a PDX class.
   * @param fieldname the name of the field to check
   * @return <code>true</code> if the named field exists; otherwise
   * <code>false</code>
   */
  virtual bool hasField(const char* fieldName) = 0;

  /**
   * Checks if the named field was {@link PdxWriter#markIdentityField}marked as
   * an identity field.
   * Note that if no fields have been marked then all the fields are used as
   * identity fields even though
   * this method will return <code>false</code> since none of them have been
   * <em>marked</em>.
   * @param fieldname the name of the field to check
   * @return <code>true</code> if the named field exists and was marked as an
   * identify field; otherwise <code>false</code>
   */
  virtual bool isIdentityField(const char* fieldName) = 0;

  /**
   * This method returns an object that represents all the unread fields which
   * must be
   * passed to {@link PdxWriter#writeUnreadFields} in the toData code.
   * <P>Note that if {@link CacheFactory#setPdxIgnoreUnreadFields}
   * is set to <code>true</code> then this method will always return an object
   * that has no unread fields.
   *
   * @return an object that represents the unread fields.
   */
  virtual PdxUnreadFieldsPtr readUnreadFields() = 0;
};
}
#endif /* PDXREADER_HPP_ */
