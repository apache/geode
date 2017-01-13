#ifndef __PDXINSTANCE_IMPL_HPP_
#define __PDXINSTANCE_IMPL_HPP_

/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*========================================================================
*/

#include <gfcpp/PdxInstance.hpp>
#include <gfcpp/WritablePdxInstance.hpp>
#include <gfcpp/PdxSerializable.hpp>
#include "PdxType.hpp"
#include "PdxLocalWriter.hpp"
#include <gfcpp/PdxFieldTypes.hpp>
#include <vector>
#include <map>

namespace gemfire {

typedef std::map<std::string, CacheablePtr> FieldVsValues;

class CPPCACHE_EXPORT PdxInstanceImpl : public WritablePdxInstance {
 public:
  /**
  * @brief destructor
  */
  virtual ~PdxInstanceImpl();

  /**
  * Deserializes and returns the domain object that this instance represents.
  * For deserialization C++ Native Client requires the domain class to be
  * registered.
  * @return the deserialized domain object.
  *
  * @see Serializable::registerPdxType
  */
  virtual PdxSerializablePtr getObject();

  /**
  * Checks if the named field exists and returns the result.
  * This can be useful when writing code that handles more than one version of
  * a PDX class.
  * @param fieldname the name of the field to check
  * @return <code>true</code> if the named field exists; otherwise
  * <code>false</code>
  */
  virtual bool hasField(const char* fieldname);

  /**
  * Reads the named field and set its value in bool type out param.
  * bool type is corresponding to java boolean type.
  * @param fieldname name of the field to read
  * @param value value of the field to be set with bool type.
  * @throws IllegalStateException if PdxInstance doesn't has the named field.
  *
  * @see PdxInstance#hasField
  */
  virtual void getField(const char* fieldname, bool& value) const;

  /**
  * Reads the named field and set its value in signed char type out param.
  * signed char type is corresponding to java byte type.
  * For C++ on Windows and Linux, signed char type is corresponding to int8_t
  * type.
  * However C++ users on Solaris should always use this api after casting int8_t
  * to signed char.
  * @param fieldname name of the field to read
  * @param value value of the field to be set with signed char type.
  * @throws IllegalStateException if PdxInstance doesn't has the named field.
  *
  * @see PdxInstance#hasField
  */
  virtual void getField(const char* fieldname, signed char& value) const;

  /**
  * Reads the named field and set its value in unsigned char type out param.
  * unsigned char type is corresponding to java byte type.
  * For C++ on Windows and Linux, unsigned char type is corresponding to int8_t
  * type.
  * However C++ users on Solaris should always use this api after casting int8_t
  * to unsigned char.
  * @param fieldname name of the field to read
  * @param value value of the field to be set with unsigned char type.
  * @throws IllegalStateException if PdxInstance doesn't has the named field.
  *
  * @see PdxInstance#hasField
  */
  virtual void getField(const char* fieldname, unsigned char& value) const;

  /**
  * Reads the named field and set its value in int16_t type out param.
  * int16_t type is corresponding to java short type.
  * @param fieldname name of the field to read
  * @param value value of the field to be set with int16_t type.
  * @throws IllegalStateException if PdxInstance doesn't has the named field.
  *
  * @see PdxInstance#hasField
  */
  virtual void getField(const char* fieldname, int16_t& value) const;

  /**
  * Reads the named field and set its value in int32_t type out param.
  * int32_t type is corresponding to java int type.
  * @param fieldname name of the field to read
  * @param value value of the field to be set with int32_t type.
  * @throws IllegalStateException if PdxInstance doesn't has the named field.
  */
  virtual void getField(const char* fieldname, int32_t& value) const;

  /**
  * Reads the named field and set its value in int64_t type out param.
  * int64_t type is corresponding to java long type.
  * @param fieldname name of the field to read
  * @param value value of the field to be set with int64_t type.
  * @throws IllegalStateException if PdxInstance doesn't has the named field.
  *
  * @see PdxInstance#hasField
  */
  virtual void getField(const char* fieldname, int64_t& value) const;

  /**
  * Reads the named field and set its value in float type out param.
  * float type is corresponding to java float type.
  * @param fieldname name of the field to read
  * @param value value of the field to be set with float type.
  * @throws IllegalStateException if PdxInstance doesn't has the named field.
  *
  * @see PdxInstance#hasField
  */
  virtual void getField(const char* fieldname, float& value) const;

  /**
  * Reads the named field and set its value in double type out param.
  * double type is corresponding to java double type.
  * @param fieldname name of the field to read
  * @param value value of the field to be set with double type.
  * @throws IllegalStateException if PdxInstance doesn't has the named field.
  *
  * @see PdxInstance#hasField
  */
  virtual void getField(const char* fieldname, double& value) const;

  /**
  * Reads the named field and set its value in wchar_t type out param.
  * wchar_t type is corresponding to java char type.
  * @param fieldname name of the field to read
  * @param value value of the field to be set with wchar_t type.
  * @throws IllegalStateException if PdxInstance doesn't has the named field.
  *
  * @see PdxInstance#hasField
  */
  virtual void getField(const char* fieldName, wchar_t& value) const;

  /**
  * Reads the named field and set its value in char type out param.
  * char type is corresponding to java char type.
  * @param fieldname name of the field to read
  * @param value value of the field to be set with char type.
  * @throws IllegalStateException if PdxInstance doesn't has the named field.
  *
  * @see PdxInstance#hasField
  */
  virtual void getField(const char* fieldName, char& value) const;

  /**
  * Reads the named field and set its value in bool array type out param.
  * bool* type is corresponding to java boolean[] type.
  * @param fieldname name of the field to read
  * @param value value of the field to be set with bool array type.
  * @param length length is set with number of bool elements.
  * @throws IllegalStateException if PdxInstance doesn't has the named field.
  *
  * @see PdxInstance#hasField
  */
  virtual void getField(const char* fieldname, bool** value,
                        int32_t& length) const;

  /**
  * Reads the named field and set its value in signed char array type out param.
  * signed char* type is corresponding to java byte[] type.
  * For C++ on Windows and Linux, signed char* type is corresponding to int8_t*
  * type.
  * However C++ users on Solaris should always use this api after casting
  * int8_t* to signed char*.
  * @param fieldname name of the field to read
  * @param value value of the field to be set with signed char array type.
  * @param length length is set with number of signed char elements.
  * @throws IllegalStateException if PdxInstance doesn't has the named field.
  *
  * @see PdxInstance#hasField
  */
  virtual void getField(const char* fieldname, signed char** value,
                        int32_t& length) const;

  /**
  * Reads the named field and set its value in unsigned char array type out
  * param.
  * unsigned char* type is corresponding to java byte[] type.
  * For C++ on Windows and Linux, unsigned char* type is corresponding to
  * int8_t* type.
  * However C++ users on Solaris should always use this api after casting
  * int8_t* to unsigned char*.
  * @param fieldname name of the field to read
  * @param value value of the field to be set with unsigned char array type.
  * @param length length is set with number of unsigned char elements.
  * @throws IllegalStateException if PdxInstance doesn't has the named field.
  *
  * @see PdxInstance#hasField
  */
  virtual void getField(const char* fieldname, unsigned char** value,
                        int32_t& length) const;

  /**
  * Reads the named field and set its value in int16_t array type out param.
  * int16_t* type is corresponding to java short[] type.
  * @param fieldname name of the field to read
  * @param value value of the field to be set with int16_t array type.
  * @param length length is set with number of int16_t elements.
  * @throws IllegalStateException if PdxInstance doesn't has the named field.
  *
  * @see PdxInstance#hasField
  */
  virtual void getField(const char* fieldname, int16_t** value,
                        int32_t& length) const;

  /**
  * Reads the named field and set its value in int32_t array type out param.
  * int32_t* type is corresponding to java int[] type.
  * @param fieldname name of the field to read
  * @param value value of the field to be set with int32_t array type.
  * @param length length is set with number of int32_t elements.
  * @throws IllegalStateException if PdxInstance doesn't has the named field.
  *
  * @see PdxInstance#hasField
  */
  virtual void getField(const char* fieldname, int32_t** value,
                        int32_t& length) const;

  /**
  * Reads the named field and set its value in int64_t array type out param.
  * int64_t* type is corresponding to java long[] type.
  * @param fieldname name of the field to read
  * @param value value of the field to be set with int64_t array type.
  * @param length length is set with number of int64_t elements.
  * @throws IllegalStateException if PdxInstance doesn't has the named field.
  *
  * @see PdxInstance#hasField
  */
  virtual void getField(const char* fieldname, int64_t** value,
                        int32_t& length) const;

  /**
  * Reads the named field and set its value in float array type out param.
  * float* type is corresponding to java float[] type.
  * @param fieldname name of the field to read
  * @param value value of the field to be set with float array type.
  * @param length length is set with number of float elements.
  * @throws IllegalStateException if PdxInstance doesn't has the named field.
  *
  * @see PdxInstance#hasField
  */
  virtual void getField(const char* fieldname, float** value,
                        int32_t& length) const;

  /**
  * Reads the named field and set its value in double array type out param.
  * double* type is corresponding to java double[] type.
  * @param fieldname name of the field to read
  * @param value value of the field to be set with double array type.
  * @param length length is set with number of double elements.
  * @throws IllegalStateException if PdxInstance doesn't has the named field.
  *
  * @see PdxInstance#hasField
  */
  virtual void getField(const char* fieldname, double** value,
                        int32_t& length) const;

  // charArray
  /**
  * Reads the named field and set its value in wchar_t array type out param.
  * wchar_t* type is corresponding to java String type.
  * @param fieldname name of the field to read
  * @param value value of the field to be set with wchar_t array type.
  * @param length length is set with number of wchar_t* elements.
  * @throws IllegalStateException if PdxInstance doesn't has the named field.
  *
  * @see PdxInstance#hasField
  */
  virtual void getField(const char* fieldName, wchar_t** value,
                        int32_t& length) const;

  /**
  * Reads the named field and set its value in char array type out param.
  * char* type is corresponding to java String type.
  * @param fieldname name of the field to read
  * @param value value of the field to be set with char array type.
  * @param length length is set with number of char* elements.
  * @throws IllegalStateException if PdxInstance doesn't has the named field.
  *
  * @see PdxInstance#hasField
  */
  virtual void getField(const char* fieldName, char** value,
                        int32_t& length) const;

  // String
  /**
  * Reads the named field and set its value in wchar_t* type out param.
  * wchar_t* type is corresponding to java String type.
  * @param fieldname name of the field to read
  * @param value value of the field to be set with wchar_t type.
  * @throws IllegalStateException if PdxInstance doesn't has the named field.
  *
  * @see PdxInstance#hasField
  */
  virtual void getField(const char* fieldname, wchar_t** value) const;

  /**
  * Reads the named field and set its value in char* type out param.
  * char* type is corresponding to java String type.
  * @param fieldname name of the field to read
  * @param value value of the field to be set with char* type.
  * @throws IllegalStateException if PdxInstance doesn't has the named field.
  *
  * @see PdxInstance#hasField
  */
  virtual void getField(const char* fieldname, char** value) const;

  // StringArray
  /**
  * Reads the named field and set its value in wchar_t* array type out param.
  * wchar_t** type is corresponding to java String[] type.
  * @param fieldname name of the field to read
  * @param value value of the field to be set with wchar_t* array type.
  * @param length length is set with number of wchar_t** elements.
  * @throws IllegalStateException if PdxInstance doesn't has the named field.
  *
  * @see PdxInstance#hasField
  */
  virtual void getField(const char* fieldname, wchar_t*** value,
                        int32_t& length) const;

  /**
  * Reads the named field and set its value in char* array type out param.
  * char** type is corresponding to java String[] type.
  * @param fieldname name of the field to read
  * @param value value of the field to be set with char* array type.
  * @param length length is set with number of char** elements.
  * @throws IllegalStateException if PdxInstance doesn't has the named field.
  *
  * @see PdxInstance#hasField
  */
  virtual void getField(const char* fieldname, char*** value,
                        int32_t& length) const;

  /**
  * Reads the named field and set its value in CacheableDatePtr type out param.
  * CacheableDatePtr type is corresponding to java Java.util.date type.
  * @param fieldname name of the field to read
  * @param value value of the field to be set with CacheableDatePtr type.
  * @throws IllegalStateException if PdxInstance doesn't has the named field.
  *
  * @see PdxInstance#hasField
  */
  virtual void getField(const char* fieldname, CacheableDatePtr& value) const;

  /**
  * Reads the named field and set its value in array of byte arrays type out
  * param.
  * int8_t** type is corresponding to java byte[][] type.
  * @param fieldname name of the field to read.
  * @param value value of the field to be set with array of byte arrays type.
  * @param arrayLength arrayLength is set to the number of byte arrays.
  * @param elementLength elementLength is set to individual byte array lengths.
  * @throws IllegalStateException if PdxInstance doesn't has the named field.
  *
  * @see PdxInstance#hasField
  */
  virtual void getField(const char* fieldName, int8_t*** value,
                        int32_t& arrayLength, int32_t*& elementLength) const;

  /**
  * Reads the named field and set its value in CacheablePtr type out param.
  * CacheablePtr type is corresponding to java object type.
  * @param fieldname name of the field to read
  * @param value value of the field to be set with CacheablePtr type.
  * @throws IllegalStateException if PdxInstance doesn't has the named field.
  * For deserialization C++ Native Client requires the domain class to be
  * registered.
  *
  * @see Serializable::registerPdxType
  * @see PdxInstance#hasField
  */
  virtual void getField(const char* fieldname, CacheablePtr& value) const;

  /**
  * Reads the named field and set its value in CacheableObjectArrayPtr type out
  * param.
  * For deserialization C++ Native Client requires the domain class to be
  * registered.
  * CacheableObjectArrayPtr type is corresponding to java Object[] type.
  * @param fieldname name of the field to read.
  * @param value value of the field to be set with CacheableObjectArrayPtr type.
  * @throws IllegalStateException if PdxInstance doesn't has the named field.
  *
  * @see Serializable::registerPdxType
  * @see PdxInstance#hasField
  */
  virtual void getField(const char* fieldname,
                        CacheableObjectArrayPtr& value) const;

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * bool type is corresponding to java boolean type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type bool
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, bool value);

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * signed char type is corresponding to java byte type.
  * For C++ on Windows and Linux, signed char type is corresponding to int8_t
  * type.
  * However C++ users on Solaris should always use this api after casting int8_t
  * to signed char.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type signed char
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, signed char value);

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * unsigned char type is corresponding to java byte type.
  * For C++ on Windows and Linux, unsigned char type is corresponding to int8_t
  * type.
  * However C++ users on Solaris should always use this api after casting int8_t
  * to unsigned char.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type unsigned char
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, unsigned char value);

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * int16_t type is corresponding to java short type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type int16_t
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, int16_t value);

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * int32_t type is corresponding to java int type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type int32_t
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, int32_t value);

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * int64_t type is corresponding to java long type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type int64_t
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, int64_t value);

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * float type is corresponding to java float type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type float
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, float value);

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * double type is corresponding to java double type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type double
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, double value);

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * wchar_t type is corresponding to java char type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type wchar_t
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, wchar_t value);

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * char type is corresponding to java char type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type char
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, char value);

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * CacheableDatePtr type is corresponding to java Java.util.date type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type CacheableDatePtr
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, CacheableDatePtr value);

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * bool* type is corresponding to java boolean[] type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type bool array
  * @param length
  *          The number of elements in bool array type.
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, bool* value, int32_t length);

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * signed char* type is corresponding to java byte[] type.
  * For C++ on Windows and Linux, signed char* type is corresponding to int8_t*
  * type.
  * However C++ users on Solaris should always use this api after casting
  * int8_t* to signed char*.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type signed char array
  * @param length
  *          The number of elements in signed char array type.
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, signed char* value,
                        int32_t length);

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * unsigned char* type is corresponding to java byte[] type.
  * For C++ on Windows and Linux, unsigned char* type is corresponding to
  * int8_t* type.
  * However C++ users on Solaris should always use this api after casting
  * int8_t* to unsigned char*.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type unsigned char array
  * @param length
  *          The number of elements in unsigned char array type.
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, unsigned char* value,
                        int32_t length);

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * int16_t* type is corresponding to java short[] type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type int16_t array
  * @param length
  *          The number of elements in int16_t array type.
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, int16_t* value, int32_t length);

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * int32_t* type is corresponding to java int[] type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type int32_t array
  * @param length
  *          The number of elements in int32_t array type.
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, int32_t* value, int32_t length);

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * int64_t* type is corresponding to java long[] type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type int64_t array
  * @param length
  *          The number of elements in int64_t array type.
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, int64_t* value, int32_t length);

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * float* type is corresponding to java float[] type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type float array
  * @param length
  *          The number of elements in float array type.
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, float* value, int32_t length);

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * double* type is corresponding to java double[] type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type double array
  * @param length
  *          The number of elements in double array type.
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, double* value, int32_t length);

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * wchar_t* type is corresponding to java String type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type wchar_t*
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, const wchar_t* value);

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * char* type is corresponding to java String type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type char*
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, const char* value);

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * wchar_t* type is corresponding to java char[] type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type wchar_t array
  * @param length
  *          The number of elements in wchar_t array type.
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, wchar_t* value, int32_t length);

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * char* type is corresponding to java char[] type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type char array
  * @param length
  *          The number of elements in char array type.
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, char* value, int32_t length);

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * wchar_t** type is corresponding to java String[] type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type wchar_t* array
  * @param length
  *          The number of elements in WCString array type.
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, wchar_t** value, int32_t length);

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * char** type is corresponding to java String[] type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type char* array
  * @param length
  *          The number of elements in CString array type.
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, char** value, int32_t length);

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * int8_t** type is corresponding to java byte[][] type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type array of byte arrays
  * @param arrayLength
  *          The number of byte arrays.
  * @param elementLength
  *          The lengths of individual byte arrays.
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, int8_t** value,
                        int32_t arrayLength, int32_t* elementLength);

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  *  So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  *
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be assigned to the field of type CacheablePtr
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, CacheablePtr value);

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * CacheableObjectArrayPtr type is corresponding to java Object[] type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type CacheableObjectArrayPtr
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, CacheableObjectArrayPtr value);

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
  virtual bool isIdentityField(const char* fieldname);

  /**
  * Creates and returns a {@link WritablePdxInstance} whose initial
  * values are those of this PdxInstance.
  * This call returns a copy of the current field values so modifications
  * made to the returned value will not modify this PdxInstance.
  * @return a {@link WritablePdxInstance}
  */
  virtual WritablePdxInstancePtr createWriter();

  /**
  * Generates a hashcode based on the identity fields of
  * this PdxInstance.
  * <p>If a PdxInstance has marked identity fields using {@link
  * PdxWriter#markIdentityField}
  * then only the marked identity fields are its identity fields.
  * Otherwise all its fields are identity fields.
  * </p>
  * For deserialization C++ Native Client requires the domain class to be
  * registered.
  * If the field is an array then all array
  * elements are used for hashcode computation.
  * Otherwise the raw bytes of its value are used to compute the hash code.
  * @throws IllegalStateException if the field contains an element that is not
  * of CacheableKey derived type.
  *
  * @see Serializable::registerPdxType
  */
  virtual uint32_t hashcode() const;

  /**
  * Prints out all of the identity fields of this PdxInstance.
  * <p>If a PdxInstance has marked identity fields using {@link
  * PdxWriter#markIdentityField}
  * then only the marked identity fields are its identity fields.
  * Otherwise all its fields are identity fields</p>.
  * For deserialization C++ Native Client requires the domain class to be
  * registered.
  *
  * @see Serializable::registerPdxType
  */
  virtual CacheableStringPtr toString() const;

  /**
  * @brief serialize this object. This is an internal method.
  */
  virtual void toData(DataOutput& output) const { PdxInstance::toData(output); }

  /**
   * @brief deserialize this object, typical implementation should return
   * the 'this' pointer. This is an internal method.
   */
  virtual Serializable* fromData(DataInput& input) {
    return PdxInstance::fromData(input);
  }

  /**
  * Returns true if the given CacheableKey derived object is equals to this
  * instance.
  * <p>If <code>other</code> is not a PdxInstance then it is not equal to this
  * instance.
  * NOTE: Even if <code>other</code> is the result of calling {@link
  * #getObject()} it will not
  * be equal to this instance</p>.
  * <p>Otherwise equality of two PdxInstances is determined as follows:
  * <ol>
  * <li>The domain class name must be equal for both PdxInstances
  * <li>Each identity field must be equal.
  * </ol> </p>
  * If one of the instances does not have a field that the other one does then
  * equals will assume it
  * has the field with a default value.
  * If a PdxInstance has marked identity fields using {@link
  * PdxWriter#markIdentityField markIdentityField}
  * then only the marked identity fields are its identity fields.
  * Otherwise all its fields are identity fields.
  * <p>An identity field is equal if all the following are true:
  * <ol>
  * <li>The field name is equal.
  * <li>The field type is equal.
  * <li>The field value is equal.
  * </ol> </p>
  * If an identity field is of type derived from <code>Cacheable</code> then it
  * is deserialized. For deserialization C++ Native Client requires the domain
  * class to be registered.
  * If the deserialized object is an array then all array elements
  * are used to determine equality.
  * If an identity field is of type <code>CacheableObjectArray</code> then it is
  * deserialized and all array elements are used to determine equality.
  * For all other field types the value does not need to be deserialized.
  * Instead the serialized raw bytes are compared and used to determine
  * equality.
  * @param other the other instance to compare to this.
  * @return <code>true</code> if this instance is equal to <code>other</code>.
  * @throws IllegalStateException if the field contains an element that is not
  * of CacheableKey derived type.
  *
  * @see Serializable::registerPdxType
  */
  virtual bool operator==(const CacheableKey& other) const;

  /** @return the size of the object in bytes
   * This is an internal method.
   * It is used in case of heap LRU property is set.
   */
  virtual uint32_t objectSize() const;

  /**
  * Return an unmodifiable list of the field names on this PdxInstance.
  * @return an unmodifiable list of the field names on this PdxInstance
  */
  virtual CacheableStringArrayPtr getFieldNames();

  // From PdxSerializable
  /**
   * @brief serialize this object in gemfire PDX format. This is an internal
   * method.
   * @param PdxWriter to serialize the PDX object
   */
  virtual void toData(PdxWriterPtr output) /*const*/;

  /**
  * @brief Deserialize this object. This is an internal method.
  * @param PdxReader to Deserialize the PDX object
  */
  virtual void fromData(PdxReaderPtr input);

  /**
  * Return the full name of the class that this pdx instance represents.
  * @return the name of the class that this pdx instance represents.
  * @throws IllegalStateException if the PdxInstance typeid is not defined yet,
  * to get classname
  * or if PdxType is not defined for PdxInstance.
  */
  virtual const char* getClassName() const;

  virtual PdxFieldTypes::PdxFieldType getFieldType(const char* fieldname) const;

  void setPdxId(int32_t typeId);

 public:
  /**
  * @brief constructors
  */
  PdxInstanceImpl();

  PdxInstanceImpl(uint8_t* buffer, int length, int typeId) {
    m_buffer = DataInput::getBufferCopy(buffer, length);
    m_bufferLength = length;
    LOGDEBUG("PdxInstanceImpl::m_bufferLength = %d ", m_bufferLength);
    m_typeId = typeId;
    m_pdxType = NULLPTR;
  }

  PdxInstanceImpl(FieldVsValues fieldVsValue, PdxTypePtr pdxType);

  PdxTypePtr getPdxType() const;

  void updatePdxStream(uint8_t* newPdxStream, int len);

 private:
  // never implemented.
  PdxInstanceImpl(const PdxInstanceImpl& other);
  void operator=(const PdxInstanceImpl& other);

  uint8_t* m_buffer;
  int m_bufferLength;
  int m_typeId;
  PdxTypePtr m_pdxType;
  FieldVsValues m_updatedFields;

  std::vector<PdxFieldTypePtr> getIdentityPdxFields(PdxTypePtr pt) const;

  int getOffset(DataInput& dataInput, PdxTypePtr pt, int sequenceId) const;

  int getRawHashCode(PdxTypePtr pt, PdxFieldTypePtr pField,
                     DataInput& dataInput) const;

  int getNextFieldPosition(DataInput& dataInput, int fieldId,
                           PdxTypePtr pt) const;

  int getSerializedLength(DataInput& dataInput, PdxTypePtr pt) const;

  bool hasDefaultBytes(PdxFieldTypePtr pField, DataInput& dataInput, int start,
                       int end) const;

  bool compareDefaulBytes(DataInput& dataInput, int start, int end,
                          int8_t* defaultBytes, int32_t length) const;

  void writeField(PdxWriterPtr writer, const char* fieldName, int typeId,
                  CacheablePtr value);

  void writeUnmodifieldField(DataInput& dataInput, int startPos, int endPos,
                             PdxLocalWriterPtr localWriter);

  void setOffsetForObject(DataInput& dataInput, PdxTypePtr pt,
                          int sequenceId) const;

  bool compareRawBytes(PdxInstanceImpl& other, PdxTypePtr myPT,
                       PdxFieldTypePtr myF, DataInput& myDataInput,
                       PdxTypePtr otherPT, PdxFieldTypePtr otherF,
                       DataInput& otherDataInput) const;

  void equatePdxFields(std::vector<PdxFieldTypePtr>& my,
                       std::vector<PdxFieldTypePtr>& other) const;

  static int deepArrayHashCode(CacheablePtr obj);

  static int enumerateMapHashCode(CacheableHashMapPtr map);

  static int enumerateVectorHashCode(CacheableVectorPtr vec);

  static int enumerateArrayListHashCode(CacheableArrayListPtr arrList);

  static int enumerateLinkedListHashCode(CacheableLinkedListPtr linkedList);

  static int enumerateObjectArrayHashCode(CacheableObjectArrayPtr objArray);

  static int enumerateSetHashCode(CacheableHashSetPtr set);

  static int enumerateLinkedSetHashCode(CacheableLinkedHashSetPtr linkedset);

  static int enumerateHashTableCode(CacheableHashTablePtr hashTable);

  static bool deepArrayEquals(CacheablePtr obj, CacheablePtr otherObj);

  static bool enumerateObjectArrayEquals(CacheableObjectArrayPtr Obj,
                                         CacheableObjectArrayPtr OtherObj);

  static bool enumerateVectorEquals(CacheableVectorPtr Obj,
                                    CacheableVectorPtr OtherObj);

  static bool enumerateArrayListEquals(CacheableArrayListPtr Obj,
                                       CacheableArrayListPtr OtherObj);

  static bool enumerateMapEquals(CacheableHashMapPtr Obj,
                                 CacheableHashMapPtr OtherObj);

  static bool enumerateSetEquals(CacheableHashSetPtr Obj,
                                 CacheableHashSetPtr OtherObj);

  static bool enumerateLinkedSetEquals(CacheableLinkedHashSetPtr Obj,
                                       CacheableLinkedHashSetPtr OtherObj);

  static bool enumerateHashTableEquals(CacheableHashTablePtr Obj,
                                       CacheableHashTablePtr OtherObj);

  static int8_t m_BooleanDefaultBytes[];
  static int8_t m_ByteDefaultBytes[];
  static int8_t m_CharDefaultBytes[];
  static int8_t m_ShortDefaultBytes[];
  static int8_t m_IntDefaultBytes[];
  static int8_t m_LongDefaultBytes[];
  static int8_t m_FloatDefaultBytes[];
  static int8_t m_DoubleDefaultBytes[];
  static int8_t m_DateDefaultBytes[];
  static int8_t m_StringDefaultBytes[];
  static int8_t m_ObjectDefaultBytes[];
  static int8_t m_NULLARRAYDefaultBytes[];
  static PdxFieldTypePtr m_DefaultPdxFieldType;
};
typedef SharedPtr<PdxInstanceImpl> PdxInstanceImplPtr;
}

#endif /* __PDXINSTANCE_IMPL_HPP_*/
