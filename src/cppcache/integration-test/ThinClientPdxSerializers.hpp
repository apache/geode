/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __PDXSERIALIZERS__
#define __PDXSERIALIZERS__

static const char *CLASSNAME1 = "PdxTests.PdxType";
static const char *CLASSNAME2 = "PdxTests.Address";

class TestPdxSerializer : public PdxSerializer {
 public:
  static void deallocate(void *testObject, const char *className) {
    ASSERT(strcmp(className, CLASSNAME1) == 0 ||
               strcmp(className, CLASSNAME2) == 0,
           "Unexpected classname in deallocate()");
    LOG("TestPdxSerializer::deallocate called");
    if (strcmp(className, CLASSNAME1) == 0) {
      PdxTests::NonPdxType *npt =
          reinterpret_cast<PdxTests::NonPdxType *>(testObject);
      delete npt;
    } else {
      PdxTests::NonPdxAddress *npa =
          reinterpret_cast<PdxTests::NonPdxAddress *>(testObject);
      delete npa;
    }
  }

  static uint32_t objectSize(void *testObject, const char *className) {
    ASSERT(strcmp(className, CLASSNAME1) == 0 ||
               strcmp(className, CLASSNAME2) == 0,
           "Unexpected classname in objectSize()");
    LOG("TestPdxSerializer::objectSize called");
    return 12345;  // dummy value
  }

  UserDeallocator getDeallocator(const char *className) {
    ASSERT(strcmp(className, CLASSNAME1) == 0 ||
               strcmp(className, CLASSNAME2) == 0,
           "Unexpected classname in getDeallocator");
    return deallocate;
  }

  UserObjectSizer getObjectSizer(const char *className) {
    ASSERT(strcmp(className, CLASSNAME1) == 0 ||
               strcmp(className, CLASSNAME2) == 0,
           "Unexpected classname in getObjectSizer");
    return objectSize;
  }

  void *fromDataForAddress(PdxReaderPtr pr) {
    try {
      PdxTests::NonPdxAddress *npa = new PdxTests::NonPdxAddress;
      npa->_aptNumber = pr->readInt("_aptNumber");
      npa->_street = pr->readString("_street");
      npa->_city = pr->readString("_city");
      return (void *)npa;
    } catch (...) {
      return NULL;
    }
  }

  void *fromData(const char *className, PdxReaderPtr pr) {
    ASSERT(strcmp(className, CLASSNAME1) == 0 ||
               strcmp(className, CLASSNAME2) == 0,
           "Unexpected classname in fromData");

    if (strcmp(className, CLASSNAME2) == 0) {
      return fromDataForAddress(pr);
    }

    PdxTests::NonPdxType *npt = new PdxTests::NonPdxType;

    try {
      int32_t *Lengtharr;
      GF_NEW(Lengtharr, int32_t[2]);
      int32_t arrLen = 0;
      npt->m_byteByteArray =
          pr->readArrayOfByteArrays("m_byteByteArray", arrLen, &Lengtharr);
      // TODO::need to write compareByteByteArray() and check for
      // m_byteByteArray elements

      npt->m_char = pr->readChar("m_char");
      // GenericValCompare

      npt->m_bool = pr->readBoolean("m_bool");
      // GenericValCompare
      npt->m_boolArray = pr->readBooleanArray("m_boolArray", npt->boolArrayLen);

      npt->m_byte = pr->readByte("m_byte");
      npt->m_byteArray = pr->readByteArray("m_byteArray", npt->byteArrayLen);
      npt->m_charArray = pr->readCharArray("m_charArray", npt->charArrayLen);

      npt->m_arraylist = pr->readObject("m_arraylist");

      npt->m_map = dynCast<CacheableHashMapPtr>(pr->readObject("m_map"));
      // TODO:Check for the size

      npt->m_hashtable = pr->readObject("m_hashtable");
      // TODO:Check for the size

      npt->m_vector = pr->readObject("m_vector");
      // TODO::Check for size

      npt->m_chs = pr->readObject("m_chs");
      // TODO::Size check

      npt->m_clhs = pr->readObject("m_clhs");
      // TODO:Size check

      npt->m_string = pr->readString("m_string");  // GenericValCompare
      npt->m_date = pr->readDate("m_dateTime");    // compareData

      npt->m_double = pr->readDouble("m_double");

      npt->m_doubleArray =
          pr->readDoubleArray("m_doubleArray", npt->doubleArrayLen);
      npt->m_float = pr->readFloat("m_float");
      npt->m_floatArray =
          pr->readFloatArray("m_floatArray", npt->floatArrayLen);
      npt->m_int16 = pr->readShort("m_int16");
      npt->m_int32 = pr->readInt("m_int32");
      npt->m_long = pr->readLong("m_long");
      npt->m_int32Array = pr->readIntArray("m_int32Array", npt->intArrayLen);
      npt->m_longArray = pr->readLongArray("m_longArray", npt->longArrayLen);
      npt->m_int16Array =
          pr->readShortArray("m_int16Array", npt->shortArrayLen);
      npt->m_sbyte = pr->readByte("m_sbyte");
      npt->m_sbyteArray = pr->readByteArray("m_sbyteArray", npt->byteArrayLen);
      npt->m_stringArray =
          pr->readStringArray("m_stringArray", npt->strLenArray);
      npt->m_uint16 = pr->readShort("m_uint16");
      npt->m_uint32 = pr->readInt("m_uint32");
      npt->m_ulong = pr->readLong("m_ulong");
      npt->m_uint32Array = pr->readIntArray("m_uint32Array", npt->intArrayLen);
      npt->m_ulongArray = pr->readLongArray("m_ulongArray", npt->longArrayLen);
      npt->m_uint16Array =
          pr->readShortArray("m_uint16Array", npt->shortArrayLen);
      // LOGINFO("PdxType::readInt() start...");

      npt->m_byte252 = pr->readByteArray("m_byte252", npt->m_byte252Len);
      npt->m_byte253 = pr->readByteArray("m_byte253", npt->m_byte253Len);
      npt->m_byte65535 = pr->readByteArray("m_byte65535", npt->m_byte65535Len);
      npt->m_byte65536 = pr->readByteArray("m_byte65536", npt->m_byte65536Len);

      npt->m_pdxEnum = pr->readObject("m_pdxEnum");

      npt->m_address = pr->readObject("m_address");

      npt->m_objectArray = pr->readObjectArray("m_objectArray");

      LOGINFO("TestPdxSerializer: NonPdxType fromData() Done.");
    } catch (...) {
      return NULL;
    }
    return (void *)npt;
  }

  bool toDataForAddress(void *testObject, PdxWriterPtr pw) {
    try {
      PdxTests::NonPdxAddress *npa =
          reinterpret_cast<PdxTests::NonPdxAddress *>(testObject);
      pw->writeInt("_aptNumber", npa->_aptNumber);
      pw->writeString("_street", npa->_street);
      pw->writeString("_city", npa->_city);
      return true;
    } catch (...) {
      return false;
    }
  }

  bool toData(void *testObject, const char *className, PdxWriterPtr pw) {
    ASSERT(strcmp(className, CLASSNAME1) == 0 ||
               strcmp(className, CLASSNAME2) == 0,
           "Unexpected classname in toData");

    if (strcmp(className, CLASSNAME2) == 0) {
      return toDataForAddress(testObject, pw);
    }

    PdxTests::NonPdxType *npt =
        reinterpret_cast<PdxTests::NonPdxType *>(testObject);

    try {
      int *lengthArr = new int[2];

      lengthArr[0] = 1;
      lengthArr[1] = 2;
      pw->writeArrayOfByteArrays("m_byteByteArray", npt->m_byteByteArray, 2,
                                 lengthArr);
      pw->writeChar("m_char", npt->m_char);
      pw->markIdentityField("m_char");
      pw->writeBoolean("m_bool", npt->m_bool);  // 1
      pw->markIdentityField("m_bool");
      pw->writeBooleanArray("m_boolArray", npt->m_boolArray, 3);
      pw->markIdentityField("m_boolArray");
      pw->writeByte("m_byte", npt->m_byte);
      pw->markIdentityField("m_byte");
      pw->writeByteArray("m_byteArray", npt->m_byteArray, 2);
      pw->markIdentityField("m_byteArray");
      pw->writeCharArray("m_charArray", npt->m_charArray, 2);
      pw->markIdentityField("m_charArray");
      pw->writeObject("m_arraylist", npt->m_arraylist);
      pw->markIdentityField("m_arraylist");
      pw->writeObject("m_map", npt->m_map);
      pw->markIdentityField("m_map");
      pw->writeObject("m_hashtable", npt->m_hashtable);
      pw->markIdentityField("m_hashtable");
      pw->writeObject("m_vector", npt->m_vector);
      pw->markIdentityField("m_vector");
      pw->writeObject("m_chs", npt->m_chs);
      pw->markIdentityField("m_chs");
      pw->writeObject("m_clhs", npt->m_clhs);
      pw->markIdentityField("m_clhs");
      pw->writeString("m_string", npt->m_string);
      pw->markIdentityField("m_string");
      pw->writeDate("m_dateTime", npt->m_date);
      pw->markIdentityField("m_dateTime");
      pw->writeDouble("m_double", npt->m_double);
      pw->markIdentityField("m_double");
      pw->writeDoubleArray("m_doubleArray", npt->m_doubleArray, 2);
      pw->markIdentityField("m_doubleArray");
      pw->writeFloat("m_float", npt->m_float);
      pw->markIdentityField("m_float");
      pw->writeFloatArray("m_floatArray", npt->m_floatArray, 2);
      pw->markIdentityField("m_floatArray");
      pw->writeShort("m_int16", npt->m_int16);
      pw->markIdentityField("m_int16");
      pw->writeInt("m_int32", npt->m_int32);
      pw->markIdentityField("m_int32");
      pw->writeLong("m_long", npt->m_long);
      pw->markIdentityField("m_long");
      pw->writeIntArray("m_int32Array", npt->m_int32Array, 4);
      pw->markIdentityField("m_int32Array");
      pw->writeLongArray("m_longArray", npt->m_longArray, 2);
      pw->markIdentityField("m_longArray");
      pw->writeShortArray("m_int16Array", npt->m_int16Array, 2);
      pw->markIdentityField("m_int16Array");
      pw->writeByte("m_sbyte", npt->m_sbyte);
      pw->markIdentityField("m_sbyte");
      pw->writeByteArray("m_sbyteArray", npt->m_sbyteArray, 2);
      pw->markIdentityField("m_sbyteArray");

      int *strlengthArr = new int[2];

      strlengthArr[0] = 5;
      strlengthArr[1] = 5;
      pw->writeStringArray("m_stringArray", npt->m_stringArray, 2);
      pw->markIdentityField("m_stringArray");
      pw->writeShort("m_uint16", npt->m_uint16);
      pw->markIdentityField("m_uint16");
      pw->writeInt("m_uint32", npt->m_uint32);
      pw->markIdentityField("m_uint32");
      pw->writeLong("m_ulong", npt->m_ulong);
      pw->markIdentityField("m_ulong");
      pw->writeIntArray("m_uint32Array", npt->m_uint32Array, 4);
      pw->markIdentityField("m_uint32Array");
      pw->writeLongArray("m_ulongArray", npt->m_ulongArray, 2);
      pw->markIdentityField("m_ulongArray");
      pw->writeShortArray("m_uint16Array", npt->m_uint16Array, 2);
      pw->markIdentityField("m_uint16Array");

      pw->writeByteArray("m_byte252", npt->m_byte252, 252);
      pw->markIdentityField("m_byte252");
      pw->writeByteArray("m_byte253", npt->m_byte253, 253);
      pw->markIdentityField("m_byte253");
      pw->writeByteArray("m_byte65535", npt->m_byte65535, 65535);
      pw->markIdentityField("m_byte65535");
      pw->writeByteArray("m_byte65536", npt->m_byte65536, 65536);
      pw->markIdentityField("m_byte65536");

      pw->writeObject("m_pdxEnum", npt->m_pdxEnum);
      pw->markIdentityField("m_pdxEnum");

      pw->writeObject("m_address", npt->m_objectArray);
      pw->writeObjectArray("m_objectArray", npt->m_objectArray);

      LOG("TestPdxSerializer: NonPdxType toData() Done......");
    } catch (...) {
      return false;
    }
    return true;
  }
};

#endif  // __PDXSERIALIZERS__
