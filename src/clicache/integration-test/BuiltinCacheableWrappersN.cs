//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;

namespace GemStone.GemFire.Cache.UnitTests.NewAPI
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Generic;
  
  #region Helper class

  public class CacheableHelper
  {
    #region Constants and statics

    private static uint[] CRC32Table = {
      0x00000000, 0x77073096, 0xee0e612c, 0x990951ba, 0x076dc419, 0x706af48f,
      0xe963a535, 0x9e6495a3, 0x0edb8832, 0x79dcb8a4, 0xe0d5e91e, 0x97d2d988,
      0x09b64c2b, 0x7eb17cbd, 0xe7b82d07, 0x90bf1d91, 0x1db71064, 0x6ab020f2,
      0xf3b97148, 0x84be41de, 0x1adad47d, 0x6ddde4eb, 0xf4d4b551, 0x83d385c7,
      0x136c9856, 0x646ba8c0, 0xfd62f97a, 0x8a65c9ec, 0x14015c4f, 0x63066cd9,
      0xfa0f3d63, 0x8d080df5, 0x3b6e20c8, 0x4c69105e, 0xd56041e4, 0xa2677172,
      0x3c03e4d1, 0x4b04d447, 0xd20d85fd, 0xa50ab56b, 0x35b5a8fa, 0x42b2986c,
      0xdbbbc9d6, 0xacbcf940, 0x32d86ce3, 0x45df5c75, 0xdcd60dcf, 0xabd13d59,
      0x26d930ac, 0x51de003a, 0xc8d75180, 0xbfd06116, 0x21b4f4b5, 0x56b3c423,
      0xcfba9599, 0xb8bda50f, 0x2802b89e, 0x5f058808, 0xc60cd9b2, 0xb10be924,
      0x2f6f7c87, 0x58684c11, 0xc1611dab, 0xb6662d3d, 0x76dc4190, 0x01db7106,
      0x98d220bc, 0xefd5102a, 0x71b18589, 0x06b6b51f, 0x9fbfe4a5, 0xe8b8d433,
      0x7807c9a2, 0x0f00f934, 0x9609a88e, 0xe10e9818, 0x7f6a0dbb, 0x086d3d2d,
      0x91646c97, 0xe6635c01, 0x6b6b51f4, 0x1c6c6162, 0x856530d8, 0xf262004e,
      0x6c0695ed, 0x1b01a57b, 0x8208f4c1, 0xf50fc457, 0x65b0d9c6, 0x12b7e950,
      0x8bbeb8ea, 0xfcb9887c, 0x62dd1ddf, 0x15da2d49, 0x8cd37cf3, 0xfbd44c65,
      0x4db26158, 0x3ab551ce, 0xa3bc0074, 0xd4bb30e2, 0x4adfa541, 0x3dd895d7,
      0xa4d1c46d, 0xd3d6f4fb, 0x4369e96a, 0x346ed9fc, 0xad678846, 0xda60b8d0,
      0x44042d73, 0x33031de5, 0xaa0a4c5f, 0xdd0d7cc9, 0x5005713c, 0x270241aa,
      0xbe0b1010, 0xc90c2086, 0x5768b525, 0x206f85b3, 0xb966d409, 0xce61e49f,
      0x5edef90e, 0x29d9c998, 0xb0d09822, 0xc7d7a8b4, 0x59b33d17, 0x2eb40d81,
      0xb7bd5c3b, 0xc0ba6cad, 0xedb88320, 0x9abfb3b6, 0x03b6e20c, 0x74b1d29a,
      0xead54739, 0x9dd277af, 0x04db2615, 0x73dc1683, 0xe3630b12, 0x94643b84,
      0x0d6d6a3e, 0x7a6a5aa8, 0xe40ecf0b, 0x9309ff9d, 0x0a00ae27, 0x7d079eb1,
      0xf00f9344, 0x8708a3d2, 0x1e01f268, 0x6906c2fe, 0xf762575d, 0x806567cb,
      0x196c3671, 0x6e6b06e7, 0xfed41b76, 0x89d32be0, 0x10da7a5a, 0x67dd4acc,
      0xf9b9df6f, 0x8ebeeff9, 0x17b7be43, 0x60b08ed5, 0xd6d6a3e8, 0xa1d1937e,
      0x38d8c2c4, 0x4fdff252, 0xd1bb67f1, 0xa6bc5767, 0x3fb506dd, 0x48b2364b,
      0xd80d2bda, 0xaf0a1b4c, 0x36034af6, 0x41047a60, 0xdf60efc3, 0xa867df55,
      0x316e8eef, 0x4669be79, 0xcb61b38c, 0xbc66831a, 0x256fd2a0, 0x5268e236,
      0xcc0c7795, 0xbb0b4703, 0x220216b9, 0x5505262f, 0xc5ba3bbe, 0xb2bd0b28,
      0x2bb45a92, 0x5cb36a04, 0xc2d7ffa7, 0xb5d0cf31, 0x2cd99e8b, 0x5bdeae1d,
      0x9b64c2b0, 0xec63f226, 0x756aa39c, 0x026d930a, 0x9c0906a9, 0xeb0e363f,
      0x72076785, 0x05005713, 0x95bf4a82, 0xe2b87a14, 0x7bb12bae, 0x0cb61b38,
      0x92d28e9b, 0xe5d5be0d, 0x7cdcefb7, 0x0bdbdf21, 0x86d3d2d4, 0xf1d4e242,
      0x68ddb3f8, 0x1fda836e, 0x81be16cd, 0xf6b9265b, 0x6fb077e1, 0x18b74777,
      0x88085ae6, 0xff0f6a70, 0x66063bca, 0x11010b5c, 0x8f659eff, 0xf862ae69,
      0x616bffd3, 0x166ccf45, 0xa00ae278, 0xd70dd2ee, 0x4e048354, 0x3903b3c2,
      0xa7672661, 0xd06016f7, 0x4969474d, 0x3e6e77db, 0xaed16a4a, 0xd9d65adc,
      0x40df0b66, 0x37d83bf0, 0xa9bcae53, 0xdebb9ec5, 0x47b2cf7f, 0x30b5ffe9,
      0xbdbdf21c, 0xcabac28a, 0x53b39330, 0x24b4a3a6, 0xbad03605, 0xcdd70693,
      0x54de5729, 0x23d967bf, 0xb3667a2e, 0xc4614ab8, 0x5d681b02, 0x2a6f2b94,
      0xb40bbe37, 0xc30c8ea1, 0x5a05df1b, 0x2d02ef8d
    };

    #endregion

    /// <summary>
    /// Generate a random <c>CData</c> structure.
    /// </summary>
    public static CData RandCData()
    {
      long rnd = (long)Util.Rand(int.MaxValue);
      rnd = (rnd << 32) + (long)Util.Rand(int.MaxValue);
      return new CData(Util.Rand(int.MaxValue), rnd);
    }

    public static PdxCData RandPdxCData()
    {
      long rnd = (long)Util.Rand(int.MaxValue);
      rnd = (rnd << 32) + (long)Util.Rand(int.MaxValue);
      return new PdxCData(Util.Rand(int.MaxValue), rnd);
    }

    public static uint CRC32(byte[] buffer)
    {
      if (buffer == null || buffer.Length == 0)
      {
        return 0;
      }

      uint crc32 = 0xffffffff;

      for (int i = 0; i < buffer.Length; i++)
      {
        crc32 = ((crc32 >> 8) & 0x00ffffff) ^
          CRC32Table[(crc32 ^ buffer[i]) & 0xff];
      }
      return ~crc32;
    }

    public static bool IsContainerTypeId(uint typeId)
    {
      return (typeId == GemFireClassIds.CacheableObjectArray) ||
        (typeId == GemFireClassIds.CacheableVector) ||
        (typeId == GemFireClassIds.CacheableArrayList) ||
        (typeId == GemFireClassIds.CacheableStack) ||
        (typeId == GemFireClassIds.CacheableHashMap) ||
        (typeId == GemFireClassIds.CacheableIdentityHashMap) ||
        (typeId == GemFireClassIds.CacheableHashTable) ||
        (typeId == GemFireClassIds.CacheableLinkedHashSet) ||
        (typeId == GemFireClassIds.CacheableHashSet);
    }

    public static bool IsUnhandledType(uint typeId)
    {
      // TODO: [sumedh] skipping CacheableFileName for now since it will
      // not work on Windows without significant workarounds; also see
      // the corresponding comment in C++ testThinClientCacheables.
      // Also skipping C# specific classes.
      return (typeId == GemFireClassIds.CacheableFileName) ||
        (typeId == GemFireClassIds.CacheableManagedObject) ||
        (typeId == GemFireClassIds.CacheableManagedObjectXml);
    }
    public static long ConstantDateTime = 0;
    public static void RegisterBuiltins(long dateTime)
    {
      ConstantDateTime = dateTime;
      CacheableWrapperFactory.ClearStaticVaraiables();
      #region Cacheable keys

      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableBoolean,
        typeof(CacheableBooleanWrapper), CacheableBooleanWrapper.Create, typeof(bool));
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableByte,
        typeof(CacheableByteWrapper), CacheableByteWrapper.Create, typeof(sbyte));
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableCharacter,
        typeof(CacheableCharacterWrapper), CacheableCharacterWrapper.Create, typeof(Char));
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableDate,
        typeof(CacheableDateWrapper), CacheableDateWrapper.Create, typeof(DateTime));
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableDouble,
        typeof(CacheableDoubleWrapper), CacheableDoubleWrapper.Create, typeof(Double));
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableFileName,
        typeof(CacheableFileNameWrapper), CacheableFileNameWrapper.Create, typeof(CacheableFileName));
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableFloat,
        typeof(CacheableFloatWrapper), CacheableFloatWrapper.Create, typeof(float));
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableInt16,
        typeof(CacheableInt16Wrapper), CacheableInt16Wrapper.Create, typeof(Int16));
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableInt32,
        typeof(CacheableInt32Wrapper), CacheableInt32Wrapper.Create, typeof(Int32));
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableInt64,
        typeof(CacheableInt64Wrapper), CacheableInt64Wrapper.Create, typeof(Int64));
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableASCIIString,
        typeof(CacheableStringWrapper), CacheableStringWrapper.Create, typeof(string));
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableString,
        typeof(CacheableUnicodeStringWrapper), CacheableUnicodeStringWrapper.Create);
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableASCIIStringHuge,
        typeof(CacheableHugeStringWrapper), CacheableHugeStringWrapper.Create);
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableStringHuge,
        typeof(CacheableHugeUnicodeStringWrapper), CacheableHugeUnicodeStringWrapper.Create);
      CacheableWrapperFactory.RegisterKeyType(10001,
      typeof(PdxCDataWrapper), PdxCDataWrapper.Create, typeof(PdxCData));

      //need to register pdx type
      Serializable.RegisterPdxType(PdxCData.CreateDeserializable);

      #endregion

      #region Other cacheables

      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableBytes,
        typeof(CacheableBytesWrapper), CacheableBytesWrapper.Create, typeof(byte[]));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableDoubleArray,
        typeof(CacheableDoubleArrayWrapper), CacheableDoubleArrayWrapper.Create, typeof(Double[]));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableFloatArray,
        typeof(CacheableFloatArrayWrapper), CacheableFloatArrayWrapper.Create, typeof(float[]));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableHashMap,
        typeof(CacheableHashMapWrapper), CacheableHashMapWrapper.Create, typeof(Dictionary<object, object>));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableHashTable,
        typeof(CacheableHashTableWrapper), CacheableHashTableWrapper.Create, typeof(System.Collections.Hashtable));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableIdentityHashMap,
        typeof(CacheableIdentityHashMapWrapper), CacheableIdentityHashMapWrapper.Create, typeof(Dictionary<object, object>));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableHashSet,
        typeof(CacheableHashSetWrapper), CacheableHashSetWrapper.Create, typeof(CacheableHashSet));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableLinkedHashSet,
        typeof(CacheableLinkedHashSetWrapper), CacheableLinkedHashSetWrapper.Create, typeof(CacheableLinkedHashSet));

      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableInt16Array,
        typeof(CacheableInt16ArrayWrapper), CacheableInt16ArrayWrapper.Create, typeof(Int16[]));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableInt32Array,
        typeof(CacheableInt32ArrayWrapper), CacheableInt32ArrayWrapper.Create, typeof(Int32[]));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableInt64Array,
        typeof(CacheableInt64ArrayWrapper), CacheableInt64ArrayWrapper.Create, typeof(Int64[]));
      {//old one
        //CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableNullString,
        //  typeof(CacheableNullStringWrapper), CacheableNullStringWrapper.Create);
        //CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableASCIIString,
        //  typeof(CacheableEmptyStringWrapper), CacheableEmptyStringWrapper.Create);
        //CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableString,
        //  typeof(CacheableEmptyUnicodeStringWrapper), CacheableEmptyUnicodeStringWrapper.Create);
      }
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableStringArray,
        typeof(CacheableStringArrayWrapper), CacheableStringArrayWrapper.Create, typeof(string[]));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableUndefined,
        typeof(CacheableUndefinedWrapper), CacheableUndefinedWrapper.Create, typeof(CacheableUndefined));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableVector,
        typeof(CacheableVectorWrapper), CacheableVectorWrapper.Create, typeof(System.Collections.ArrayList));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableObjectArray,
        typeof(CacheableObjectArrayWrapper), CacheableObjectArrayWrapper.Create, typeof(CacheableObjectArray));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableArrayList,
        typeof(CacheableArrayListWrapper), CacheableArrayListWrapper.Create, typeof(List<object>));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableStack,
        typeof(CacheableStackWrapper), CacheableStackWrapper.Create, typeof(Stack<object>));

      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableManagedObject,
        typeof(CacheableObjectWrapper), CacheableObjectWrapper.Create, typeof(CacheableObject));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableManagedObjectXml,
        typeof(CacheableObjectXmlWrapper), CacheableObjectXmlWrapper.Create, typeof(CacheableObjectXml));
     


      #endregion
    }

    public static void RegisterBuiltinsJavaHashCode(long dateTime)
    {
      ConstantDateTime = dateTime;
      CacheableWrapperFactory.ClearStaticVaraiables();
      #region Cacheable keys

      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableBoolean,
        typeof(CacheableBooleanWrapper), CacheableBooleanWrapper.Create, typeof(bool));
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableByte,
        typeof(CacheableByteWrapper), CacheableByteWrapper.Create, typeof(sbyte));
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableCharacter,
        typeof(CacheableCharacterWrapper), CacheableCharacterWrapper.Create, typeof(Char));
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableDate,
        typeof(CacheableDateWrapper), CacheableDateWrapper.Create, typeof(DateTime));
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableDouble,
        typeof(CacheableDoubleWrapper), CacheableDoubleWrapper.Create, typeof(Double));
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableFileName,
        typeof(CacheableFileNameWrapper), CacheableFileNameWrapper.Create, typeof(CacheableFileName));
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableFloat,
        typeof(CacheableFloatWrapper), CacheableFloatWrapper.Create, typeof(float));
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableInt16,
        typeof(CacheableInt16Wrapper), CacheableInt16Wrapper.Create, typeof(Int16));
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableInt32,
        typeof(CacheableInt32Wrapper), CacheableInt32Wrapper.Create, typeof(Int32));
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableInt64,
        typeof(CacheableInt64Wrapper), CacheableInt64Wrapper.Create, typeof(Int64));
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableASCIIString,
        typeof(CacheableStringWrapper), CacheableStringWrapper.Create, typeof(string));
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableString,
        typeof(CacheableUnicodeStringWrapper), CacheableUnicodeStringWrapper.Create);
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableASCIIStringHuge,
        typeof(CacheableHugeStringWrapper), CacheableHugeStringWrapper.Create);
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableStringHuge,
        typeof(CacheableHugeUnicodeStringWrapper), CacheableHugeUnicodeStringWrapper.Create);
      
      //need to register pdx type
      Serializable.RegisterPdxType(PdxCData.CreateDeserializable);

      #endregion

      #region Other cacheables

      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableBytes,
        typeof(CacheableBytesWrapper), CacheableBytesWrapper.Create, typeof(byte[]));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableDoubleArray,
        typeof(CacheableDoubleArrayWrapper), CacheableDoubleArrayWrapper.Create, typeof(Double[]));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableFloatArray,
        typeof(CacheableFloatArrayWrapper), CacheableFloatArrayWrapper.Create, typeof(float[]));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableHashMap,
        typeof(CacheableHashMapWrapper), CacheableHashMapWrapper.Create, typeof(Dictionary<object, object>));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableHashTable,
        typeof(CacheableHashTableWrapper), CacheableHashTableWrapper.Create, typeof(System.Collections.Hashtable));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableIdentityHashMap,
        typeof(CacheableIdentityHashMapWrapper), CacheableIdentityHashMapWrapper.Create, typeof(Dictionary<object, object>));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableHashSet,
        typeof(CacheableHashSetWrapper), CacheableHashSetWrapper.Create, typeof(CacheableHashSet));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableLinkedHashSet,
        typeof(CacheableLinkedHashSetWrapper), CacheableLinkedHashSetWrapper.Create, typeof(CacheableLinkedHashSet));

      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableInt16Array,
        typeof(CacheableInt16ArrayWrapper), CacheableInt16ArrayWrapper.Create, typeof(Int16[]));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableInt32Array,
        typeof(CacheableInt32ArrayWrapper), CacheableInt32ArrayWrapper.Create, typeof(Int32[]));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableInt64Array,
        typeof(CacheableInt64ArrayWrapper), CacheableInt64ArrayWrapper.Create, typeof(Int64[]));
      {//old one
        //CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableNullString,
        //  typeof(CacheableNullStringWrapper), CacheableNullStringWrapper.Create);
        //CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableASCIIString,
        //  typeof(CacheableEmptyStringWrapper), CacheableEmptyStringWrapper.Create);
        //CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableString,
        //  typeof(CacheableEmptyUnicodeStringWrapper), CacheableEmptyUnicodeStringWrapper.Create);
      }
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableStringArray,
        typeof(CacheableStringArrayWrapper), CacheableStringArrayWrapper.Create, typeof(string[]));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableUndefined,
        typeof(CacheableUndefinedWrapper), CacheableUndefinedWrapper.Create, typeof(CacheableUndefined));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableVector,
        typeof(CacheableVectorWrapper), CacheableVectorWrapper.Create, typeof(System.Collections.ArrayList));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableObjectArray,
        typeof(CacheableObjectArrayWrapper), CacheableObjectArrayWrapper.Create, typeof(CacheableObjectArray));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableArrayList,
        typeof(CacheableArrayListWrapper), CacheableArrayListWrapper.Create, typeof(List<object>));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableStack,
        typeof(CacheableStackWrapper), CacheableStackWrapper.Create, typeof(Stack<object>));

      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableManagedObject,
        typeof(CacheableObjectWrapper), CacheableObjectWrapper.Create, typeof(CacheableObject));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableManagedObjectXml,
        typeof(CacheableObjectXmlWrapper), CacheableObjectXmlWrapper.Create, typeof(CacheableObjectXml));



      #endregion
    }

    public static void RegisterBuiltinsAD(long dateTime)
    {
      ConstantDateTime = dateTime;
      CacheableWrapperFactory.ClearStaticVaraiables();
      #region Cacheable keys

      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableBoolean,
        typeof(CacheableBooleanWrapper), CacheableBooleanWrapper.Create, typeof(bool));
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableByte,
        typeof(CacheableByteWrapper), CacheableByteWrapper.Create, typeof(sbyte));
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableCharacter,
        typeof(CacheableCharacterWrapper), CacheableCharacterWrapper.Create, typeof(Char));
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableDate,
        typeof(CacheableDateWrapper), CacheableDateWrapper.Create, typeof(DateTime));
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableDouble,
        typeof(CacheableDoubleWrapper), CacheableDoubleWrapper.Create, typeof(Double));
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableFileName,
        typeof(CacheableFileNameWrapper), CacheableFileNameWrapper.Create, typeof(CacheableFileName));
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableFloat,
        typeof(CacheableFloatWrapper), CacheableFloatWrapper.Create, typeof(float));
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableInt16,
        typeof(CacheableInt16Wrapper), CacheableInt16Wrapper.Create, typeof(Int16));
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableInt32,
        typeof(CacheableInt32Wrapper), CacheableInt32Wrapper.Create, typeof(Int32));
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableInt64,
        typeof(CacheableInt64Wrapper), CacheableInt64Wrapper.Create, typeof(Int64));
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableASCIIString,
        typeof(CacheableStringWrapper), CacheableStringWrapper.Create, typeof(string));
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableString,
        typeof(CacheableUnicodeStringWrapper), CacheableUnicodeStringWrapper.Create);
      {
        //CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableASCIIStringHuge,
        //  typeof(CacheableHugeStringWrapper), CacheableHugeStringWrapper.Create);
        //CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableStringHuge,
        //  typeof(CacheableHugeUnicodeStringWrapper), CacheableHugeUnicodeStringWrapper.Create);
      }
      CacheableWrapperFactory.RegisterKeyType(10001,
     typeof(PdxCDataWrapper), PdxCDataWrapper.Create, typeof(PdxCData));

      //need to register pdx type
      Serializable.RegisterPdxType(PdxCData.CreateDeserializable);


      #endregion

      #region Other cacheables

      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableBytes,
        typeof(CacheableBytesWrapper), CacheableBytesWrapper.Create, typeof(byte[]));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableDoubleArray,
        typeof(CacheableDoubleArrayWrapper), CacheableDoubleArrayWrapper.Create, typeof(Double[]));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableFloatArray,
        typeof(CacheableFloatArrayWrapper), CacheableFloatArrayWrapper.Create, typeof(float[]));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableHashMap,
        typeof(CacheableHashMapWrapper), CacheableHashMapWrapper.Create, typeof(Dictionary<object, object>));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableHashTable,
        typeof(CacheableHashTableWrapper), CacheableHashTableWrapper.Create, typeof(System.Collections.Hashtable));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableIdentityHashMap,
        typeof(CacheableIdentityHashMapWrapper), CacheableIdentityHashMapWrapper.Create, typeof(Dictionary<object, object>));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableHashSet,
        typeof(CacheableHashSetWrapper), CacheableHashSetWrapper.Create, typeof(CacheableHashSet));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableLinkedHashSet,
        typeof(CacheableLinkedHashSetWrapper), CacheableLinkedHashSetWrapper.Create, typeof(CacheableLinkedHashSet));

      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableInt16Array,
        typeof(CacheableInt16ArrayWrapper), CacheableInt16ArrayWrapper.Create, typeof(Int16[]));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableInt32Array,
        typeof(CacheableInt32ArrayWrapper), CacheableInt32ArrayWrapper.Create, typeof(Int32[]));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableInt64Array,
        typeof(CacheableInt64ArrayWrapper), CacheableInt64ArrayWrapper.Create, typeof(Int64[]));
      {//old one
        //CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableNullString,
        //  typeof(CacheableNullStringWrapper), CacheableNullStringWrapper.Create);
        //CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableASCIIString,
        //  typeof(CacheableEmptyStringWrapper), CacheableEmptyStringWrapper.Create);
        //CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableString,
        //  typeof(CacheableEmptyUnicodeStringWrapper), CacheableEmptyUnicodeStringWrapper.Create);
      }
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableStringArray,
        typeof(CacheableStringArrayWrapper), CacheableStringArrayWrapper.Create, typeof(string[]));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableUndefined,
        typeof(CacheableUndefinedWrapper), CacheableUndefinedWrapper.Create, typeof(CacheableUndefined));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableVector,
        typeof(CacheableVectorWrapper), CacheableVectorWrapper.Create, typeof(System.Collections.ArrayList));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableObjectArray,
        typeof(CacheableObjectArrayWrapper), CacheableObjectArrayWrapper.Create, typeof(CacheableObjectArray));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableArrayList,
        typeof(CacheableArrayListWrapper), CacheableArrayListWrapper.Create, typeof(List<object>));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableStack,
        typeof(CacheableStackWrapper), CacheableStackWrapper.Create, typeof(Stack<object>));

      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableManagedObject,
        typeof(CacheableObjectWrapper), CacheableObjectWrapper.Create, typeof(CacheableObject));
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableManagedObjectXml,
        typeof(CacheableObjectXmlWrapper), CacheableObjectXmlWrapper.Create, typeof(CacheableObjectXml));


      #endregion
    }
  }

  #endregion

  #region Builtin cacheable keys

  public class CacheableBooleanWrapper : CacheableKeyWrapper
  {
    #region Static factory creation function

    public static CacheableKeyWrapper Create()
    {
      return new CacheableBooleanWrapper();
    }

    #endregion

    #region CacheableKeyWrapper Members

    public override int MaxKeys
    {
      get
      {
        return 2;
      }
    }

    public override void InitKey(int keyIndex, int maxSize)
    {
      bool value = (keyIndex % 2 == 1 ? true : false);
      m_cacheableObject = value;
    }

    public override void InitRandomValue(int maxSize)
    {
      bool value = (Util.Rand(byte.MaxValue) % 2 == 1 ? true : false);
      m_cacheableObject = value;
    }

    public override uint GetChecksum(object cacheableObject)
    {
      //Util.Log("cacheableObject Type = {0}", cacheableObject.GetType());
      bool value = (bool)cacheableObject;
      Assert.IsNotNull(value, "GetChecksum: Null object.");
      return (uint)(value ? 1 : 0);
    }

    public override int GetHashCodeN(object cacheableObject)
    {
      bool val = (bool)cacheableObject;
      if (val) return 1231;
      else return 1237;
    }

    #endregion
  }

  public class CacheableByteWrapper : CacheableKeyWrapper
  {
    #region Static factory creation function

    public static CacheableKeyWrapper Create()
    {
      return new CacheableByteWrapper();
    }

    #endregion

    #region CacheableKeyWrapper Members

    public override int MaxKeys
    {
      get
      {
        return byte.MaxValue;
      }
    }

    public override void InitKey(int keyIndex, int maxSize)
    {
      sbyte value = (sbyte)keyIndex;
      m_cacheableObject = value;
    }

    public override void InitRandomValue(int maxSize)
    {
      sbyte value = (sbyte)Util.Rand(byte.MaxValue);
      m_cacheableObject = value;
    }

    public override uint GetChecksum(object cacheableObject)
    {
      sbyte value = (sbyte)cacheableObject;
      Assert.IsNotNull(value, "GetChecksum: Null object.");
      byte[] buffer = new byte[1];
      buffer[0] = (byte)value;
      return CacheableHelper.CRC32(buffer);
    }

    public override int GetHashCodeN(object cacheableObject)
    {
      sbyte val = (sbyte)cacheableObject;
      return val;
    }

    #endregion
  }

  public class CacheableCharacterWrapper : CacheableKeyWrapper
  {
    #region Static factory creation function

    public static CacheableKeyWrapper Create()
    {
      return new CacheableCharacterWrapper();
    }

    #endregion

    #region CacheableKeyWrapper Members

    public override int MaxKeys
    {
      get
      {
        return char.MaxValue;
      }
    }

    public override void InitKey(int keyIndex, int maxSize)
    {
      char value = (char)keyIndex;
      m_cacheableObject = value;
    }

    public override void InitRandomValue(int maxSize)
    {
      char value = (char)Util.Rand(char.MaxValue);
      m_cacheableObject = value;
    }

    public override uint GetChecksum(object cacheableObject)
    {
      char obj = (char)cacheableObject;
      Assert.IsNotNull(obj, "GetChecksum: Null object.");
      char value = obj;
      int numBytes = sizeof(char);
      byte[] buffer = new byte[numBytes];
      for (int i = 0; i < numBytes; i++)
      {
        buffer[i] = (byte)(value & 0xff);
        value = (char)(value >> 8);
      }
      return CacheableHelper.CRC32(buffer);
    }

    public override int GetHashCodeN(object cacheableObject)
    {
      char val = (char)cacheableObject;
      return val.GetHashCode();
    }
    #endregion
  }

  public class CacheableDateWrapper : CacheableKeyWrapper
  {
    #region Static factory creation function

    public static CacheableKeyWrapper Create()
    {
      return new CacheableDateWrapper();
    }

    #endregion

    #region CacheableKeyWrapper Members

    public override int MaxKeys
    {
      get
      {
        return int.MaxValue;
      }
    }

    public override void InitKey(int keyIndex, int maxSize)
    {
      //m_cacheableObject = DateTime.Today.AddMinutes(keyIndex % 0xFFFF);
      DateTime dt = new DateTime(CacheableHelper.ConstantDateTime + keyIndex*TimeSpan.TicksPerMinute );
      m_cacheableObject = dt;
      Util.Log(" datevalue initkey " + dt.ToString());
    }

    private DateTime createRoundOffDTVal(DateTime dt)
    { 
      long ticksToAdd = dt.Ticks % TimeSpan.TicksPerMillisecond;
        ticksToAdd = (ticksToAdd >= (TimeSpan.TicksPerMillisecond/ 2) ?
          (TimeSpan.TicksPerMillisecond - ticksToAdd) : -ticksToAdd);
        dt = dt.AddTicks(ticksToAdd);
        return dt;
    }

    public override void InitRandomValue(int maxSize)
    {
      int rnd = Util.Rand(int.MaxValue);
      //DateTime value = DateTime.Now.AddMilliseconds(rnd % 2 == 0 ? rnd : -rnd);
      DateTime value = new DateTime(CacheableHelper.ConstantDateTime);
      m_cacheableObject = value;
      Util.Log(" datevalue InitRandomValue " + value.ToString());
    }

    public override uint GetChecksum(object cacheableObject)
    {
      //CacheableDate obj = cacheableObject as CacheableDate;
      DateTime obj = (DateTime)cacheableObject;
      Util.Log(" datevalue getchecksum " + obj.ToString());
      Assert.IsNotNull(obj, "GetChecksum: Null object.");

      obj = createRoundOffDTVal(obj);
      long value = obj.Ticks;
      int numBytes = sizeof(long);
      byte[] buffer = new byte[numBytes];
      for (int i = 0; i < numBytes; i++)
      {
        buffer[i] = (byte)(value & 0xff);
        value = value >> 8;
      }
      uint cks = CacheableHelper.CRC32(buffer);
      Util.Log(" datevalue getchecksum  " + cks);
      return cks;
    }

    #endregion
  }

  public class CacheableDoubleWrapper : CacheableKeyWrapper
  {
    #region Private constants

    private const double MaxError = 1E-20;

    #endregion

    #region Static factory creation function

    public static CacheableKeyWrapper Create()
    {
      return new CacheableDoubleWrapper();
    }

    #endregion

    #region CacheableKeyWrapper Members

    public override int MaxKeys
    {
      get
      {
        return int.MaxValue;
      }
    }

    public override void InitKey(int keyIndex, int maxSize)
    {
      double value = (double)keyIndex;
      m_cacheableObject = value;
    }

    public override void InitRandomValue(int maxSize)
    {
      double value = Util.Rand() * double.MaxValue;
      m_cacheableObject = value;
    }

    public override uint GetChecksum(object cacheableObject)
    {
      double value = (double)cacheableObject;
      Assert.IsNotNull(value, "GetChecksum: Null object.");
      return CacheableHelper.CRC32(BitConverter.GetBytes(value));
    }

    public override int GetHashCodeN(object cacheableObject)
    {
      double val = (double)cacheableObject;
      return val.GetHashCode();
    }
    #endregion
  }

  public class CacheableFileNameWrapper : CacheableKeyWrapper
  {
    #region Static factory creation function

    public static CacheableKeyWrapper Create()
    {
      return new CacheableFileNameWrapper();
    }

    #endregion

    #region CacheableKeyWrapper Members

    public override int MaxKeys
    {
      get
      {
        return int.MaxValue;
      }
    }

    public override void InitKey(int keyIndex, int maxSize)
    {
      Assert.Greater(maxSize, 0, "Size of key should be greater than zero.");
      if (maxSize < 11)
      {
        maxSize = 11;
      }
      string value = "C:\\" + new string('\x0905', maxSize - 13) +
        keyIndex.ToString("D10");
      m_cacheableObject = CacheableFileName.Create(value);
    }

    public override void InitRandomValue(int maxSize)
    {
      Assert.Greater(maxSize, 0, "Size of value should be greater than zero.");
      byte[] buffer = Util.RandBytes(maxSize / 2);
      char[] value = new char[maxSize / 2];
      for (int i = 0; i < maxSize / 2; i++)
      {
        value[i] = (char)((int)buffer[i] + 0x0901);
      }
      m_cacheableObject = CacheableFileName.Create(value);
    }

    public override uint GetChecksum(object cacheableObject)
    {
      CacheableFileName obj = cacheableObject as CacheableFileName;
      Assert.IsNotNull(obj, "GetChecksum: Null object.");

      string value = obj.Value;
      byte[] buffer = new byte[value.Length * 2];
      for (int i = 0; i < value.Length; i++)
      {
        char c = value[i];
        buffer[i * 2] = (byte)(c & 0xff);
        buffer[i * 2 + 1] = (byte)((c >> 8) & 0xff);
      }
      return CacheableHelper.CRC32(buffer);
    }

    #endregion
  }

  public class CacheableFloatWrapper : CacheableKeyWrapper
  {
    #region Private constants

    private const float MaxError = 1E-10F;

    #endregion

    #region Static factory creation function

    public static CacheableKeyWrapper Create()
    {
      return new CacheableFloatWrapper();
    }

    #endregion

    #region CacheableKeyWrapper Members

    public override int MaxKeys
    {
      get
      {
        return int.MaxValue;
      }
    }

    public override void InitKey(int keyIndex, int maxSize)
    {
      float value = (float)keyIndex;
      m_cacheableObject = value;
    }

    public override void InitRandomValue(int maxSize)
    {
      float value = (float)Util.Rand() * float.MaxValue;
      m_cacheableObject = value;
    }

    public override uint GetChecksum(object cacheableObject)
    {
      float value = (float)cacheableObject;
      Assert.IsNotNull(value, "GetChecksum: Null object.");
      return CacheableHelper.CRC32(BitConverter.GetBytes(value));
    }
    
    public override int GetHashCodeN(object cacheableObject)
    {
      float val = (float)cacheableObject;
      return val.GetHashCode();
    }
    #endregion
  }

  public class CacheableInt16Wrapper : CacheableKeyWrapper
  {
    #region Static factory creation function

    public static CacheableKeyWrapper Create()
    {
      return new CacheableInt16Wrapper();
    }

    #endregion

    #region CacheableKeyWrapper Members

    public override int MaxKeys
    {
      get
      {
        return short.MaxValue;
      }
    }

    public override void InitKey(int keyIndex, int maxSize)
    {
      short value = (short)keyIndex;
      m_cacheableObject = value;
    }

    public override void InitRandomValue(int maxSize)
    {
      short value = (short)Util.Rand(short.MaxValue);
      m_cacheableObject = value;
    }

    public override uint GetChecksum(object cacheableObject)
    {
      Int16 obj = (Int16)cacheableObject;
      Assert.IsNotNull(obj, "GetChecksum: Null object.");
      short value = obj;
      int numBytes = sizeof(short);
      byte[] buffer = new byte[numBytes];
      for (int i = 0; i < numBytes; i++)
      {
        buffer[i] = (byte)(value & 0xff);
        value = (short)(value >> 8);
      }
      return CacheableHelper.CRC32(buffer);
    }

    public override int GetHashCodeN(object cacheableObject)
    {
      Int16 val = (Int16)cacheableObject;
      return val;
    }
    #endregion
  }

  public class CacheableInt32Wrapper : CacheableKeyWrapper
  {
    #region Static factory creation function

    public static CacheableKeyWrapper Create()
    {
      return new CacheableInt32Wrapper();
    }

    #endregion

    #region CacheableKeyWrapper Members

    public override int MaxKeys
    {
      get
      {
        return int.MaxValue;
      }
    }

    public override void InitKey(int keyIndex, int maxSize)
    {
      int value = keyIndex;
      m_cacheableObject = value;
    }

    public override void InitRandomValue(int maxSize)
    {
      int value = Util.Rand(int.MaxValue);
      m_cacheableObject = value;
    }

    public override uint GetChecksum(object cacheableObject)
    {
      Int32 obj = (Int32)cacheableObject;
      Assert.IsNotNull(obj, "GetChecksum: Null object.");

      int value = obj;
      int numBytes = sizeof(int);
      byte[] buffer = new byte[numBytes];
      for (int i = 0; i < numBytes; i++)
      {
        buffer[i] = (byte)(value & 0xff);
        value = value >> 8;
      }
      return CacheableHelper.CRC32(buffer);
    }

    public override int GetHashCodeN(object cacheableObject)
    {
      Int32 val = (Int32)cacheableObject;
      return val;
    }
    #endregion
  }

  public class CacheableInt64Wrapper : CacheableKeyWrapper
  {
    #region Static factory creation function

    public static CacheableKeyWrapper Create()
    {
      return new CacheableInt64Wrapper();
    }

    #endregion

    #region CacheableKeyWrapper Members

    public override int MaxKeys
    {
      get
      {
        return int.MaxValue;
      }
    }

    public override void InitKey(int keyIndex, int maxSize)
    {
      long value = (long)keyIndex;
      m_cacheableObject = value;
    }

    public override void InitRandomValue(int maxSize)
    {
      long value = Util.Rand(int.MaxValue);
      value = (value << 32) + Util.Rand(int.MaxValue);
      m_cacheableObject = value;
    }

    public override uint GetChecksum(object cacheableObject)
    {
      Int64 obj = (Int64)cacheableObject;
      Assert.IsNotNull(obj, "GetChecksum: Null object.");
      long value = obj;
      int numBytes = sizeof(long);
      byte[] buffer = new byte[numBytes];
      for (int i = 0; i < numBytes; i++)
      {
        buffer[i] = (byte)(value & 0xff);
        value = value >> 8;
      }
      return CacheableHelper.CRC32(buffer);
    }

    public override int GetHashCodeN(object cacheableObject)
    {
      Int64 val = (Int64)cacheableObject;
      return val.GetHashCode();
    }
    #endregion
  }

  public class CacheableStringWrapper : CacheableKeyWrapper
  {
    #region Static factory creation function

    public static CacheableKeyWrapper Create()
    {
      return new CacheableStringWrapper();
    }

    #endregion

    #region CacheableKeyWrapper Members

    public override int MaxKeys
    {
      get
      {
        return int.MaxValue;
      }
    }

    public override void InitKey(int keyIndex, int maxSize)
    {
      Assert.Greater(maxSize, 0, "Size of key should be greater than zero.");
      if (maxSize < 11)
      {
        maxSize = 11;
      }
      if (keyIndex == 0)
      {
        m_cacheableObject = string.Empty;
      }
      else
      {
        string value = new string('A', maxSize - 10) + keyIndex.ToString("D10");
        m_cacheableObject = value;
      }
    }

    public override void InitRandomValue(int maxSize)
    {
      Assert.Greater(maxSize, 0, "Size of value should be greater than zero.");
      maxSize = (maxSize / 2) > 1 ? (maxSize / 2) : 2;
      Util.Log(" in cacheable string wrapper maxsize = " + maxSize);
      if (maxSize == 2)
      {
        m_cacheableObject = string.Empty;
      }
      else
      {
        byte[] buffer = Util.RandBytes(maxSize);
        string value = BitConverter.ToString(buffer).Replace("-", string.Empty);
        Util.Log("cacheable string wrapper " + value);
        m_cacheableObject = value;
      }
    }

    public override uint GetChecksum(object cacheableObject)
    {
      string obj = (string)cacheableObject;
      Assert.IsNotNull(obj, "GetChecksum: Null object.");

      string value = obj;
      if (value != null)
      {
        byte[] buffer = new byte[value.Length];
        for (int i = 0; i < value.Length; i++)
        {
          buffer[i] = (byte)value[i];
        }
        return CacheableHelper.CRC32(buffer);
      }
      return 0;
    }

    public override int GetHashCodeN(object cacheableObject)
    {
      string val = (string)cacheableObject;
      return val.GetHashCode();
    }
    #endregion
  }

  public class CacheableUnicodeStringWrapper : CacheableKeyWrapper
  {
    #region Static factory creation function

    public static CacheableKeyWrapper Create()
    {
      return new CacheableUnicodeStringWrapper();
    }

    #endregion

    #region CacheableKeyWrapper Members

    public override int MaxKeys
    {
      get
      {
        return int.MaxValue;
      }
    }

    public override void InitKey(int keyIndex, int maxSize)
    {
      Assert.Greater(maxSize, 0, "Size of key should be greater than zero.");
      if (maxSize < 11)
      {
        maxSize = 11;
      }
      if (keyIndex == 0)
      {
        m_cacheableObject = string.Empty;
      }
      else
      {
        string value = new string('\x0905', maxSize - 10) + keyIndex.ToString("D10");
        m_cacheableObject = value;
      }
    }

    public override void InitRandomValue(int maxSize)
    {
      Assert.Greater(maxSize, 0, "Size of value should be greater than zero.");
      if (maxSize == 2)
      {
        m_cacheableObject = string.Empty;
      }
      else
      {
        byte[] buffer = Util.RandBytes(maxSize / 2);
        char[] value = new char[maxSize / 2];
        for (int i = 0; i < maxSize / 2; i++)
        {
          value[i] = (char)((int)buffer[i] + 0x0901);
        }
        m_cacheableObject = new string(value);
      }
    }

    public override uint GetChecksum(object cacheableObject)
    {
      string obj = (string)cacheableObject;
      Assert.IsNotNull(obj, "GetChecksum: Null object.");

      string value = obj;
      byte[] buffer = new byte[value.Length * 2];
      for (int i = 0; i < value.Length; i++)
      {
        char c = value[i];
        buffer[i * 2] = (byte)(c & 0xff);
        buffer[i * 2 + 1] = (byte)((c >> 8) & 0xff);
      }
      return CacheableHelper.CRC32(buffer);
    }

    public override int GetHashCodeN(object cacheableObject)
    {
      string val = (string)cacheableObject;
      return val.GetHashCode();
    }
    #endregion
  }

  public class CacheableHugeStringWrapper : CacheableKeyWrapper
  {
    #region Static factory creation function

    public static CacheableKeyWrapper Create()
    {
      return new CacheableHugeStringWrapper();
    }

    #endregion

    #region CacheableKeyWrapper Members

    public override int MaxKeys
    {
      get
      {
        return int.MaxValue;
      }
    }

    public override void InitKey(int keyIndex, int maxSize)
    {
      Assert.Greater(maxSize, 0, "Size of key should be greater than zero.");
      if (maxSize < 0xFFFF)
      {
        maxSize += 0xFFFF + 1;
      }
      if (keyIndex == 0)
      {
        m_cacheableObject = string.Empty;
      }
      else
      {
        string value = new string('A', maxSize - 10) + keyIndex.ToString("D10");
        m_cacheableObject = value;
      }
    }

    public override void InitRandomValue(int maxSize)
    {
      if (maxSize < 0xFFFF)
      {
        maxSize += 0xFFFF + 1;
      }
      Assert.Greater(maxSize, 0, "Size of value should be greater than zero.");
      if (maxSize < 0xFFFF + 10)
      {
        m_cacheableObject = string.Empty;
      }
      else
      {
        byte[] buffer = Util.RandBytes(maxSize);
        string value = BitConverter.ToString(buffer).Replace("-", string.Empty);
        m_cacheableObject = value;
      }
    }

    public override uint GetChecksum(object cacheableObject)
    {
      string obj = (string)cacheableObject;
      Assert.IsNotNull(obj, "GetChecksum: Null object.");

      string value = obj;
      byte[] buffer = new byte[value.Length];
      for (int i = 0; i < value.Length; i++)
      {
        buffer[i] = (byte)value[i];
      }
      return CacheableHelper.CRC32(buffer);
    }

    public override int GetHashCodeN(object cacheableObject)
    {
      string val = (string)cacheableObject;
      return val.GetHashCode();
    }
    #endregion
  }

  public class CacheableHugeUnicodeStringWrapper : CacheableKeyWrapper
  {
    #region Static factory creation function

    public static CacheableKeyWrapper Create()
    {
      return new CacheableHugeUnicodeStringWrapper();
    }

    #endregion

    #region CacheableKeyWrapper Members

    public override int MaxKeys
    {
      get
      {
        return int.MaxValue;
      }
    }

    public override void InitKey(int keyIndex, int maxSize)
    {
      Assert.Greater(maxSize, 0, "Size of key should be greater than zero.");
      if (maxSize < 0xFFFF)
      {
        maxSize += 0xFFFF + 1;
      }
      if (keyIndex == 0)
      {
        m_cacheableObject = string.Empty;
      }
      else
      {
        string value = new string('\x0905', maxSize - 10) + keyIndex.ToString("D10");
        m_cacheableObject = value;
      }
    }

    public override void InitRandomValue(int maxSize)
    {
      if (maxSize < 0xFFFF)
      {
        maxSize += 0xFFFF + 1;
      }
      Assert.Greater(maxSize, 0, "Size of value should be greater than zero.");
      if (maxSize < 0xFFFF + 10)
      {
        m_cacheableObject = string.Empty;
      }
      else
      {
        byte[] buffer = Util.RandBytes(maxSize);
        char[] value = new char[maxSize];
        for (int i = 0; i < maxSize; i++)
        {
          value[i] = (char)((int)buffer[i] + 0x0901);
        }
        m_cacheableObject = new string(value);
      }
    }

    public override uint GetChecksum(object cacheableObject)
    {
      string obj = (string)cacheableObject;
      Assert.IsNotNull(obj, "GetChecksum: Null object.");

      string value = obj;
      byte[] buffer = new byte[value.Length * 2];
      for (int i = 0; i < value.Length; i++)
      {
        char c = value[i];
        buffer[i * 2] = (byte)(c & 0xff);
        buffer[i * 2 + 1] = (byte)((c >> 8) & 0xff);
      }
      return CacheableHelper.CRC32(buffer);
    }

    public override int GetHashCodeN(object cacheableObject)
    {
      string val = (string)cacheableObject;
      return val.GetHashCode();
    }
    #endregion
  }

  #endregion

  #region Builtin cacheables that are not keys

  public class CacheableBytesWrapper : CacheableWrapper
  {
    #region Static factory creation function

    public static CacheableWrapper Create()
    {
      return new CacheableBytesWrapper();
    }

    #endregion

    #region CacheableWrapper Members

    public override void InitRandomValue(int maxSize)
    {
      byte[] value = Util.RandBytes(maxSize);
      m_cacheableObject = value;
    }

    public override uint GetChecksum(object cacheableObject)
    {
      byte[] value = (byte[])cacheableObject;
      Assert.IsNotNull(value, "GetChecksum: Null object.");

      return CacheableHelper.CRC32(value);
    }
    public override int GetHashCodeN(object cacheableObject)
    {
      byte[] val = (byte[])cacheableObject;
      return val.GetHashCode();
    }

    #endregion
  }

  public class CacheableDoubleArrayWrapper : CacheableWrapper
  {
    #region Static factory creation function

    public static CacheableWrapper Create()
    {
      return new CacheableDoubleArrayWrapper();
    }

    #endregion

    #region CacheableWrapper Members

    public override void InitRandomValue(int maxSize)
    {
      int arraySize = maxSize / sizeof(double);
      arraySize = arraySize > 1 ? arraySize : 2;
      double[] value = new double[arraySize];
      for (int arrayIndex = 0; arrayIndex < arraySize; arrayIndex++)
      {
        value[arrayIndex] = Util.Rand() * double.MaxValue;
      }
      m_cacheableObject = value;
    }

    public override uint GetChecksum(object cacheableObject)
    {
      Double[] obj = cacheableObject as Double[];
      Assert.IsNotNull(obj, "GetChecksum: Null object.");

      double[] value = obj;
      System.IO.MemoryStream ms = new System.IO.MemoryStream();
      foreach (double v in value)
      {
        byte[] buffer = BitConverter.GetBytes(v);
        ms.Write(buffer, 0, buffer.Length);
      }
      return CacheableHelper.CRC32(ms.ToArray());
    }

    public override int GetHashCodeN(object cacheableObject)
    {
      double[] val = (double[])cacheableObject;
      return val.GetHashCode();
    }
    #endregion
  }

  public class CacheableFloatArrayWrapper : CacheableWrapper
  {
    #region Static factory creation function

    public static CacheableWrapper Create()
    {
      return new CacheableFloatArrayWrapper();
    }

    #endregion

    #region CacheableWrapper Members

    public override void InitRandomValue(int maxSize)
    {
      int arraySize = maxSize / sizeof(float);
      arraySize = arraySize > 1 ? arraySize : 2;
      float[] value = new float[arraySize];
      for (int arrayIndex = 0; arrayIndex < arraySize; arrayIndex++)
      {
        value[arrayIndex] = (float)Util.Rand() * float.MaxValue;
      }
      m_cacheableObject = value;
    }

    public override uint GetChecksum(object cacheableObject)
    {
      float[] obj = cacheableObject as float[];
      Assert.IsNotNull(obj, "GetChecksum: Null object.");

      float[] value = obj;
      System.IO.MemoryStream ms = new System.IO.MemoryStream();
      foreach (float v in value)
      {
        byte[] buffer = BitConverter.GetBytes(v);
        ms.Write(buffer, 0, buffer.Length);
      }
      return CacheableHelper.CRC32(ms.ToArray());
    }

    public override int GetHashCodeN(object cacheableObject)
    {
      float[] val = (float[])cacheableObject;
      return val.GetHashCode();
    }
    #endregion
  }

  public class CacheableHashMapWrapper : CacheableWrapper
  {
    #region Static factory creation function

    public static CacheableWrapper Create()
    {
      return new CacheableHashMapWrapper();
    }

    #endregion

    #region CacheableWrapper Members

    public override void InitRandomValue(int maxSize)
    {
      //CacheableHashMap map = new CacheableHashMap();
      Dictionary<object, object> map = new Dictionary<object, object>();
      ICollection<UInt32> keyTypeIds =
        CacheableWrapperFactory.GetRegisteredKeyTypeIds();
      ICollection<UInt32> valueTypeIds =
        CacheableWrapperFactory.GetRegisteredValueTypeIds();
      int keySize = 16;
      maxSize = maxSize / (keyTypeIds.Count * valueTypeIds.Count) + 1;
      CacheableKeyWrapper keyWrapper;
      CacheableWrapper valueWrapper;
      foreach (UInt32 keyTypeId in keyTypeIds)
      {
        int index = 0;
        foreach (UInt32 valueTypeId in valueTypeIds)
        {
          if ((valueTypeId == GemFireClassIds.CacheableASCIIStringHuge ||
            valueTypeId == GemFireClassIds.CacheableStringHuge)
              && !(keyTypeId == GemFireClassIds.CacheableBoolean))
          {
            continue;
          }
          if ((keyTypeId == GemFireClassIds.CacheableASCIIStringHuge ||
            keyTypeId == GemFireClassIds.CacheableStringHuge)
              && !(valueTypeId == GemFireClassIds.CacheableBoolean))
          {
            continue;
          }
          // null object does not work on server side during deserialization
          if (valueTypeId == GemFireClassIds.CacheableNullString)
          {
            continue;
          }

          keyWrapper = CacheableWrapperFactory.CreateKeyInstance(keyTypeId);
          Assert.IsNotNull(keyWrapper, "CacheableHashMapWrapper.InitRandomValue:" +
            " Could not create an instance of typeId [{0}].", keyTypeId);
          if (keyWrapper.MaxKeys <= index)
          {
            break;
          }
          keyWrapper.InitKey((int)(keyTypeId << 8) + index, keySize);
          if (!CacheableHelper.IsContainerTypeId(valueTypeId) &&
            !CacheableHelper.IsUnhandledType(valueTypeId))
          {
            valueWrapper = CacheableWrapperFactory.CreateInstance(valueTypeId);
            Assert.IsNotNull(valueWrapper, "CacheableHashMapWrapper.InitRandomValue:" +
              " Could not create an instance of typeId [{0}].", valueTypeId);
            valueWrapper.InitRandomValue(maxSize);
            Util.Log(" updating map " + keyWrapper.CacheableKey + " : " + valueWrapper.Cacheable);
            map.Add(keyWrapper.CacheableKey, valueWrapper.Cacheable);
          }
          index++;
        }
      }
      m_cacheableObject = map;
    }

    public override uint GetChecksum(object cacheableObject)
    {
      //CacheableHashMap value = cacheableObject as CacheableHashMap;
      Dictionary<object, object> value = cacheableObject as Dictionary<object, object>;
      Assert.IsNotNull(value, "GetChecksum: Null object.");

      uint ckSum = 0;
      CacheableKeyWrapper keyWrapper;
      CacheableWrapper valueWrapper;
      foreach (KeyValuePair<object, object> pair in value)
      {
        Util.Log(" pair " + pair.Key.GetType() + " : " + pair.Value.GetType());
        keyWrapper = CacheableWrapperFactory.CreateKeyInstance((pair.Key));
        Assert.IsNotNull(keyWrapper, "CacheableHashMap.GetChecksum:" +
          " Could not create an instance of typeId [{0}].",
          keyWrapper.TypeId);
        valueWrapper = CacheableWrapperFactory.CreateInstance(
          pair.Value);
        Assert.IsNotNull(valueWrapper, "CacheableHashMap.GetChecksum:" +
          " Could not create an instance of typeId [{0}].",
          valueWrapper.TypeId);

        ckSum ^= (keyWrapper.GetChecksum(pair.Key) ^
          valueWrapper.GetChecksum(pair.Value));
      }
      return ckSum;
    }

    #endregion
  }

  public class CacheableHashTableWrapper : CacheableWrapper
  {
    #region Static factory creation function

    public static CacheableWrapper Create()
    {
      return new CacheableHashTableWrapper();
    }

    #endregion

    #region CacheableWrapper Members

    public override void InitRandomValue(int maxSize)
    {
      //CacheableHashTable table = new CacheableHashTable();
      System.Collections.Hashtable table = new System.Collections.Hashtable();
      ICollection<UInt32> keyTypeIds =
        CacheableWrapperFactory.GetRegisteredKeyTypeIds();
      ICollection<UInt32> valueTypeIds =
        CacheableWrapperFactory.GetRegisteredValueTypeIds();
      int keySize = 16;
      maxSize = maxSize / (keyTypeIds.Count * valueTypeIds.Count) + 1;
      CacheableKeyWrapper keyWrapper;
      CacheableWrapper valueWrapper;
      foreach (UInt32 keyTypeId in keyTypeIds)
      {
        int index = 0;
        foreach (UInt32 valueTypeId in valueTypeIds)
        {
          if ((valueTypeId == GemFireClassIds.CacheableASCIIStringHuge ||
            valueTypeId == GemFireClassIds.CacheableStringHuge)
            && !(keyTypeId == GemFireClassIds.CacheableBoolean))
          {
            continue;
          }
          if ((keyTypeId == GemFireClassIds.CacheableASCIIStringHuge ||
            keyTypeId == GemFireClassIds.CacheableStringHuge)
            && !(valueTypeId == GemFireClassIds.CacheableBoolean))
          {
            continue;
          }
          if (valueTypeId == GemFireClassIds.CacheableNullString)
          {
            continue;
          }

          keyWrapper = CacheableWrapperFactory.CreateKeyInstance(keyTypeId);
          Assert.IsNotNull(keyWrapper, "CacheableHashTableWrapper.InitRandomValue:" +
            " Could not create an instance of typeId [{0}].", keyTypeId);
          if (keyWrapper.MaxKeys <= index)
          {
            break;
          }
          keyWrapper.InitKey((int)(keyTypeId << 8) + index, keySize);

          if (!CacheableHelper.IsContainerTypeId(valueTypeId) &&
              !CacheableHelper.IsUnhandledType(valueTypeId))
          {
            valueWrapper = CacheableWrapperFactory.CreateInstance(valueTypeId);
            Assert.IsNotNull(valueWrapper, "CacheableHashTableWrapper.InitRandomValue:" +
              " Could not create an instance of typeId [{0}].", valueTypeId);
            valueWrapper.InitRandomValue(maxSize);
            if (valueWrapper.Cacheable == null)
            {
              Util.Log(" adding null value " + valueWrapper.GetType() + " : " + valueTypeId);
            }
            else
              Util.Log(" adding value " + valueWrapper.GetType() + " : " + valueTypeId);
            table.Add(keyWrapper.CacheableKey, valueWrapper.Cacheable);
          }
          index++;
        }
      }
      m_cacheableObject = table;
    }

    public override uint GetChecksum(object cacheableObject)
    {
      //Dictionary<object, object> value = new Dictionary<object, object>();
      System.Collections.Hashtable value = cacheableObject as System.Collections.Hashtable;
      Assert.IsNotNull(value, "GetChecksum: Null object.");

      uint ckSum = 0;
      CacheableKeyWrapper keyWrapper;
      CacheableWrapper valueWrapper;
      foreach (System.Collections.DictionaryEntry pair in value)
      {
        keyWrapper = CacheableWrapperFactory.CreateKeyInstance(
          pair.Key);
        Assert.IsNotNull(keyWrapper, "CacheableHashTable.GetChecksum:" +
          " Could not create an instance of typeId [{0}].",
          keyWrapper.TypeId);
        valueWrapper = CacheableWrapperFactory.CreateInstance(
          pair.Value);
        Assert.IsNotNull(valueWrapper, "CacheableHashTable.GetChecksum:" +
          " Could not create an instance of typeId [{0}].",
          valueWrapper.TypeId);

        ckSum ^= (keyWrapper.GetChecksum(pair.Key) ^
          valueWrapper.GetChecksum(pair.Value));
      }
      return ckSum;
    }

    #endregion
  }

  public class CacheableIdentityHashMapWrapper : CacheableWrapper
  {
    #region Static factory creation function

    public static CacheableWrapper Create()
    {
      return new CacheableIdentityHashMapWrapper();
    }

    #endregion

    #region CacheableWrapper Members

    public override void InitRandomValue(int maxSize)
    {
      //CacheableIdentityHashMap map = new CacheableIdentityHashMap();
      Dictionary<object, object> map = new Dictionary<object, object>();
      ICollection<UInt32> keyTypeIds =
        CacheableWrapperFactory.GetRegisteredKeyTypeIds();
      ICollection<UInt32> valueTypeIds =
        CacheableWrapperFactory.GetRegisteredValueTypeIds();
      int keySize = 16;
      maxSize = maxSize / (keyTypeIds.Count * valueTypeIds.Count) + 1;
      CacheableKeyWrapper keyWrapper;
      CacheableWrapper valueWrapper;
      foreach (UInt32 keyTypeId in keyTypeIds)
      {
        int index = 0;
        foreach (UInt32 valueTypeId in valueTypeIds)
        {
          if ((valueTypeId == GemFireClassIds.CacheableASCIIStringHuge ||
            valueTypeId == GemFireClassIds.CacheableStringHuge)
            && !(keyTypeId == GemFireClassIds.CacheableBoolean))
          {
            continue;
          }
          if ((keyTypeId == GemFireClassIds.CacheableASCIIStringHuge ||
            keyTypeId == GemFireClassIds.CacheableStringHuge)
            && !(valueTypeId == GemFireClassIds.CacheableBoolean))
          {
            continue;
          }
          // null object does not work on server side during deserialization
          if (valueTypeId == GemFireClassIds.CacheableNullString)
          {
            continue;
          }

          keyWrapper = CacheableWrapperFactory.CreateKeyInstance(keyTypeId);
          Assert.IsNotNull(keyWrapper, "CacheableIdentityHashMapWrapper.InitRandomValue:" +
            " Could not create an instance of typeId [{0}].", keyTypeId);
          if (keyWrapper.MaxKeys <= index)
          {
            break;
          }
          keyWrapper.InitKey((int)(keyTypeId << 8) + index, keySize);

          if (!CacheableHelper.IsContainerTypeId(valueTypeId) &&
              !CacheableHelper.IsUnhandledType(valueTypeId))
          {
            valueWrapper = CacheableWrapperFactory.CreateInstance(valueTypeId);
            Assert.IsNotNull(valueWrapper, "CacheableIdentityHashMapWrapper.InitRandomValue:" +
              " Could not create an instance of typeId [{0}].", valueTypeId);
            valueWrapper.InitRandomValue(maxSize);
            map.Add(keyWrapper.CacheableKey, valueWrapper.Cacheable);
          }
          index++;
        }
      }
      m_cacheableObject = map;
    }

    public override uint GetChecksum(object cacheableObject)
    {

      //CacheableHashMap value = cacheableObject as CacheableHashMap;
      Dictionary<object, object> value = new Dictionary<object, object>();
      Assert.IsNotNull(value, "GetChecksum: Null object.");

      uint ckSum = 0;
      CacheableKeyWrapper keyWrapper;
      CacheableWrapper valueWrapper;
      foreach (KeyValuePair<object, object> pair in value)
      {
        keyWrapper = CacheableWrapperFactory.CreateKeyInstance(
          pair.Key);
        Assert.IsNotNull(keyWrapper, "CacheableIdentityHashMap.GetChecksum:" +
          " Could not create an instance of typeId [{0}].",
          keyWrapper.TypeId);
        valueWrapper = CacheableWrapperFactory.CreateInstance(
          pair.Value);
        Assert.IsNotNull(valueWrapper, "CacheableIdentityHashMap.GetChecksum:" +
          " Could not create an instance of typeId [{0}].",
          valueWrapper.TypeId);

        ckSum ^= (keyWrapper.GetChecksum(pair.Key) ^
          valueWrapper.GetChecksum(pair.Value));
      }
      return ckSum;
    }

    #endregion
  }

  public class CacheableHashSetWrapper : CacheableWrapper
  {
    #region Static factory creation function

    public static CacheableWrapper Create()
    {
      return new CacheableHashSetWrapper();
    }

    #endregion

    #region CacheableWrapper Members

    public override void InitRandomValue(int maxSize)
    {
      CacheableHashSet set = new CacheableHashSet();
      //List<object> set = new List<object>();

      //Dictionary set = new Dictionary();
      ICollection<UInt32> keyTypeIds =
        CacheableWrapperFactory.GetRegisteredKeyTypeIds();
      maxSize = maxSize / keyTypeIds.Count + 1;
      CacheableKeyWrapper wrapper;
      int keyIndex = 0;
      foreach (UInt32 typeId in keyTypeIds)
      {
        if (!CacheableHelper.IsContainerTypeId(typeId) &&
          !CacheableHelper.IsUnhandledType(typeId))
        {
          wrapper = CacheableWrapperFactory.CreateKeyInstance(typeId);
          Assert.IsNotNull(wrapper, "CacheableHashSetWrapper.InitRandomValue:" +
              " Could not create an instance of typeId [{0}].", typeId);
          wrapper.InitKey(keyIndex++, maxSize);


          set.Add(wrapper.CacheableKey);
        }
      }
      m_cacheableObject = set;
    }

    public override uint GetChecksum(object cacheableObject)
    {
      CacheableHashSet value = cacheableObject as CacheableHashSet;
      //List<object> value = cacheableObject as List<object>;
      Assert.IsNotNull(value, "GetChecksum: Null object.");

      uint ckSum = 0;
      CacheableKeyWrapper wrapper;
      foreach (object key in value)
      {
        wrapper = CacheableWrapperFactory.CreateKeyInstance(key);
        Assert.IsNotNull(wrapper, "CacheableHashSet.GetChecksum:" +
          " Could not create an instance of typeId [{0}].", wrapper.TypeId);
        ckSum ^= wrapper.GetChecksum(key);
      }
      return ckSum;
    }

    public override int GetHashCodeN(object cacheableObject)
    {
      CacheableHashSet value = cacheableObject as CacheableHashSet;

      return value.GetHashCode();
    }
    #endregion
  }

  public class CacheableLinkedHashSetWrapper : CacheableWrapper
  {
    #region Static factory creation function

    public static CacheableWrapper Create()
    {
      return new CacheableLinkedHashSetWrapper();
    }

    #endregion

    #region CacheableWrapper Members

    public override void InitRandomValue(int maxSize)
    {
      CacheableLinkedHashSet set = new CacheableLinkedHashSet();
      //Dictionary<object, object> set = new Dictionary<object, object>();
      //List<object> set = new List<object>();
      ICollection<UInt32> keyTypeIds =
        CacheableWrapperFactory.GetRegisteredKeyTypeIds();
      maxSize = maxSize / keyTypeIds.Count + 1;
      CacheableKeyWrapper wrapper;
      int keyIndex = 0;
      foreach (UInt32 typeId in keyTypeIds)
      {
        if (!CacheableHelper.IsContainerTypeId(typeId) &&
          !CacheableHelper.IsUnhandledType(typeId))
        {
          wrapper = CacheableWrapperFactory.CreateKeyInstance(typeId);
          Assert.IsNotNull(wrapper, "CacheableLinkedHashSetWrapper.InitRandomValue:" +
            " Could not create an instance of typeId [{0}].", typeId);
          wrapper.InitKey(keyIndex++, maxSize);
          set.Add(wrapper.CacheableKey);
        }
      }
      m_cacheableObject = set;
    }

    public override uint GetChecksum(object cacheableObject)
    {
      //List<object> value = new List<object>();
      CacheableLinkedHashSet value = cacheableObject as CacheableLinkedHashSet;
      Assert.IsNotNull(value, "GetChecksum: Null object.");

      uint ckSum = 0;
      CacheableKeyWrapper wrapper;
      foreach (object key in value)
      {
        wrapper = CacheableWrapperFactory.CreateKeyInstance(key);
        Assert.IsNotNull(wrapper, "CacheableLinkedHashSet.GetChecksum:" +
          " Could not create an instance of typeId [{0}].", wrapper.TypeId);
        ckSum ^= wrapper.GetChecksum(key);
      }
      return ckSum;
    }

    #endregion
  }

  public class CacheableInt16ArrayWrapper : CacheableWrapper
  {
    #region Static factory creation function

    public static CacheableWrapper Create()
    {
      return new CacheableInt16ArrayWrapper();
    }

    #endregion

    #region CacheableWrapper Members

    public override void InitRandomValue(int maxSize)
    {
      int arraySize = maxSize / sizeof(short);
      arraySize = arraySize > 1 ? arraySize : 2;
      short[] value = new short[arraySize];
      for (int arrayIndex = 0; arrayIndex < arraySize; arrayIndex++)
      {
        value[arrayIndex] = (short)Util.Rand(short.MaxValue);
      }
      m_cacheableObject = value;
    }

    public override uint GetChecksum(object cacheableObject)
    {
      Int16[] obj = cacheableObject as Int16[];
      Assert.IsNotNull(obj, "GetChecksum: Null object.");

      short[] value = obj;
      System.IO.MemoryStream ms = new System.IO.MemoryStream();
      foreach (short v in value)
      {
        short conv = v;
        int numBytes = sizeof(short);
        for (int i = 0; i < numBytes; i++)
        {
          ms.WriteByte((byte)(conv & 0xff));
          conv = (short)(conv >> 8);
        }
      }
      return CacheableHelper.CRC32(ms.ToArray());
    }

    #endregion
  }

  public class CacheableInt32ArrayWrapper : CacheableWrapper
  {
    #region Static factory creation function

    public static CacheableWrapper Create()
    {
      return new CacheableInt32ArrayWrapper();
    }

    #endregion

    #region CacheableWrapper Members

    public override void InitRandomValue(int maxSize)
    {
      int arraySize = maxSize / sizeof(int);
      arraySize = arraySize > 1 ? arraySize : 2;
      int[] value = new int[arraySize];
      for (int arrayIndex = 0; arrayIndex < arraySize; arrayIndex++)
      {
        value[arrayIndex] = Util.Rand(int.MaxValue);
      }
      m_cacheableObject = value;
    }

    public override uint GetChecksum(object cacheableObject)
    {
      Int32[] obj = cacheableObject as Int32[];
      Assert.IsNotNull(obj, "GetChecksum: Null object.");

      int[] value = obj;
      System.IO.MemoryStream ms = new System.IO.MemoryStream();
      foreach (int v in value)
      {
        int conv = v;
        int numBytes = sizeof(int);
        for (int i = 0; i < numBytes; i++)
        {
          ms.WriteByte((byte)(conv & 0xff));
          conv = conv >> 8;
        }
      }
      return CacheableHelper.CRC32(ms.ToArray());
    }

    #endregion
  }

  public class CacheableInt64ArrayWrapper : CacheableWrapper
  {
    #region Static factory creation function

    public static CacheableWrapper Create()
    {
      return new CacheableInt64ArrayWrapper();
    }

    #endregion

    #region CacheableWrapper Members

    public override void InitRandomValue(int maxSize)
    {
      int arraySize = maxSize / sizeof(long);
      arraySize = arraySize > 1 ? arraySize : 2;
      long[] value = new long[arraySize];
      long rnd;
      for (int arrayIndex = 0; arrayIndex < arraySize; arrayIndex++)
      {
        rnd = (long)Util.Rand(int.MaxValue);
        rnd = (rnd << 32) + (long)Util.Rand(int.MaxValue);
        value[arrayIndex] = rnd;
      }
      m_cacheableObject = value;
    }

    public override uint GetChecksum(object cacheableObject)
    {
      Int64[] obj = cacheableObject as Int64[];
      Assert.IsNotNull(obj, "GetChecksum: Null object.");

      long[] value = obj;
      System.IO.MemoryStream ms = new System.IO.MemoryStream();
      foreach (long v in value)
      {
        long conv = v;
        int numBytes = sizeof(long);
        for (int i = 0; i < numBytes; i++)
        {
          ms.WriteByte((byte)(conv & 0xff));
          conv = conv >> 8;
        }
      }
      return CacheableHelper.CRC32(ms.ToArray());
    }

    #endregion
  }

  public class CacheableNullStringWrapper : CacheableWrapper
  {
    #region Static factory creation function

    public static CacheableWrapper Create()
    {
      return new CacheableNullStringWrapper();
    }

    #endregion

    #region CacheableWrapper Members

    public override void InitRandomValue(int maxSize)
    {
      //m_cacheableObject = CacheableString.Create((string)null);
      m_cacheableObject = null;
    }

    public override uint GetChecksum(object cacheableObject)
    {
      //CacheableString value = cacheableObject as CacheableString;
      Assert.IsNull(cacheableObject, "GetChecksum: expected null object.");
      return (uint)0;
    }

    #endregion
  }

  public class CacheableEmptyStringWrapper : CacheableWrapper
  {
    #region Static factory creation function

    public static CacheableWrapper Create()
    {
      return new CacheableEmptyStringWrapper();
    }

    #endregion

    #region CacheableWrapper Members

    public override void InitRandomValue(int maxSize)
    {
      //m_cacheableObject = CacheableString.Create(string.Empty);
      m_cacheableObject = string.Empty;
    }

    public override uint GetChecksum(object cacheableObject)
    {
      string value = cacheableObject as string;
      Assert.IsNotNull(value, "GetChecksum: expected non null object.");
      Assert.IsTrue(value == string.Empty || value.Length == 0, "Expected IsNullOrEmpty " + value.Length);
      return (uint)0;
    }

    #endregion
  }

  public class CacheableEmptyUnicodeStringWrapper : CacheableWrapper
  {
    // TODO : VJR : Is this a real UNICODE empty string?
    // See bugs #324 and #356 to add this wrapper when tracking them.

    #region Static factory creation function

    public static CacheableWrapper Create()
    {
      return new CacheableEmptyUnicodeStringWrapper();
    }

    #endregion

    #region CacheableWrapper Members

    public override void InitRandomValue(int maxSize)
    {
      //m_cacheableObject = CacheableString.Create(string.Empty);
      m_cacheableObject = string.Empty;
    }

    public override uint GetChecksum(object cacheableObject)
    {
      string value = cacheableObject as string;
      Assert.IsNotNull(value, "GetChecksum: expected non null object.");
      Assert.IsTrue(value == string.Empty || value.Length == 0, "Expected IsNullOrEmpty");
      return (uint)0;
    }

    #endregion
  }

  public class CacheableStringArrayWrapper : CacheableWrapper
  {
    #region Static factory creation function

    public static CacheableWrapper Create()
    {
      return new CacheableStringArrayWrapper();
    }

    #endregion

    #region CacheableWrapper Members

    public override void InitRandomValue(int maxSize)
    {
      int arraySize = 16;
      maxSize = maxSize / arraySize;
      if (maxSize < 2)
      {
        maxSize = 2;
      }
      String[] value = new String[arraySize];
      for (int arrayIndex = 0; arrayIndex < arraySize; arrayIndex++)
      {
        if (arrayIndex % 2 == 0)
        {
          byte[] buffer = Util.RandBytes(maxSize / 2);
          value[arrayIndex] = BitConverter.ToString(buffer).Replace("-", string.Empty);
        }
        else
        {
          byte[] buffer = Util.RandBytes(maxSize / 2);
          char[] charArray = new char[maxSize / 2];
          for (int i = 0; i < maxSize / 2; i++)
          {
            charArray[i] = (char)((int)buffer[i] + 0x0901);
          }
          value[arrayIndex] = new String(charArray);
        }
      }
      //m_cacheableObject = CacheableStringArray.Create(value);
      m_cacheableObject = value;
    }

    public override uint GetChecksum(object cacheableObject)
    {
      String[] value = cacheableObject as String[];
      Assert.IsNotNull(value, "GetChecksum: Null object.");

      System.IO.MemoryStream ms = new System.IO.MemoryStream();
      foreach (String str in value)
      {
        foreach (char c in str)
        {
          ms.WriteByte((byte)(c & 0xff));
          byte uByte = (byte)((c >> 8) & 0xff);
          if(uByte != 0x00)
            ms.WriteByte(uByte);
        }        
      }
      return CacheableHelper.CRC32(ms.ToArray());
    }

    #endregion
  }

  public class CacheableUndefinedWrapper : CacheableWrapper
  {
    #region Static factory creation function

    public static CacheableWrapper Create()
    {
      return new CacheableUndefinedWrapper();
    }

    #endregion

    #region CacheableWrapper Members

    public override void InitRandomValue(int maxSize)
    {
      m_cacheableObject = new CacheableUndefined();
    }

    public override uint GetChecksum(object cacheableObject)
    {
      CacheableUndefined value = cacheableObject as CacheableUndefined;
      // TODO: [sumedh] server sends back null; check this
      //Assert.IsNotNull(value, "GetChecksum: Null object.");

      return 0;
    }

    #endregion
  }

  public class CacheableVectorWrapper : CacheableWrapper
  {
    #region Static factory creation function

    public static CacheableWrapper Create()
    {
      return new CacheableVectorWrapper();
    }

    #endregion

    #region CacheableWrapper Members

    public override void InitRandomValue(int maxSize)
    {
      //CacheableVector vec = new CacheableVector();
      System.Collections.ArrayList vec = new System.Collections.ArrayList();

      ICollection<UInt32> valueTypeIds =
        CacheableWrapperFactory.GetRegisteredValueTypeIds();
      maxSize = maxSize / valueTypeIds.Count + 1;
      CacheableWrapper wrapper;
      foreach (UInt32 typeId in valueTypeIds)
      {
        if (!CacheableHelper.IsContainerTypeId(typeId) &&
          !CacheableHelper.IsUnhandledType(typeId))
        {
          wrapper = CacheableWrapperFactory.CreateInstance(typeId);
          Assert.IsNotNull(wrapper, "CacheableVectorWrapper.InitRandomValue:" +
            " Could not create an instance of typeId [{0}].", typeId);
          wrapper.InitRandomValue(maxSize);
          vec.Add(wrapper.Cacheable);
        }
      }
      m_cacheableObject = vec;
    }

    public override uint GetChecksum(object cacheableObject)
    {
      System.Collections.ArrayList value = cacheableObject as System.Collections.ArrayList;
      Assert.IsNotNull(value, "GetChecksum: Null object.");

      uint ckSum = 0;
      CacheableWrapper wrapper;
      foreach (object obj in value)
      {
        if (obj == null)
        {
          continue;
        }
        wrapper = CacheableWrapperFactory.CreateInstance(obj);
        Assert.IsNotNull(wrapper, "CacheableVectorWrapper.GetChecksum:" +
          " Could not create an instance of typeId [{0}].", wrapper.TypeId);
        ckSum ^= wrapper.GetChecksum(obj);
      }
      return ckSum;
    }

    #endregion
  }

  public class CacheableObjectArrayWrapper : CacheableWrapper
  {
    #region Static factory creation function

    public static CacheableWrapper Create()
    {
      return new CacheableObjectArrayWrapper();
    }

    #endregion

    #region CacheableWrapper Members

    public override void InitRandomValue(int maxSize)
    {
      CacheableObjectArray arr = new CacheableObjectArray();
      ICollection<UInt32> valueTypeIds =
        CacheableWrapperFactory.GetRegisteredValueTypeIds();
      maxSize = maxSize / valueTypeIds.Count + 1;
      CacheableWrapper wrapper;
      foreach (UInt32 typeId in valueTypeIds)
      {
        if (!CacheableHelper.IsContainerTypeId(typeId) &&
          !CacheableHelper.IsUnhandledType(typeId))
        {
          wrapper = CacheableWrapperFactory.CreateInstance(typeId);
          Assert.IsNotNull(wrapper, "CacheableObjectArrayWrapper.InitRandomValue:" +
            " Could not create an instance of typeId [{0}].", typeId);
          wrapper.InitRandomValue(maxSize);

          arr.Add(wrapper.Cacheable);
        }
      }
      m_cacheableObject = arr;
    }

    public override uint GetChecksum(object cacheableObject)
    {
      CacheableObjectArray value = cacheableObject as CacheableObjectArray;
      Assert.IsNotNull(value, "GetChecksum: Null object.");

      uint ckSum = 0;
      CacheableWrapper wrapper;
      foreach (object obj in value)
      {
        if (obj == null)
        {
          continue;
        }
        wrapper = CacheableWrapperFactory.CreateInstance(obj);
        Assert.IsNotNull(wrapper, "CacheableObjectArrayWrapper.GetChecksum:" +
          " Could not create an instance of typeId [{0}].", wrapper.TypeId );
        ckSum ^= wrapper.GetChecksum(obj);
      }
      return ckSum;
    }

    #endregion
  }

  public class CacheableArrayListWrapper : CacheableWrapper
  {
    #region Static factory creation function

    public static CacheableWrapper Create()
    {
      return new CacheableArrayListWrapper();
    }

    #endregion

    #region CacheableWrapper Members

    public override void InitRandomValue(int maxSize)
    {
      //CacheableArrayList arrayList = new CacheableArrayList();
      List<object> arrayList = new List<object>();

      ICollection<UInt32> valueTypeIds =
        CacheableWrapperFactory.GetRegisteredValueTypeIds();
      maxSize = maxSize / valueTypeIds.Count + 1;
      CacheableWrapper wrapper;
      Util.Log(" arrayList size InitRandomValue ");
      foreach (UInt32 typeId in valueTypeIds)
      {
        if (!CacheableHelper.IsContainerTypeId(typeId) &&
          !CacheableHelper.IsUnhandledType(typeId))
        {
          wrapper = CacheableWrapperFactory.CreateInstance(typeId);
          Assert.IsNotNull(wrapper, "CacheableArrayListWrapper.InitRandomValue:" +
            " Could not create an instance of typeId [{0}].", typeId);
          wrapper.InitRandomValue(maxSize);
          arrayList.Add(wrapper.Cacheable);
        }
      }
      m_cacheableObject = arrayList;
      Util.Log(" arrayList size InitRandomValue " + arrayList.Count);
    }

    public override uint GetChecksum(object cacheableObject)
    {
      List<object> value = cacheableObject as List<object>;
      Assert.IsNotNull(value, "GetChecksum: Null object.");

      uint ckSum = 0;
      CacheableWrapper wrapper;
      foreach (object obj in value)
      {
        if (obj == null)
        {
          continue;
        }
        wrapper = CacheableWrapperFactory.CreateInstance(obj);
        Assert.IsNotNull(wrapper, "CacheableArrayListWrapper.GetChecksum:" +
          " Could not create an instance of typeId [{0}].", wrapper.TypeId);
        ckSum ^= wrapper.GetChecksum(obj);
      }
      return ckSum;
    }

    #endregion
  }

  public class CacheableStackWrapper : CacheableWrapper
  {
    #region Static factory creation function

    public static CacheableWrapper Create()
    {
      return new CacheableStackWrapper();
    }

    #endregion

    #region CacheableWrapper Members

    public override void InitRandomValue(int maxSize)
    {
      //CacheableStack vec = new CacheableStack();
      Stack<object> vec = new Stack<object>();

      ICollection<UInt32> valueTypeIds =
        CacheableWrapperFactory.GetRegisteredValueTypeIds();
      maxSize = maxSize / valueTypeIds.Count + 1;
      CacheableWrapper wrapper;
      foreach (UInt32 typeId in valueTypeIds)
      {
        if (!CacheableHelper.IsContainerTypeId(typeId) &&
            !CacheableHelper.IsUnhandledType(typeId))
        {
          wrapper = CacheableWrapperFactory.CreateInstance(typeId);
          Assert.IsNotNull(wrapper, "CacheableStackWrapper.InitRandomValue:" +
            " Could not create an instance of typeId [{0}].", typeId);
          wrapper.InitRandomValue(maxSize);
          vec.Push(wrapper.Cacheable);
        }
      }
      m_cacheableObject = vec;
    }

    public override uint GetChecksum(object cacheableObject)
    {
      Stack<object> value = cacheableObject as Stack<object>;
      Assert.IsNotNull(value, "GetChecksum: Null object.");

      uint ckSum = 0;
      CacheableWrapper wrapper;
      foreach (Object obj in value)
      {
        if (obj == null)
        {
          continue;
        }
        wrapper = CacheableWrapperFactory.CreateInstance(obj);
        Assert.IsNotNull(wrapper, "CacheableStackWrapper.GetChecksum:" +
          " Could not create an instance of typeId [{0}].", wrapper.TypeId);
        ckSum ^= wrapper.GetChecksum(obj);
      }
      return ckSum;
    }

    #endregion
  }

  public class CacheableObjectWrapper : CacheableWrapper
  {
    #region Static factory creation function

    public static CacheableWrapper Create()
    {
      return new CacheableObjectWrapper();
    }

    #endregion

    #region CacheableWrapper Members

    public override void InitRandomValue(int maxSize)
    {
      CData value = CacheableHelper.RandCData();
      m_cacheableObject = CacheableObject.Create(value);
    }

    public override uint GetChecksum(object cacheableObject)
    {
      CacheableObject obj = cacheableObject as CacheableObject;
      Assert.IsNotNull(obj, "GetChecksum: Null object.");

      CData value = (CData)obj.Value;
      System.IO.MemoryStream ms = new System.IO.MemoryStream();
      int first = value.First;
      int numBytes = sizeof(int);
      for (int i = 0; i < numBytes; i++)
      {
        ms.WriteByte((byte)(first & 0xff));
        first = first >> 8;
      }
      long second = value.Second;
      numBytes = sizeof(long);
      for (int i = 0; i < numBytes; i++)
      {
        ms.WriteByte((byte)(second & 0xff));
        second = second >> 8;
      }
      return CacheableHelper.CRC32(ms.ToArray());
    }


    #endregion
  }

  public class CacheableObjectXmlWrapper : CacheableWrapper
  {
    #region Static factory creation function

    public static CacheableWrapper Create()
    {
      return new CacheableObjectXmlWrapper();
    }

    #endregion

    #region CacheableWrapper Members

    public override void InitRandomValue(int maxSize)
    {
      CData value = CacheableHelper.RandCData();
      m_cacheableObject = CacheableObjectXml.Create(value);
    }

    public override uint GetChecksum(object cacheableObject)
    {
      CacheableObjectXml obj = cacheableObject as CacheableObjectXml;
      Assert.IsNotNull(obj, "GetChecksum: Null object.");

      CData value = (CData)obj.Value;
      System.IO.MemoryStream ms = new System.IO.MemoryStream();
      int first = value.First;
      int numBytes = sizeof(int);
      for (int i = 0; i < numBytes; i++)
      {
        ms.WriteByte((byte)(first & 0xff));
        first = first >> 8;
      }
      long second = value.Second;
      numBytes = sizeof(long);
      for (int i = 0; i < numBytes; i++)
      {
        ms.WriteByte((byte)(second & 0xff));
        second = second >> 8;
      }
      return CacheableHelper.CRC32(ms.ToArray());
    }

    #endregion
  }

  public class PdxCDataWrapper : CacheableKeyWrapper
  {
    #region Static factory creation function

    public static CacheableKeyWrapper Create()
    {      
      return new PdxCDataWrapper();
    }

    #endregion

    #region CacheableWrapper Members

    public override void InitRandomValue(int maxSize)
    {
      PdxCData value = CacheableHelper.RandPdxCData();
      m_cacheableObject = value;
    }

    public override uint GetChecksum(object cacheableObject)
    {
      PdxCData value = cacheableObject as PdxCData;
      Assert.IsNotNull(value, "GetChecksum: Null object.");

      System.IO.MemoryStream ms = new System.IO.MemoryStream();
      int first = value.First;
      int numBytes = sizeof(int);
      for (int i = 0; i < numBytes; i++)
      {
        ms.WriteByte((byte)(first & 0xff));
        first = first >> 8;
      }
      long second = value.Second;
      numBytes = sizeof(long);
      for (int i = 0; i < numBytes; i++)
      {
        ms.WriteByte((byte)(second & 0xff));
        second = second >> 8;
      }
      return CacheableHelper.CRC32(ms.ToArray());
    }

    #endregion

    public override int MaxKeys
    {
      get { return 10; }
    }

    public override void InitKey(int keyIndex, int maxSize)
    {
      //PdxCData value = CacheableHelper.RandPdxCData();
      PdxCData value = new PdxCData(keyIndex, keyIndex*2999);
      m_cacheableObject = value;
    }
  }

  public class TypesClass {
    private Object[,] typesArray;
    private Object NullItem = null;
    public TypesClass()
    {
      typesArray = new Object[,] {
      { /*TypeIds.BoolId ,*/ false, false, false, false, false, false, false, false },
      { /*TypeIds.ByteId , */(byte)0, (byte)1, (byte)2, (byte)3, (byte)4, (byte)5, (byte)6, (byte)7 },
      { /*TypeIds.CharId ,*/ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h' },
      { /*TypeIds.DecimalId , */(decimal)0.0m, (decimal)1.0m, (decimal)2.0m, (decimal)3.0m , (decimal)4.0m, (decimal)5.0m, (decimal)6.0m, (decimal)7.0m },
      { /*TypeIds.DoubleId , */(double)0.0, (double)1.0, (double)2.0, (double)3.0 , (double)4.0, (double)5.0, (double)6.0, (double)7.0 },
      { /*TypeIds.FloatId , */(float)0.0f, (float)1.0f, (float)2.0f, (float)3.0f , (float)4.0f, (float)5.0f, (float)6.0f, (float)7.0f },
      { /*TypeIds.IntId , */0, 1, 2, 3 ,4 , 5, 6, 7},
      { /*TypeIds.LongId , */(long)0L, (long)1L, (long)2L, (long)3L ,(long)4L , (long)5L, (long)6L, (long)7L},
      { /*TypeIds.SbyteId , */(sbyte)0, (sbyte)1, (sbyte)2, (sbyte)3 ,(sbyte)4 , (sbyte)5, (sbyte)6, (sbyte)7 },
      { /*TypeIds.ShortId , */(short)0, (short)1, (short)2, (short)3 ,(short)4 , (short)5, (short)6, (short)7},
      { /*TypeIds.StructId ,*/ 0, 1, 2, 3 ,4 , 5, 6, 7},
      { /*TypeIds.UintId ,*/ (uint)0u, (uint)1u, (uint)2u, (uint)3u ,(uint)4u , (uint)5u, (uint)6u, (uint)7u},
      { /*TypeIds.UlongId ,*/ (ulong)0UL, (ulong)1UL, (ulong)2UL, (ulong)3UL ,(ulong)4UL , (ulong)5UL, (ulong)6UL, (ulong)7UL},
      { /*TypeIds.UshortId ,*/ (ushort)0, (ushort)1, (ushort)2, (ushort)3 ,(ushort)4 , (ushort)5, (ushort)6, (ushort)7 },
      { /*TypeIds.StringId ,*/ "Zero", "One", "Two", "Three" , "Four", "Five", "Six", "Seven" },
      { /*TypeIds.ObjectId ,*/ (object)"Zero", (object)"One", (object)"Two", (object)"Three" , (object)"Four", (object)"Five", (object)"Six", (object)"Seven" },
      { /*TypeIds.BoolArrayId ,*/ new bool[]{false, false, false, false, false, false, false, false}, new bool[]{false, false, false, false, false, false, false, false}, 
        new bool[]{false, false, false, false, false, false, false, false}, new bool[]{false, false, false, false, false, false, false, false}, 
        new bool[]{false, false, false, false, false, false, false, false}, new bool[]{false, false, false, false, false, false, false, false}, 
        new bool[]{false, false, false, false, false, false, false, false}, new bool[]{false, false, false, false, false, false, false, false} },
      { /*TypeIds.ByteArrayId ,*/ new byte[]{(byte)0, (byte)1, (byte)2, (byte)3, (byte)4, (byte)5, (byte)6, (byte)7}, new byte[]{(byte)0, (byte)1, (byte)2, (byte)3, (byte)4, (byte)5, (byte)6, (byte)7}, new byte[]{(byte)0, (byte)1, (byte)2, (byte)3, (byte)4, (byte)5, (byte)6, (byte)7}, 
        new byte[]{(byte)0, (byte)1, (byte)2, (byte)3, (byte)4, (byte)5, (byte)6, (byte)7}, new byte[]{(byte)0, (byte)1, (byte)2, (byte)3, (byte)4, (byte)5, (byte)6, (byte)7}, new byte[]{(byte)0, (byte)1, (byte)2, (byte)3, (byte)4, (byte)5, (byte)6, (byte)7}, new byte[]{(byte)0, (byte)1, (byte)2, (byte)3, (byte)4, (byte)5, (byte)6, (byte)7},
        new byte[]{(byte)0, (byte)1, (byte)2, (byte)3, (byte)4, (byte)5, (byte)6, (byte)7} },
      { /*TypeIds.CharArrayId ,*/ new char[]{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'}, new char[]{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'}, new char[]{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'}, 
        new char[]{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'}, new char[]{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'}, new char[]{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'}, new char[]{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'},
        new char[]{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'} },
      { /*TypeIds.DecimalArrayId ,*/ new decimal[]{(decimal)0, (decimal)1, (decimal)2, (decimal)3, (decimal)4, (decimal)5, (decimal)6, (decimal)7}, new decimal[]{(decimal)0, (decimal)1, (decimal)2, (decimal)3, (decimal)4, (decimal)5, (decimal)6, (decimal)7}, new decimal[]{(decimal)0, (decimal)1, (decimal)2, (decimal)3, (decimal)4, (decimal)5, (decimal)6, (decimal)7}, 
        new decimal[]{(decimal)0, (decimal)1, (decimal)2, (decimal)3, (decimal)4, (decimal)5, (decimal)6, (decimal)7}, new decimal[]{(decimal)0, (decimal)1, (decimal)2, (decimal)3, (decimal)4, (decimal)5, (decimal)6, (decimal)7}, new decimal[]{(decimal)0, (decimal)1, (decimal)2, (decimal)3, (decimal)4, (decimal)5, (decimal)6, (decimal)7}, new decimal[]{(decimal)0, (decimal)1, (decimal)2, (decimal)3, (decimal)4, (decimal)5, (decimal)6, (decimal)7},
        new decimal[]{(decimal)0, (decimal)1, (decimal)2, (decimal)3, (decimal)4, (decimal)5, (decimal)6, (decimal)7} },      
      { /*TypeIds.doubleArrayId ,*/ new Double[]{(Double)1.7E+0D, (Double)1.7E+1D, (Double)1.7E+2D, (Double)1.7E+3D, (Double)1.7E+4D, (Double)1.7E+5D, (Double)1.7E+6D, (Double)1.7E+7D}, new Double[]{(Double)1.7E+0D, (Double)1.7E+1D, (Double)1.7E+2D, (Double)1.7E+3D, (Double)1.7E+4D, (Double)1.7E+5D, (Double)1.7E+6D, (Double)1.7E+7D}, new Double[]{(Double)1.7E+0D, (Double)1.7E+1D, (Double)1.7E+2D, (Double)1.7E+3D, (Double)1.7E+4D, (Double)1.7E+5D, (Double)1.7E+6D, (Double)1.7E+7D}, 
        new Double[]{(Double)1.7E+0D, (Double)1.7E+1D, (Double)1.7E+2D, (Double)1.7E+3D, (Double)1.7E+4D, (Double)1.7E+5D, (Double)1.7E+6D, (Double)1.7E+7D}, new Double[]{(Double)1.7E+0D, (Double)1.7E+1D, (Double)1.7E+2D, (Double)1.7E+3D, (Double)1.7E+4D, (Double)1.7E+5D, (Double)1.7E+6D, (Double)1.7E+7D}, new Double[]{(Double)1.7E+0D, (Double)1.7E+1D, (Double)1.7E+2D, (Double)1.7E+3D, (Double)1.7E+4D, (Double)1.7E+5D, (Double)1.7E+6D, (Double)1.7E+7D}, new Double[]{(Double)1.7E+0D, (Double)1.7E+1D, (Double)1.7E+2D, (Double)1.7E+3D, (Double)1.7E+4D, (Double)1.7E+5D, (Double)1.7E+6D, (Double)1.7E+7D},
        new Double[]{(Double)1.7E+0D, (Double)1.7E+1D, (Double)1.7E+2D, (Double)1.7E+3D, (Double)1.7E+4D, (Double)1.7E+5D, (Double)1.7E+6D, (Double)1.7E+7D} },
      { /*TypeIds.floatArrayId ,*/ new float[]{(float)0.0f, (float)1.0f, (float)2.0f, (float)3.0f, (float)4.0f, (float)5.0f, (float)6.0f, (float)7.0f}, new float[]{(float)0.0f, (float)1.0f, (float)2.0f, (float)3.0f, (float)4.0f, (float)5.0f, (float)6.0f, (float)7.0f}, new float[]{(float)0.0f, (float)1.0f, (float)2.0f, (float)3.0f, (float)4.0f, (float)5.0f, (float)6.0f, (float)7.0f}, 
        new float[]{(float)0.0f, (float)1.0f, (float)2.0f, (float)3.0f, (float)4.0f, (float)5.0f, (float)6.0f, (float)7.0f}, new float[]{(float)0.0f, (float)1.0f, (float)2.0f, (float)3.0f, (float)4.0f, (float)5.0f, (float)6.0f, (float)7.0f}, new float[]{(float)0.0f, (float)1.0f, (float)2.0f, (float)3.0f, (float)4.0f, (float)5.0f, (float)6.0f, (float)7.0f}, new float[]{(float)0.0f, (float)1.0f, (float)2.0f, (float)3.0f, (float)4.0f, (float)5.0f, (float)6.0f, (float)7.0f},
        new float[]{(float)0.0f, (float)1.0f, (float)2.0f, (float)3.0f, (float)4.0f, (float)5.0f, (float)6.0f, (float)7.0f} },
      { /*TypeIds.intArrayId ,*/ new int[]{(int)0, (int)1, (int)2, (int)3, (int)4, (int)5, (int)6, (int)7}, new int[]{(int)0, (int)1, (int)2, (int)3, (int)4, (int)5, (int)6, (int)7}, new int[]{(int)0, (int)1, (int)2, (int)3, (int)4, (int)5, (int)6, (int)7}, 
        new int[]{(int)0, (int)1, (int)2, (int)3, (int)4, (int)5, (int)6, (int)7}, new int[]{(int)0, (int)1, (int)2, (int)3, (int)4, (int)5, (int)6, (int)7}, new int[]{(int)0, (int)1, (int)2, (int)3, (int)4, (int)5, (int)6, (int)7}, new int[]{(int)0, (int)1, (int)2, (int)3, (int)4, (int)5, (int)6, (int)7},
        new int[]{(int)0, (int)1, (int)2, (int)3, (int)4, (int)5, (int)6, (int)7} },
      { /*TypeIds.longArrayId ,*/ new long[]{(long)0L, (long)1L, (long)2L, (long)3L, (long)4L, (long)5L, (long)6L, (long)7L}, new long[]{(long)0L, (long)1L, (long)2L, (long)3L, (long)4L, (long)5L, (long)6L, (long)7L}, new long[]{(long)0L, (long)1L, (long)2L, (long)3L, (long)4L, (long)5L, (long)6L, (long)7L}, 
        new long[]{(long)0L, (long)1L, (long)2L, (long)3L, (long)4L, (long)5L, (long)6L, (long)7L}, new long[]{(long)0L, (long)1L, (long)2L, (long)3L, (long)4L, (long)5L, (long)6L, (long)7L}, new long[]{(long)0L, (long)1L, (long)2L, (long)3L, (long)4L, (long)5L, (long)6L, (long)7L}, new long[]{(long)0L, (long)1L, (long)2L, (long)3L, (long)4L, (long)5L, (long)6L, (long)7L},
        new long[]{(long)0L, (long)1L, (long)2L, (long)3L, (long)4L, (long)5L, (long)6L, (long)7L} },
      { /*TypeIds.sbyteArrayId ,*/ new sbyte[]{(sbyte)0 , (sbyte)1 , (sbyte)2 , (sbyte)3 , (sbyte)4 , (sbyte)5 , (sbyte)6 , (sbyte)7 }, new sbyte[]{(sbyte)0 , (sbyte)1 , (sbyte)2 , (sbyte)3 , (sbyte)4 , (sbyte)5 , (sbyte)6 , (sbyte)7 }, new sbyte[]{(sbyte)0 , (sbyte)1 , (sbyte)2 , (sbyte)3 , (sbyte)4 , (sbyte)5 , (sbyte)6 , (sbyte)7 }, 
        new sbyte[]{(sbyte)0 , (sbyte)1 , (sbyte)2 , (sbyte)3 , (sbyte)4 , (sbyte)5 , (sbyte)6 , (sbyte)7 }, new sbyte[]{(sbyte)0 , (sbyte)1 , (sbyte)2 , (sbyte)3 , (sbyte)4 , (sbyte)5 , (sbyte)6 , (sbyte)7 }, new sbyte[]{(sbyte)0 , (sbyte)1 , (sbyte)2 , (sbyte)3 , (sbyte)4 , (sbyte)5 , (sbyte)6 , (sbyte)7 }, new sbyte[]{(sbyte)0 , (sbyte)1 , (sbyte)2 , (sbyte)3 , (sbyte)4 , (sbyte)5 , (sbyte)6 , (sbyte)7 },
        new sbyte[]{(sbyte)0 , (sbyte)1 , (sbyte)2 , (sbyte)3 , (sbyte)4 , (sbyte)5 , (sbyte)6 , (sbyte)7 } },
      { /*TypeIds.shortArrayId ,*/ new short[]{(short)0 , (short)1 , (short)2 , (short)3 , (short)4 , (short)5 , (short)6 , (short)7 }, new short[]{(short)0 , (short)1 , (short)2 , (short)3 , (short)4 , (short)5 , (short)6 , (short)7 }, new short[]{(short)0 , (short)1 , (short)2 , (short)3 , (short)4 , (short)5 , (short)6 , (short)7 }, 
        new short[]{(short)0 , (short)1 , (short)2 , (short)3 , (short)4 , (short)5 , (short)6 , (short)7 }, new short[]{(short)0 , (short)1 , (short)2 , (short)3 , (short)4 , (short)5 , (short)6 , (short)7 }, new short[]{(short)0 , (short)1 , (short)2 , (short)3 , (short)4 , (short)5 , (short)6 , (short)7 }, new short[]{(short)0 , (short)1 , (short)2 , (short)3 , (short)4 , (short)5 , (short)6 , (short)7 },
        new short[]{(short)0 , (short)1 , (short)2 , (short)3 , (short)4 , (short)5 , (short)6 , (short)7 } },
      { /*TypeIds.StructArrayId ,*/ new int[]{0, 1, 2, 3 ,4 , 5, 6, 7},new int[]{0, 1, 2, 3 ,4 , 5, 6, 7},new int[]{0, 1, 2, 3 ,4 , 5, 6, 7},new int[]{0, 1, 2, 3 ,4 , 5, 6, 7},new int[]{0, 1, 2, 3 ,4 , 5, 6, 7},new int[]{0, 1, 2, 3 ,4 , 5, 6, 7},new int[]{0, 1, 2, 3 ,4 , 5, 6, 7},new int[]{0, 1, 2, 3 ,4 , 5, 6, 7}},
      { /*TypeIds.uintArrayId ,*/ new uint[]{(uint)0 , (uint)1 , (uint)2 , (uint)3 , (uint)4 , (uint)5 , (uint)6 , (uint)7 }, new uint[]{(uint)0 , (uint)1 , (uint)2 , (uint)3 , (uint)4 , (uint)5 , (uint)6 , (uint)7 }, new uint[]{(uint)0 , (uint)1 , (uint)2 , (uint)3 , (uint)4 , (uint)5 , (uint)6 , (uint)7 }, 
        new uint[]{(uint)0 , (uint)1 , (uint)2 , (uint)3 , (uint)4 , (uint)5 , (uint)6 , (uint)7 }, new uint[]{(uint)0 , (uint)1 , (uint)2 , (uint)3 , (uint)4 , (uint)5 , (uint)6 , (uint)7 }, new uint[]{(uint)0 , (uint)1 , (uint)2 , (uint)3 , (uint)4 , (uint)5 , (uint)6 , (uint)7 }, new uint[]{(uint)0 , (uint)1 , (uint)2 , (uint)3 , (uint)4 , (uint)5 , (uint)6 , (uint)7 },
        new uint[]{(uint)0 , (uint)1 , (uint)2 , (uint)3 , (uint)4 , (uint)5 , (uint)6 , (uint)7 } },
      { /*TypeIds.ulongArrayId ,*/ new ulong[]{(uint)0 , (ulong)1 , (ulong)2 , (ulong)3 , (ulong)4 , (ulong)5 , (ulong)6 , (ulong)7 }, new ulong[]{(ulong)0 , (ulong)1 , (ulong)2 , (ulong)3 , (ulong)4 , (ulong)5 , (ulong)6 , (ulong)7 }, new ulong[]{(ulong)0 , (ulong)1 , (ulong)2 , (ulong)3 , (ulong)4 , (ulong)5 , (ulong)6 , (ulong)7 }, 
        new ulong[]{(ulong)0 , (ulong)1 , (ulong)2 , (ulong)3 , (ulong)4 , (ulong)5 , (ulong)6 , (ulong)7 }, new ulong[]{(ulong)0 , (ulong)1 , (ulong)2 , (ulong)3 , (ulong)4 , (ulong)5 , (ulong)6 , (ulong)7 }, new ulong[]{(ulong)0 , (ulong)1 , (ulong)2 , (ulong)3 , (ulong)4 , (ulong)5 , (ulong)6 , (ulong)7 }, new ulong[]{(ulong)0 , (ulong)1 , (ulong)2 , (ulong)3 , (ulong)4 , (ulong)5 , (ulong)6 , (ulong)7 },
        new ulong[]{(ulong)0 , (ulong)1 , (ulong)2 , (ulong)3 , (ulong)4 , (ulong)5 , (ulong)6 , (ulong)7 } },
      { /*TypeIds.ushortArrayId ,*/ new ushort[]{(ushort)0 , (ushort)1 , (ushort)2 , (ushort)3 , (ushort)4 , (ushort)5 , (ushort)6 , (ushort)7 }, new ushort[]{(ushort)0 , (ushort)1 , (ushort)2 , (ushort)3 , (ushort)4 , (ushort)5 , (ushort)6 , (ushort)7 }, new ushort[]{(ushort)0 , (ushort)1 , (ushort)2 , (ushort)3 , (ushort)4 , (ushort)5 , (ushort)6 , (ushort)7 }, 
        new ushort[]{(ushort)0 , (ushort)1 , (ushort)2 , (ushort)3 , (ushort)4 , (ushort)5 , (ushort)6 , (ushort)7 }, new ushort[]{(ushort)0 , (ushort)1 , (ushort)2 , (ushort)3 , (ushort)4 , (ushort)5 , (ushort)6 , (ushort)7 }, new ushort[]{(ushort)0 , (ushort)1 , (ushort)2 , (ushort)3 , (ushort)4 , (ushort)5 , (ushort)6 , (ushort)7 }, new ushort[]{(ushort)0 , (ushort)1 , (ushort)2 , (ushort)3 , (ushort)4 , (ushort)5 , (ushort)6 , (ushort)7 },
        new ushort[]{(ushort)0 , (ushort)1 , (ushort)2 , (ushort)3 , (ushort)4 , (ushort)5 , (ushort)6 , (ushort)7 } },      
      { /*TypeIds.stringArrayId ,*/ new string[]{"0" ,  "1" ,  "2" ,  "3" ,  "4" ,  "5" ,  "6" ,  "7" }, new string[]{ "0" ,  "1" ,  "2" ,  "3" ,  "4" ,  "5" ,  "6" ,  "7" }, new string[]{ "0" ,  "1" ,  "2" ,  "3" ,  "4" ,  "5" ,  "6" ,  "7" }, 
        new string[]{ "0" ,  "1" ,  "2" ,  "3" ,  "4" ,  "5" ,  "6" ,  "7" }, new string[]{ "0" ,  "1" ,  "2" ,  "3" ,  "4" ,  "5" ,  "6" ,  "7" }, new string[]{ "0" ,  "1" ,  "2" ,  "3" ,  "4" ,  "5" ,  "6" ,  "7" }, new string[]{ "0" ,  "1" ,  "2" ,  "3" ,  "4" ,  "5" ,  "6" ,  "7" },
        new string[]{ "0" ,  "1" ,  "2" ,  "3" ,  "4",  "5" ,  "6" ,  "7" } },
      { /*TypeIds.ObjectArrayId ,*/ new object[]{(object)0 , (object)1 , (object)2 , (object)3 , (object)4 , (object)5 , (object)6 , (object)7 }, new object[]{(object)0 , (object)1 , (object)2 , (object)3 , (object)4 , (object)5 , (object)6 , (object)7 }, new object[]{(object)0 , (object)1 , (object)2 , (object)3 , (object)4 , (object)5 , (object)6 , (object)7 }, 
        new object[]{(object)0 , (object)1 , (object)2 , (object)3 , (object)4 , (object)5 , (object)6 , (object)7 }, new object[]{(object)0 , (object)1 , (object)2 , (object)3 , (object)4 , (object)5 , (object)6 , (object)7 }, new object[]{(object)0 , (object)1 , (object)2 , (object)3 , (object)4 , (object)5 , (object)6 , (object)7 }, new object[]{(object)0 , (object)1 , (object)2 , (object)3 , (object)4 , (object)5 , (object)6 , (object)7 },
        new object[]{(object)0 , (object)1 , (object)2 , (object)3 , (object)4 , (object)5 , (object)6 , (object)7 } }      
      };      
    }

    public enum TypeIds
    {
      BoolId = 0,
      ByteId,
      CharId,
      DecimalId,
      DoubleId,
      FloatId,
      IntId,
      LongId,
      SbyteId,
      ShortId,
      StructId,
      UintId,
      UlongId,
      UshortId,
      StringId,
      ObjectId, 
      //These all array types will be values only.
      BoolArrayId,
      ByteArrayId,
      CharArrayId,
      DecimalArrayId,
      DoubleArrayId,
      FloatArrayId,
      IntArrayId,
      LongArrayId,
      SbyteArrayId,
      ShortArrayId,
      StructArrayId,
      UintArrayId,
      UlongArrayId,
      UshortArrayId,
      StringArrayId,
      ObjectArrayId
    };

    public TItem GetTypeItem<TItem>(int KeyId, int index)
    {
      //Util.Log("TItem type is {0} ", typeof(TItem));
      return (TItem)typesArray[KeyId, index];
    }

    public TItem GetArrayTypeItem<TItem>(int KeyId, int index)
    {
      //Util.Log("TItem type is {0} keyid = {1} index = {2}", typeof(TItem), KeyId, index);
      TItem temp = (TItem)typesArray[KeyId, index];
      return temp;
    }

    public Object[,] GetTypesArray()
    {
      return typesArray;
    }

    public Object GetNullItem()
    {
      return NullItem;
    }
  
};
  #endregion
}
