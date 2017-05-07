//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;

namespace GemStone.GemFire.Cache.UnitTests
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;

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

    public static void RegisterBuiltins()
    {
      CacheableWrapperFactory.ClearStatics();

      #region Cacheable keys

      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableBoolean,
        typeof(CacheableBooleanWrapper), CacheableBooleanWrapper.Create);
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableByte,
        typeof(CacheableByteWrapper), CacheableByteWrapper.Create);
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableCharacter,
        typeof(CacheableCharacterWrapper), CacheableCharacterWrapper.Create);
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableDate,
        typeof(CacheableDateWrapper), CacheableDateWrapper.Create);
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableDouble,
        typeof(CacheableDoubleWrapper), CacheableDoubleWrapper.Create);
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableFileName,
        typeof(CacheableFileNameWrapper), CacheableFileNameWrapper.Create);
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableFloat,
        typeof(CacheableFloatWrapper), CacheableFloatWrapper.Create);
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableInt16,
        typeof(CacheableInt16Wrapper), CacheableInt16Wrapper.Create);
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableInt32,
        typeof(CacheableInt32Wrapper), CacheableInt32Wrapper.Create);
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableInt64,
        typeof(CacheableInt64Wrapper), CacheableInt64Wrapper.Create);
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableASCIIString,
        typeof(CacheableStringWrapper), CacheableStringWrapper.Create);
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableString,
        typeof(CacheableUnicodeStringWrapper), CacheableUnicodeStringWrapper.Create);
      //TODO:hitesh with appdomain it is not working
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableASCIIStringHuge,
        typeof(CacheableHugeStringWrapper), CacheableHugeStringWrapper.Create);
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableStringHuge,
        typeof(CacheableHugeUnicodeStringWrapper), CacheableHugeUnicodeStringWrapper.Create);
      
      #endregion

      #region Other cacheables

      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableBytes,
        typeof(CacheableBytesWrapper), CacheableBytesWrapper.Create);
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableDoubleArray,
        typeof(CacheableDoubleArrayWrapper), CacheableDoubleArrayWrapper.Create);
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableFloatArray,
        typeof(CacheableFloatArrayWrapper), CacheableFloatArrayWrapper.Create);
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableHashMap,
        typeof(CacheableHashMapWrapper), CacheableHashMapWrapper.Create);
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableHashTable,
        typeof(CacheableHashTableWrapper), CacheableHashTableWrapper.Create);
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableIdentityHashMap,
        typeof(CacheableIdentityHashMapWrapper), CacheableIdentityHashMapWrapper.Create);
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableHashSet,
        typeof(CacheableHashSetWrapper), CacheableHashSetWrapper.Create);
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableLinkedHashSet,
        typeof(CacheableLinkedHashSetWrapper), CacheableLinkedHashSetWrapper.Create);

      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableInt16Array,
        typeof(CacheableInt16ArrayWrapper), CacheableInt16ArrayWrapper.Create);
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableInt32Array,
        typeof(CacheableInt32ArrayWrapper), CacheableInt32ArrayWrapper.Create);
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableInt64Array,
        typeof(CacheableInt64ArrayWrapper), CacheableInt64ArrayWrapper.Create);
      //CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableNullString,
        //typeof(CacheableNullStringWrapper), CacheableNullStringWrapper.Create);
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableASCIIString,
        typeof(CacheableEmptyStringWrapper), CacheableEmptyStringWrapper.Create);
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableString,
        typeof(CacheableEmptyUnicodeStringWrapper), CacheableEmptyUnicodeStringWrapper.Create);
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableStringArray,
        typeof(CacheableStringArrayWrapper), CacheableStringArrayWrapper.Create);
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableUndefined,
        typeof(CacheableUndefinedWrapper), CacheableUndefinedWrapper.Create);
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableVector,
        typeof(CacheableVectorWrapper), CacheableVectorWrapper.Create);
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableObjectArray,
        typeof(CacheableObjectArrayWrapper), CacheableObjectArrayWrapper.Create);
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableArrayList,
        typeof(CacheableArrayListWrapper), CacheableArrayListWrapper.Create);
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableStack,
        typeof(CacheableStackWrapper), CacheableStackWrapper.Create);

      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableManagedObject,
        typeof(CacheableObjectWrapper), CacheableObjectWrapper.Create);
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableManagedObjectXml,
        typeof(CacheableObjectXmlWrapper), CacheableObjectXmlWrapper.Create);
      
      #endregion
    }

    public static void RegisterBuiltinsAD()
    {
      CacheableWrapperFactory.ClearStatics();

      #region Cacheable keys

      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableBoolean,
        typeof(CacheableBooleanWrapper), CacheableBooleanWrapper.Create);
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableByte,
        typeof(CacheableByteWrapper), CacheableByteWrapper.Create);
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableCharacter,
        typeof(CacheableCharacterWrapper), CacheableCharacterWrapper.Create);
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableDate,
        typeof(CacheableDateWrapper), CacheableDateWrapper.Create);
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableDouble,
        typeof(CacheableDoubleWrapper), CacheableDoubleWrapper.Create);
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableFileName,
        typeof(CacheableFileNameWrapper), CacheableFileNameWrapper.Create);
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableFloat,
        typeof(CacheableFloatWrapper), CacheableFloatWrapper.Create);
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableInt16,
        typeof(CacheableInt16Wrapper), CacheableInt16Wrapper.Create);
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableInt32,
        typeof(CacheableInt32Wrapper), CacheableInt32Wrapper.Create);
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableInt64,
        typeof(CacheableInt64Wrapper), CacheableInt64Wrapper.Create);
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableASCIIString,
        typeof(CacheableStringWrapper), CacheableStringWrapper.Create);
      CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableString,
        typeof(CacheableUnicodeStringWrapper), CacheableUnicodeStringWrapper.Create);
      //TODO:hitesh with appdomain it is not working
      //CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableASCIIStringHuge,
      //  typeof(CacheableHugeStringWrapper), CacheableHugeStringWrapper.Create);
      //CacheableWrapperFactory.RegisterKeyType(GemFireClassIds.CacheableStringHuge,
      //  typeof(CacheableHugeUnicodeStringWrapper), CacheableHugeUnicodeStringWrapper.Create);

      #endregion

      #region Other cacheables

      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableBytes,
        typeof(CacheableBytesWrapper), CacheableBytesWrapper.Create);
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableDoubleArray,
        typeof(CacheableDoubleArrayWrapper), CacheableDoubleArrayWrapper.Create);
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableFloatArray,
        typeof(CacheableFloatArrayWrapper), CacheableFloatArrayWrapper.Create);
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableHashMap,
        typeof(CacheableHashMapWrapper), CacheableHashMapWrapper.Create);
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableHashTable,
        typeof(CacheableHashTableWrapper), CacheableHashTableWrapper.Create);
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableIdentityHashMap,
        typeof(CacheableIdentityHashMapWrapper), CacheableIdentityHashMapWrapper.Create);
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableHashSet,
        typeof(CacheableHashSetWrapper), CacheableHashSetWrapper.Create);
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableLinkedHashSet,
        typeof(CacheableLinkedHashSetWrapper), CacheableLinkedHashSetWrapper.Create);

      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableInt16Array,
        typeof(CacheableInt16ArrayWrapper), CacheableInt16ArrayWrapper.Create);
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableInt32Array,
        typeof(CacheableInt32ArrayWrapper), CacheableInt32ArrayWrapper.Create);
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableInt64Array,
        typeof(CacheableInt64ArrayWrapper), CacheableInt64ArrayWrapper.Create);
      //CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableNullString,
      //typeof(CacheableNullStringWrapper), CacheableNullStringWrapper.Create);
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableASCIIString,
        typeof(CacheableEmptyStringWrapper), CacheableEmptyStringWrapper.Create);
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableString,
        typeof(CacheableEmptyUnicodeStringWrapper), CacheableEmptyUnicodeStringWrapper.Create);
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableStringArray,
        typeof(CacheableStringArrayWrapper), CacheableStringArrayWrapper.Create);
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableUndefined,
        typeof(CacheableUndefinedWrapper), CacheableUndefinedWrapper.Create);
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableVector,
        typeof(CacheableVectorWrapper), CacheableVectorWrapper.Create);
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableObjectArray,
        typeof(CacheableObjectArrayWrapper), CacheableObjectArrayWrapper.Create);
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableArrayList,
        typeof(CacheableArrayListWrapper), CacheableArrayListWrapper.Create);
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableStack,
        typeof(CacheableStackWrapper), CacheableStackWrapper.Create);

      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableManagedObject,
        typeof(CacheableObjectWrapper), CacheableObjectWrapper.Create);
      CacheableWrapperFactory.RegisterType(GemFireClassIds.CacheableManagedObjectXml,
        typeof(CacheableObjectXmlWrapper), CacheableObjectXmlWrapper.Create);

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
      m_cacheableObject = new CacheableBoolean(value);
    }

    public override void InitRandomValue(int maxSize)
    {
      bool value = (Util.Rand(byte.MaxValue) % 2 == 1 ? true : false);
      m_cacheableObject = new CacheableBoolean(value);
    }

    public override uint GetChecksum(IGFSerializable cacheableObject)
    {
      CacheableBoolean value = cacheableObject as CacheableBoolean;
      Assert.IsNotNull(value, "GetChecksum: Null object.");

      return (uint)(value.Value ? 1 : 0);
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
      byte value = (byte)keyIndex;
      m_cacheableObject = new CacheableByte(value);
    }

    public override void InitRandomValue(int maxSize)
    {
      byte value = (byte)Util.Rand(byte.MaxValue);
      m_cacheableObject = new CacheableByte(value);
    }

    public override uint GetChecksum(IGFSerializable cacheableObject)
    {
      CacheableByte value = cacheableObject as CacheableByte;
      Assert.IsNotNull(value, "GetChecksum: Null object.");

      byte[] buffer = new byte[1];
      buffer[0] = value.Value;
      return CacheableHelper.CRC32(buffer);
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
      m_cacheableObject = new CacheableCharacter(value);
    }

    public override void InitRandomValue(int maxSize)
    {
      char value = (char)Util.Rand(char.MaxValue);
      m_cacheableObject = new CacheableCharacter(value);
    }

    public override uint GetChecksum(IGFSerializable cacheableObject)
    {
      CacheableCharacter obj = cacheableObject as CacheableCharacter;
      Assert.IsNotNull(obj, "GetChecksum: Null object.");

      char value = obj.Value;
      int numBytes = (int)sizeof(char);
      byte[] buffer = new byte[numBytes];
      for (int i = 0; i < numBytes; i++)
      {
        buffer[i] = (byte)(value & 0xff);
        value = (char)(value >> 8);
      }
      return CacheableHelper.CRC32(buffer);
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
      m_cacheableObject = new CacheableDate(
        DateTime.Today.AddMinutes(keyIndex % 0xFFFF));
    }

    public override void InitRandomValue(int maxSize)
    {
      int rnd = Util.Rand(int.MaxValue);
      DateTime value = DateTime.Now.AddMilliseconds(rnd % 2 == 0 ? rnd : -rnd);
      m_cacheableObject = new CacheableDate(value);
    }

    public override uint GetChecksum(IGFSerializable cacheableObject)
    {
      CacheableDate obj = cacheableObject as CacheableDate;
      Assert.IsNotNull(obj, "GetChecksum: Null object.");

      long value = obj.Value.Ticks;
      int numBytes = (int)sizeof(long);
      byte[] buffer = new byte[numBytes];
      for (int i = 0; i < numBytes; i++)
      {
        buffer[i] = (byte)(value & 0xff);
        value = value >> 8;
      }
      return CacheableHelper.CRC32(buffer);
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
      m_cacheableObject = new CacheableDouble(value);
    }

    public override void InitRandomValue(int maxSize)
    {
      double value = Util.Rand() * double.MaxValue;
      m_cacheableObject = new CacheableDouble(value);
    }

    public override uint GetChecksum(IGFSerializable cacheableObject)
    {
      CacheableDouble value = cacheableObject as CacheableDouble;
      Assert.IsNotNull(value, "GetChecksum: Null object.");

      return CacheableHelper.CRC32(BitConverter.GetBytes(value.Value));
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

    public override uint GetChecksum(IGFSerializable cacheableObject)
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
      m_cacheableObject = new CacheableFloat(value);
    }

    public override void InitRandomValue(int maxSize)
    {
      float value = (float)Util.Rand() * float.MaxValue;
      m_cacheableObject = new CacheableFloat(value);
    }

    public override uint GetChecksum(IGFSerializable cacheableObject)
    {
      CacheableFloat value = cacheableObject as CacheableFloat;
      Assert.IsNotNull(value, "GetChecksum: Null object.");

      return CacheableHelper.CRC32(BitConverter.GetBytes(value.Value));
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
      m_cacheableObject = new CacheableInt16(value);
    }

    public override void InitRandomValue(int maxSize)
    {
      short value = (short)Util.Rand(short.MaxValue);
      m_cacheableObject = new CacheableInt16(value);
    }

    public override uint GetChecksum(IGFSerializable cacheableObject)
    {
      CacheableInt16 obj = cacheableObject as CacheableInt16;
      Assert.IsNotNull(obj, "GetChecksum: Null object.");

      short value = obj.Value;
      int numBytes = (int)sizeof(short);
      byte[] buffer = new byte[numBytes];
      for (int i = 0; i < numBytes; i++)
      {
        buffer[i] = (byte)(value & 0xff);
        value = (short)(value >> 8);
      }
      return CacheableHelper.CRC32(buffer);
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
      m_cacheableObject = new CacheableInt32(value);
    }

    public override void InitRandomValue(int maxSize)
    {
      int value = Util.Rand(int.MaxValue);
      m_cacheableObject = new CacheableInt32(value);
    }

    public override uint GetChecksum(IGFSerializable cacheableObject)
    {
      CacheableInt32 obj = cacheableObject as CacheableInt32;
      Assert.IsNotNull(obj, "GetChecksum: Null object.");

      int value = obj.Value;
      int numBytes = (int)sizeof(int);
      byte[] buffer = new byte[numBytes];
      for (int i = 0; i < numBytes; i++)
      {
        buffer[i] = (byte)(value & 0xff);
        value = value >> 8;
      }
      return CacheableHelper.CRC32(buffer);
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
      m_cacheableObject = new CacheableInt64(value);
    }

    public override void InitRandomValue(int maxSize)
    {
      long value = Util.Rand(int.MaxValue);
      value = (value << 32) + Util.Rand(int.MaxValue);
      m_cacheableObject = new CacheableInt64(value);
    }

    public override uint GetChecksum(IGFSerializable cacheableObject)
    {
      CacheableInt64 obj = cacheableObject as CacheableInt64;
      Assert.IsNotNull(obj, "GetChecksum: Null object.");

      long value = obj.Value;
      int numBytes = (int)sizeof(long);
      byte[] buffer = new byte[numBytes];
      for (int i = 0; i < numBytes; i++)
      {
        buffer[i] = (byte)(value & 0xff);
        value = value >> 8;
      }
      return CacheableHelper.CRC32(buffer);
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
        m_cacheableObject = new CacheableString(string.Empty);
      }
      else
      {
        string value = new string('A', maxSize - 10) + keyIndex.ToString("D10");
        m_cacheableObject = new CacheableString(value);
      }
    }

    public override void InitRandomValue(int maxSize)
    {
      Assert.Greater(maxSize, 0, "Size of value should be greater than zero.");
      maxSize = (maxSize / 2) > 1 ? (maxSize / 2) : 2;
      Util.Log("hitesh in cacheable string wrapper maxsize = " + maxSize);
      if (maxSize == 2)
      {
        m_cacheableObject = new CacheableString(string.Empty);
      }
      else
      {
        byte[] buffer = Util.RandBytes(maxSize);
        string value = BitConverter.ToString(buffer).Replace("-", string.Empty);
        Util.Log("hitesh in cacheable string wrapper " + value);
        m_cacheableObject = new CacheableString(value);
      }
    }

    public override uint GetChecksum(IGFSerializable cacheableObject)
    {
      CacheableString obj = cacheableObject as CacheableString;
      Assert.IsNotNull(obj, "GetChecksum: Null object.");

      string value = obj.Value;
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
        m_cacheableObject = new CacheableString(string.Empty);
      }
      else
      {
        string value = new string('\x0905', maxSize - 10) + keyIndex.ToString("D10");
        m_cacheableObject = new CacheableString(value);
      }
    }

    public override void InitRandomValue(int maxSize)
    {
      Assert.Greater(maxSize, 0, "Size of value should be greater than zero.");
      if (maxSize == 2)
      {
        m_cacheableObject = new CacheableString(string.Empty);
      }
      else
      {
        byte[] buffer = Util.RandBytes(maxSize / 2);
        char[] value = new char[maxSize / 2];
        for (int i = 0; i < maxSize / 2; i++)
        {
          value[i] = (char)((int)buffer[i] + 0x0901);
        }
        m_cacheableObject = new CacheableString(value);
      }
    }

    public override uint GetChecksum(IGFSerializable cacheableObject)
    {
      CacheableString obj = cacheableObject as CacheableString;
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
        m_cacheableObject = new CacheableString(string.Empty);
      }
      else
      {
        string value = new string('A', maxSize - 10) + keyIndex.ToString("D10");
        m_cacheableObject = new CacheableString(value);
      }
    }

    public override void InitRandomValue(int maxSize)
    {
      if (maxSize < 0xFFFF)
      {
        maxSize += 0xFFFF + 1;
      }
      Assert.Greater(maxSize, 0, "Size of value should be greater than zero.");
      if (maxSize < 0xFFFF+10)
      {
        m_cacheableObject = new CacheableString(string.Empty);
      }
      else
      {
        byte[] buffer = Util.RandBytes(maxSize);
        string value = BitConverter.ToString(buffer).Replace("-", string.Empty);
        m_cacheableObject = new CacheableString(value);
      }
    }

    public override uint GetChecksum(IGFSerializable cacheableObject)
    {
      CacheableString obj = cacheableObject as CacheableString;
      Assert.IsNotNull(obj, "GetChecksum: Null object.");

      string value = obj.Value;
      byte[] buffer = new byte[value.Length];
      for (int i = 0; i < value.Length; i++)
      {
        buffer[i] = (byte)value[i];
      }
      return CacheableHelper.CRC32(buffer);
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
        m_cacheableObject = new CacheableString(string.Empty);
      }
      else
      {
        string value = new string('\x0905', maxSize - 10) + keyIndex.ToString("D10");
        m_cacheableObject = new CacheableString(value);
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
        m_cacheableObject = new CacheableString(string.Empty);
      }
      else
      {
        byte[] buffer = Util.RandBytes(maxSize);
        char[] value = new char[maxSize];
        for (int i = 0; i < maxSize; i++)
        {
          value[i] = (char)((int)buffer[i] + 0x0901);
        }
        m_cacheableObject = new CacheableString(value);
      }
    }

    public override uint GetChecksum(IGFSerializable cacheableObject)
    {
      CacheableString obj = cacheableObject as CacheableString;
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
      m_cacheableObject = CacheableBytes.Create(value);
    }

    public override uint GetChecksum(IGFSerializable cacheableObject)
    {
      CacheableBytes value = cacheableObject as CacheableBytes;
      Assert.IsNotNull(value, "GetChecksum: Null object.");

      return CacheableHelper.CRC32(value.Value);
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
      int arraySize = maxSize / (int)sizeof(double);
      arraySize = arraySize > 1 ? arraySize : 2;
      double[] value = new double[arraySize];
      for (int arrayIndex = 0; arrayIndex < arraySize; arrayIndex++)
      {
        value[arrayIndex] = Util.Rand() * double.MaxValue;
      }
      m_cacheableObject = CacheableDoubleArray.Create(value);
    }

    public override uint GetChecksum(IGFSerializable cacheableObject)
    {
      CacheableDoubleArray obj = cacheableObject as CacheableDoubleArray;
      Assert.IsNotNull(obj, "GetChecksum: Null object.");

      double[] value = obj.Value;
      System.IO.MemoryStream ms = new System.IO.MemoryStream();
      foreach (double v in value)
      {
        byte[] buffer = BitConverter.GetBytes(v);
        ms.Write(buffer, 0, buffer.Length);
      }
      return CacheableHelper.CRC32(ms.ToArray());
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
      int arraySize = maxSize / (int)sizeof(float);
      arraySize = arraySize > 1 ? arraySize : 2;
      float[] value = new float[arraySize];
      for (int arrayIndex = 0; arrayIndex < arraySize; arrayIndex++)
      {
        value[arrayIndex] = (float)Util.Rand() * float.MaxValue;
      }
      m_cacheableObject = CacheableFloatArray.Create(value);
    }

    public override uint GetChecksum(IGFSerializable cacheableObject)
    {
      CacheableFloatArray obj = cacheableObject as CacheableFloatArray;
      Assert.IsNotNull(obj, "GetChecksum: Null object.");

      float[] value = obj.Value;
      System.IO.MemoryStream ms = new System.IO.MemoryStream();
      foreach (float v in value)
      {
        byte[] buffer = BitConverter.GetBytes(v);
        ms.Write(buffer, 0, buffer.Length);
      }
      return CacheableHelper.CRC32(ms.ToArray());
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
      CacheableHashMap map = new CacheableHashMap();

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
            map.Add(keyWrapper.CacheableKey, valueWrapper.Cacheable);
          }
          index++;
        }
      }
      m_cacheableObject = map;
    }

    public override uint GetChecksum(IGFSerializable cacheableObject)
    {
      CacheableHashMap value = cacheableObject as CacheableHashMap;
      Assert.IsNotNull(value, "GetChecksum: Null object.");

      uint ckSum = 0;
      CacheableKeyWrapper keyWrapper;
      CacheableWrapper valueWrapper;
      foreach (KeyValuePair<ICacheableKey, IGFSerializable> pair in value)
      {
        keyWrapper = CacheableWrapperFactory.CreateKeyInstance(
          pair.Key.ClassId);
        Assert.IsNotNull(keyWrapper, "CacheableHashMap.GetChecksum:" +
          " Could not create an instance of typeId [{0}].",
          pair.Key.ClassId);
        valueWrapper = CacheableWrapperFactory.CreateInstance(
          pair.Value.ClassId);
        Assert.IsNotNull(valueWrapper, "CacheableHashMap.GetChecksum:" +
          " Could not create an instance of typeId [{0}].",
          pair.Value.ClassId);

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
      CacheableHashTable table = new CacheableHashTable();

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
            table.Add(keyWrapper.CacheableKey, valueWrapper.Cacheable);
          }
          index++;
        }
      }
      m_cacheableObject = table;
    }

    public override uint GetChecksum(IGFSerializable cacheableObject)
    {
      CacheableHashMap value = cacheableObject as CacheableHashMap;
      Assert.IsNotNull(value, "GetChecksum: Null object.");

      uint ckSum = 0;
      CacheableKeyWrapper keyWrapper;
      CacheableWrapper valueWrapper;
      foreach (KeyValuePair<ICacheableKey, IGFSerializable> pair in value)
      {
        keyWrapper = CacheableWrapperFactory.CreateKeyInstance(
          pair.Key.ClassId);
        Assert.IsNotNull(keyWrapper, "CacheableHashTable.GetChecksum:" +
          " Could not create an instance of typeId [{0}].",
          pair.Key.ClassId);
        valueWrapper = CacheableWrapperFactory.CreateInstance(
          pair.Value.ClassId);
        Assert.IsNotNull(valueWrapper, "CacheableHashTable.GetChecksum:" +
          " Could not create an instance of typeId [{0}].",
          pair.Value.ClassId);

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
      CacheableIdentityHashMap map = new CacheableIdentityHashMap();

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

    public override uint GetChecksum(IGFSerializable cacheableObject)
    {
      CacheableHashMap value = cacheableObject as CacheableHashMap;
      Assert.IsNotNull(value, "GetChecksum: Null object.");

      uint ckSum = 0;
      CacheableKeyWrapper keyWrapper;
      CacheableWrapper valueWrapper;
      foreach (KeyValuePair<ICacheableKey, IGFSerializable> pair in value)
      {
        keyWrapper = CacheableWrapperFactory.CreateKeyInstance(
          pair.Key.ClassId);
        Assert.IsNotNull(keyWrapper, "CacheableIdentityHashMap.GetChecksum:" +
          " Could not create an instance of typeId [{0}].",
          pair.Key.ClassId);
        valueWrapper = CacheableWrapperFactory.CreateInstance(
          pair.Value.ClassId);
        Assert.IsNotNull(valueWrapper, "CacheableIdentityHashMap.GetChecksum:" +
          " Could not create an instance of typeId [{0}].",
          pair.Value.ClassId);

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

    public override uint GetChecksum(IGFSerializable cacheableObject)
    {
      CacheableHashSet value = cacheableObject as CacheableHashSet;
      Assert.IsNotNull(value, "GetChecksum: Null object.");

      uint ckSum = 0;
      CacheableKeyWrapper wrapper;
      foreach (ICacheableKey key in value)
      {
        wrapper = CacheableWrapperFactory.CreateKeyInstance(key.ClassId);
        Assert.IsNotNull(wrapper, "CacheableHashSet.GetChecksum:" +
          " Could not create an instance of typeId [{0}].", key.ClassId);
        ckSum ^= wrapper.GetChecksum(key);
      }
      return ckSum;
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

    public override uint GetChecksum(IGFSerializable cacheableObject)
    {
      CacheableLinkedHashSet value = cacheableObject as CacheableLinkedHashSet;
      Assert.IsNotNull(value, "GetChecksum: Null object.");

      uint ckSum = 0;
      CacheableKeyWrapper wrapper;
      foreach (ICacheableKey key in value)
      {
        wrapper = CacheableWrapperFactory.CreateKeyInstance(key.ClassId);
        Assert.IsNotNull(wrapper, "CacheableLinkedHashSet.GetChecksum:" +
          " Could not create an instance of typeId [{0}].", key.ClassId);
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
      int arraySize = maxSize / (int)sizeof(short);
      arraySize = arraySize > 1 ? arraySize : 2;
      short[] value = new short[arraySize];
      for (int arrayIndex = 0; arrayIndex < arraySize; arrayIndex++)
      {
        value[arrayIndex] = (short)Util.Rand(short.MaxValue);
      }
      m_cacheableObject = CacheableInt16Array.Create(value);
    }

    public override uint GetChecksum(IGFSerializable cacheableObject)
    {
      CacheableInt16Array obj = cacheableObject as CacheableInt16Array;
      Assert.IsNotNull(obj, "GetChecksum: Null object.");

      short[] value = obj.Value;
      System.IO.MemoryStream ms = new System.IO.MemoryStream();
      foreach (short v in value)
      {
        short conv = v;
        int numBytes = (int)sizeof(short);
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
      int arraySize = maxSize / (int)sizeof(int);
      arraySize = arraySize > 1 ? arraySize : 2;
      int[] value = new int[arraySize];
      for (int arrayIndex = 0; arrayIndex < arraySize; arrayIndex++)
      {
        value[arrayIndex] = Util.Rand(int.MaxValue);
      }
      m_cacheableObject = CacheableInt32Array.Create(value);
    }

    public override uint GetChecksum(IGFSerializable cacheableObject)
    {
      CacheableInt32Array obj = cacheableObject as CacheableInt32Array;
      Assert.IsNotNull(obj, "GetChecksum: Null object.");

      int[] value = obj.Value;
      System.IO.MemoryStream ms = new System.IO.MemoryStream();
      foreach (int v in value)
      {
        int conv = v;
        int numBytes = (int)sizeof(int);
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
      int arraySize = maxSize / (int)sizeof(long);
      arraySize = arraySize > 1 ? arraySize : 2;
      long[] value = new long[arraySize];
      long rnd;
      for (int arrayIndex = 0; arrayIndex < arraySize; arrayIndex++)
      {
        rnd = (long)Util.Rand(int.MaxValue);
        rnd = (rnd << 32) + (long)Util.Rand(int.MaxValue);
        value[arrayIndex] = rnd;
      }
      m_cacheableObject = CacheableInt64Array.Create(value);
    }

    public override uint GetChecksum(IGFSerializable cacheableObject)
    {
      CacheableInt64Array obj = cacheableObject as CacheableInt64Array;
      Assert.IsNotNull(obj, "GetChecksum: Null object.");

      long[] value = obj.Value;
      System.IO.MemoryStream ms = new System.IO.MemoryStream();
      foreach (long v in value)
      {
        long conv = v;
        int numBytes = (int)sizeof(long);
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
      m_cacheableObject = CacheableString.Create((string)null);
    }

    public override uint GetChecksum(IGFSerializable cacheableObject)
    {
      CacheableString value = cacheableObject as CacheableString;
      Assert.IsNull(value, "GetChecksum: expected null object.");
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
      m_cacheableObject = CacheableString.Create(string.Empty);
    }

    public override uint GetChecksum(IGFSerializable cacheableObject)
    {
      CacheableString value = cacheableObject as CacheableString;
      Assert.IsNotNull(value, "GetChecksum: expected non null object.");
      Assert.IsTrue(CacheableString.IsNullOrEmpty(value), "Expected IsNullOrEmpty");
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
      m_cacheableObject = CacheableString.Create(string.Empty);
    }

    public override uint GetChecksum(IGFSerializable cacheableObject)
    {
      CacheableString value = cacheableObject as CacheableString;
      Assert.IsNotNull(value, "GetChecksum: expected non null object.");
      Assert.IsTrue(CacheableString.IsNullOrEmpty(value), "Expected IsNullOrEmpty");
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
      CacheableString[] value = new CacheableString[arraySize];
      for (int arrayIndex = 0; arrayIndex < arraySize; arrayIndex++)
      {
        if (arrayIndex % 2 == 0)
        {
          byte[] buffer = Util.RandBytes(maxSize / 2);
          value[arrayIndex] = new CacheableString(
            BitConverter.ToString(buffer).Replace("-", string.Empty));
        }
        else
        {
          byte[] buffer = Util.RandBytes(maxSize / 2);
          char[] charArray = new char[maxSize / 2];
          for (int i = 0; i < maxSize / 2; i++)
          {
            charArray[i] = (char)((int)buffer[i] + 0x0901);
          }
          value[arrayIndex] = new CacheableString(charArray);
        }
      }
      m_cacheableObject = CacheableStringArray.Create(value);
    }

    public override uint GetChecksum(IGFSerializable cacheableObject)
    {
      CacheableStringArray value = cacheableObject as CacheableStringArray;
      Assert.IsNotNull(value, "GetChecksum: Null object.");

      System.IO.MemoryStream ms = new System.IO.MemoryStream();
      foreach (CacheableString str in value.GetValues())
      {
        if (str.IsWideString)
        {
          foreach (char c in str.Value)
          {
            ms.WriteByte((byte)(c & 0xff));
            ms.WriteByte((byte)((c >> 8) & 0xff));
          }
        }
        else
        {
          foreach (char c in str.Value)
          {
            ms.WriteByte((byte)c);
          }
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

    public override uint GetChecksum(IGFSerializable cacheableObject)
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
      CacheableVector vec = new CacheableVector();

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

    public override uint GetChecksum(IGFSerializable cacheableObject)
    {
      CacheableVector value = cacheableObject as CacheableVector;
      Assert.IsNotNull(value, "GetChecksum: Null object.");

      uint ckSum = 0;
      CacheableWrapper wrapper;
      foreach (IGFSerializable obj in value)
      {
        if (obj == null)
        {
          continue;
        }
        wrapper = CacheableWrapperFactory.CreateInstance(obj.ClassId);
        Assert.IsNotNull(wrapper, "CacheableVectorWrapper.GetChecksum:" +
          " Could not create an instance of typeId [{0}].", obj.ClassId);
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

    public override uint GetChecksum(IGFSerializable cacheableObject)
    {
      CacheableObjectArray value = cacheableObject as CacheableObjectArray;
      Assert.IsNotNull(value, "GetChecksum: Null object.");

      uint ckSum = 0;
      CacheableWrapper wrapper;
      foreach (IGFSerializable obj in value)
      {
        if (obj == null)
        {
          continue;
        }
        wrapper = CacheableWrapperFactory.CreateInstance(obj.ClassId);
        Assert.IsNotNull(wrapper, "CacheableObjectArrayWrapper.GetChecksum:" +
          " Could not create an instance of typeId [{0}].", obj.ClassId);
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
      CacheableArrayList arrayList = new CacheableArrayList();

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
          Assert.IsNotNull(wrapper, "CacheableArrayListWrapper.InitRandomValue:" +
            " Could not create an instance of typeId [{0}].", typeId);
          wrapper.InitRandomValue(maxSize);
          arrayList.Add(wrapper.Cacheable);
        }
      }
      m_cacheableObject = arrayList;
    }

    public override uint GetChecksum(IGFSerializable cacheableObject)
    {
      CacheableArrayList value = cacheableObject as CacheableArrayList;
      Assert.IsNotNull(value, "GetChecksum: Null object.");

      uint ckSum = 0;
      CacheableWrapper wrapper;
      foreach (IGFSerializable obj in value)
      {
        if (obj == null)
        {
          continue;
        }
        wrapper = CacheableWrapperFactory.CreateInstance(obj.ClassId);
        Assert.IsNotNull(wrapper, "CacheableArrayListWrapper.GetChecksum:" +
          " Could not create an instance of typeId [{0}].", obj.ClassId);
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
      CacheableStack vec = new CacheableStack();

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

    public override uint GetChecksum(IGFSerializable cacheableObject)
    {
      CacheableStack value = cacheableObject as CacheableStack;
      Assert.IsNotNull(value, "GetChecksum: Null object.");

      uint ckSum = 0;
      CacheableWrapper wrapper;
      foreach (IGFSerializable obj in value)
      {
        if (obj == null)
        {
          continue;
        }
        wrapper = CacheableWrapperFactory.CreateInstance(obj.ClassId);
        Assert.IsNotNull(wrapper, "CacheableStackWrapper.GetChecksum:" +
          " Could not create an instance of typeId [{0}].", obj.ClassId);
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

    public override uint GetChecksum(IGFSerializable cacheableObject)
    {
      CacheableObject obj = cacheableObject as CacheableObject;
      Assert.IsNotNull(obj, "GetChecksum: Null object.");

      CData value = (CData)obj.Value;
      System.IO.MemoryStream ms = new System.IO.MemoryStream();
      int first = value.First;
      int numBytes = (int)sizeof(int);
      for (int i = 0; i < numBytes; i++)
      {
        ms.WriteByte((byte)(first & 0xff));
        first = first >> 8;
      }
      long second = value.Second;
      numBytes = (int)sizeof(long);
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

    public override uint GetChecksum(IGFSerializable cacheableObject)
    {
      CacheableObjectXml obj = cacheableObject as CacheableObjectXml;
      Assert.IsNotNull(obj, "GetChecksum: Null object.");

      CData value = (CData)obj.Value;
      System.IO.MemoryStream ms = new System.IO.MemoryStream();
      int first = value.First;
      int numBytes = (int)sizeof(int);
      for (int i = 0; i < numBytes; i++)
      {
        ms.WriteByte((byte)(first & 0xff));
        first = first >> 8;
      }
      long second = value.Second;
      numBytes = (int)sizeof(long);
      for (int i = 0; i < numBytes; i++)
      {
        ms.WriteByte((byte)(second & 0xff));
        second = second >> 8;
      }
      return CacheableHelper.CRC32(ms.ToArray());
    }

    #endregion
  }

  #endregion
}
