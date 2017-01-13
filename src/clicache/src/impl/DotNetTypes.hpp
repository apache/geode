/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      namespace Generic
      {
        namespace Internal
        {
          public ref class DotNetTypes sealed
          {
          public:
              static Type^ IntType = Int32::typeid;
              static Type^ StringType = String::typeid;
              static Type^ BooleanType = Boolean::typeid;
              static Type^ FloatType = float::typeid;
              static Type^ DoubleType = Double::typeid;
              static Type^ CharType = Char::typeid;
              static Type^ SByteType = SByte::typeid;
              static Type^ ShortType = Int16::typeid;
              static Type^ LongType = Int64::typeid;
              static Type^ ByteArrayType = Type::GetType("System.Byte[]");
              static Type^ DoubleArrayType = Type::GetType("System.Double[]");
              static Type^ FloatArrayType = Type::GetType("System.Single[]");
              static Type^ ShortArrayType = Type::GetType("System.Int16[]");
              static Type^ IntArrayType = Type::GetType("System.Int32[]");
              static Type^ LongArrayType = Type::GetType("System.Int64[]");
              static Type^ BoolArrayType = Type::GetType("System.Boolean[]");
              static Type^ CharArrayType = Type::GetType("System.Char[]");
              static Type^ StringArrayType = Type::GetType("System.String[]");
              static Type^ DateType = Type::GetType("System.DateTime");
              static Type^ ByteArrayOfArrayType = Type::GetType("System.Byte[][]");
              static Type^ ObjectArrayType = Type::GetType("System.Collections.Generic.List`1[System.Object]");

              static Type^ VoidType = Type::GetType("System.Void");
              static Type^ ObjectType = Type::GetType("System.Object");
          };
        }
      }
    }
  }
}