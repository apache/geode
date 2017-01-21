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

#pragma once

namespace Apache
{
  namespace Geode
  {
    namespace Client
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
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache

}