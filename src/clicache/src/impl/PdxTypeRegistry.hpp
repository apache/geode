/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once
#include "PdxType.hpp"
#include "PdxRemotePreservedData.hpp"
#include "../IPdxSerializable.hpp"
#include "WeakhashMap.hpp"
#include "EnumInfo.hpp"
using namespace System;
using namespace System::Threading;
using namespace System::Collections::Generic;

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
        public ref class PdxTypeRegistry
        {
        public:
					//test hook;
					static int testGetNumberOfPdxIds();

					//test hook
					static int testNumberOfPreservedData();

          static void AddPdxType(Int32 typeId, PdxType^ pdxType);

          static PdxType^ GetPdxType(Int32 typeId);

          static void AddLocalPdxType(String^ localType, PdxType^ pdxType);

          static PdxType^ GetLocalPdxType(String^ localType);

          static void SetMergedType(Int32 remoteTypeId, PdxType^ mergedType);

          static PdxType^ GetMergedType(Int32 remoteTypeId);

          static void SetPreserveData(IPdxSerializable^ obj, PdxRemotePreservedData^ preserveData);

          static PdxRemotePreservedData^ GetPreserveData(IPdxSerializable^ obj);      

          static void clear();

          static Int32 GetPDXIdForType(Type^ type, const char* poolname, PdxType^ nType, bool checkIfThere);

          static Int32 GetPDXIdForType(PdxType^ type, const char* poolname);

					static property bool PdxIgnoreUnreadFields
					{
						bool get() {return pdxIgnoreUnreadFields;}
						void set(bool value){pdxIgnoreUnreadFields = value;}
					}

          static property bool PdxReadSerialized
					{
						bool get() {return pdxReadSerialized;}
						void set(bool value){pdxReadSerialized= value;}
					}

          static Int32 GetEnumValue(EnumInfo^ ei);

          static EnumInfo^ GetEnum(Int32 enumVal);

        private:

          static IDictionary<Int32, PdxType^>^ typeIdToPdxType = gcnew Dictionary<Int32, PdxType^>();

          static IDictionary<PdxType^, Int32>^ pdxTypeToTypeId = gcnew Dictionary<PdxType^, Int32>();

          static IDictionary<Int32, PdxType^>^ remoteTypeIdToMergedPdxType = gcnew Dictionary<Int32, PdxType^>();

          static IDictionary<String^, PdxType^>^ localTypeToPdxType = gcnew Dictionary<String^, PdxType^>();

          static IDictionary<EnumInfo^, Int32>^ enumToInt = gcnew Dictionary<EnumInfo^, Int32>();

          static IDictionary<Int32, EnumInfo^>^ intToEnum = gcnew Dictionary<Int32, EnumInfo^>();

          //TODO: this will be weak hashmap
          //static IDictionary<IPdxSerializable^ , PdxRemotePreservedData^>^ preserveData = gcnew Dictionary<IPdxSerializable^ , PdxRemotePreservedData^>();
          static WeakHashMap^ preserveData = gcnew WeakHashMap();          

          static ReaderWriterLock^ g_readerWriterLock = gcnew ReaderWriterLock();

					static bool pdxIgnoreUnreadFields = false;
          static bool pdxReadSerialized = false;
        };
      }
			}
    }
  }
}