/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "PdxFieldType.hpp"
//#include "../DataOutput.hpp"
//#include "../DataInput.hpp"
#include "../GemFireClassIds.hpp"
using namespace System;
using namespace System::Collections::Generic;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      namespace Generic
      {
				ref class DataOutput;
				ref class DataInput;
      namespace Internal
      {
        public ref class PdxType : public IGFSerializable
        {
        private:
          Object^                 m_lockObj;
          static const String^    m_javaPdxClass = "org.apache.geode.pdx.internal.PdxType";
          IList<PdxFieldType^>^   m_pdxFieldTypes;
          IList<PdxType^>^        m_otherVersions;
          Int32                   m_cachedHashcode;
          
          //Type^                 m_pdxDomainType;
          String^               m_className;
          Int32                   m_gemfireTypeId;
          bool                    m_isLocal;
          Int32                   m_numberOfVarLenFields;
          Int32                   m_varLenFieldIdx;

          Int32                   m_numberOfFieldsExtra;
          
          array<Int32>^           m_remoteToLocalFieldMap;
          array<Int32>^           m_localToRemoteFieldMap;

          array<Int32, 2>^        m_positionMap;
          IDictionary<String^, PdxFieldType^>^ m_fieldNameVsPdxType;

          bool                   m_isVarLenFieldAdded;
					bool									 m_noJavaClass;
        
					void initRemoteToLocal();
          void initLocalToRemote();
          //first has more fields than second
          PdxType^ isContains(PdxType^ first, PdxType^ second);
          PdxType^ clone();

          void generatePositionMap();
          Int32 variableLengthFieldPosition(PdxFieldType^ varLenField, uint8_t* offsetPosition, Int32 offsetSize, Int32 pdxStreamlen);
          Int32 fixedLengthFieldPosition(PdxFieldType^ fixLenField, uint8_t* offsetPosition, Int32 offsetSize, Int32 pdxStreamlen);

          PdxType^ isLocalTypeContains(PdxType^ otherType);
          PdxType^ isRemoteTypeContains(PdxType^ localType);
         public:
           PdxType()
           {
             m_cachedHashcode = 0;
             m_lockObj = gcnew Object();
             m_pdxFieldTypes = gcnew List<PdxFieldType^>();
             m_otherVersions = gcnew List<PdxType^>();
             m_isLocal = false;
             m_numberOfVarLenFields = 0;
             m_varLenFieldIdx = 0;//start with 0
             m_isVarLenFieldAdded = false;
             m_fieldNameVsPdxType = gcnew Dictionary<String^, PdxFieldType^>();
						 m_noJavaClass = false;
             m_gemfireTypeId = 0;
            // m_pdxDomainType = nullptr;
           }

           PdxType(String^ pdxDomainClassName,
                   bool  isLocal )
           {
             m_cachedHashcode = 0;
              m_lockObj = gcnew Object();
              m_pdxFieldTypes = gcnew List<PdxFieldType^>();
              m_otherVersions = gcnew List<PdxType^>();
            //  m_className = className;
             // m_pdxDomainType = pdxDomainType;
              m_className = pdxDomainClassName;
              m_isLocal = isLocal;
              m_numberOfVarLenFields = 0;
              m_varLenFieldIdx = 0;//start with 0
              m_isVarLenFieldAdded = false;
              m_fieldNameVsPdxType = gcnew Dictionary<String^, PdxFieldType^>();
							m_noJavaClass = false;
              m_gemfireTypeId = 0;
           }

           static IGFSerializable^ CreateDeserializable()
           {
             return gcnew PdxType();
           }
           property Int32 TotalVarLenFields
           {
             Int32 get() {return m_numberOfVarLenFields;};
           }
					 property Int32 Totalfields
					 {
						 Int32 get() {return m_pdxFieldTypes->Count;};
					 }
           property IList<PdxFieldType^>^ PdxFieldList
           {
             IList<PdxFieldType^>^ get(){return m_pdxFieldTypes;}
           }
           property Int32 TypeId
           {
             Int32 get() {return m_gemfireTypeId;}
             void set(Int32 value) {m_gemfireTypeId = value;}
           }
          /* property Type^ PdxDomainType
           {
             Type^ get() {return m_pdxDomainType;}
             void set(Type^ type) {m_pdxDomainType = type;}
           }*/
           property String^ PdxClassName
           {
             String^ get() {return m_className;}
             void set(String^ className) {m_className = className;}
           }
           property Int32 NumberOfFieldsExtra
           {
             Int32 get(){return m_numberOfFieldsExtra;}
           }

           PdxFieldType^ GetPdxField(String^ fieldName)
           {
             PdxFieldType^ retVal= nullptr;

             m_fieldNameVsPdxType->TryGetValue(fieldName, retVal);

             return retVal;
           }
           void AddOtherVersion(PdxType^ otherVersion)
           {
            m_otherVersions->Add(otherVersion);
           }

           array<int>^ GetLocalToRemoteMap();
           array<int>^ GetRemoteToLocalMap();
           property Int32 NumberOfVarLenFields
           {
             Int32 get(){return m_numberOfVarLenFields;}
           }
           property bool IsLocal
           {
             bool get() {return m_isLocal;}
             void set(bool val) {m_isLocal = val;}
           }
           virtual void ToData( DataOutput^ output );
           virtual IGFSerializable^ FromData( DataInput^ input );
           virtual property uint32_t ObjectSize
           {
             uint32_t get( ){return 0;}
           }
           virtual property uint32_t ClassId
           {
             uint32_t get( ){return GemFireClassIds::PdxType;}
           }
           virtual String^ ToString( ) override
           {
            return "PdxType";
           }
           void AddFixedLengthTypeField(String^ fieldName, String^ className, Byte typeId, Int32 size);
           void AddVariableLengthTypeField(String^ fieldName, String^ className, Byte typeId);
           void InitializeType();
           PdxType^ MergeVersion(PdxType^ otherVersion);
           Int32 GetFieldPosition(String^ fieldName, uint8_t* offsetPosition, Int32 offsetSize, Int32 pdxStreamlen);
           Int32 GetFieldPosition(Int32 fieldIdx, uint8_t* offsetPosition, Int32 offsetSize, Int32 pdxStreamlen);

           virtual bool Equals(Object^ otherType) override;
           virtual Int32 GetHashCode() override;
        };
			}
      }
    }
  }
}
