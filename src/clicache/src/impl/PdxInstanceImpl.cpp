/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once
#include "PdxInstanceImpl.hpp"
#include "PdxHelper.hpp"
#include "PdxTypeRegistry.hpp"
#include "../GemFireClassIds.hpp"
#include "PdxType.hpp"
#include "PdxLocalWriter.hpp"
#include "../DataInput.hpp"
#include "DotNetTypes.hpp"
#include <CacheRegionHelper.hpp>
#include <gfcpp/Cache.hpp>
#include <CacheImpl.hpp>
#include "PdxType.hpp"
using namespace System::Text;

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
          //this is for PdxInstanceFactory
          PdxInstanceImpl::PdxInstanceImpl(Dictionary<String^, Object^>^ fieldVsValue, PdxType^ pdxType)
          {
            m_updatedFields = fieldVsValue;
            m_typeId = 0;
            m_own = false;
            m_buffer = NULL;
            m_bufferLength = 0;
            m_pdxType = pdxType;
            
            m_pdxType->InitializeType();//to generate static position map

            //need to initiailize stream. this will call todata and in toData we will have stream
            gemfire::DataOutput* output = gemfire::DataOutput::getDataOutput();
            
            try
            {
              GemStone::GemFire::Cache::Generic::DataOutput mg_output( &(*output), true );
              GemStone::GemFire::Cache::Generic::Internal::PdxHelper::SerializePdx(%mg_output, this);
            }
            finally
            {
              gemfire::DataOutput::releaseDataOutput(output);
            }
          }

          String^ PdxInstanceImpl::GetClassName()
          {
            if(m_typeId != 0 )
            {
              PdxType^ pdxtype = Internal::PdxTypeRegistry::GetPdxType(m_typeId);
              if(pdxtype == nullptr)//will it ever happen
                throw gcnew IllegalStateException("PdxType is not defined for PdxInstance: " + m_typeId);
              return pdxtype->PdxClassName;
            }
            //will it ever happen
            throw gcnew IllegalStateException("PdxInstance typeid is not defined yet, to get classname." );
          }
        Object^ PdxInstanceImpl::GetObject()
        {
          DataInput^ dataInput = gcnew DataInput(m_buffer, m_bufferLength);
          dataInput->setRootObjectPdx(true);
          int64 sampleStartNanos =Utils::startStatOpTime();
          Object^ ret = Internal::PdxHelper::DeserializePdx(dataInput, true, m_typeId, m_bufferLength);
          //dataInput->ResetPdx(0);

          CachePtr cache = CacheFactory::getAnyInstance();
          if (cache == NULLPTR)
          {
            throw gcnew IllegalStateException("cache has not been created yet.");;
          }
          if (cache->isClosed())
          {
            throw gcnew IllegalStateException("cache has been closed. ");            
          }
          CacheImpl* cacheImpl = CacheRegionHelper::getCacheImpl(cache.ptr());
          if (cacheImpl != NULL) {          
            Utils::updateStatOpTime(cacheImpl->m_cacheStats->getStat(),
              cacheImpl->m_cacheStats->getPdxInstanceDeserializationTimeId(),
              sampleStartNanos);
            cacheImpl->m_cacheStats->incPdxInstanceDeserializations();		
          }
          return ret;
        }

        bool PdxInstanceImpl::HasField(String^ fieldName)
        {
          PdxType^ pt = getPdxType();
          return pt->GetPdxField(fieldName) != nullptr;
        }
          
        IList<String^>^ PdxInstanceImpl::GetFieldNames()
        {
          PdxType^ pt = getPdxType();

          IList<PdxFieldType^>^ pdxFieldList = pt->PdxFieldList;
          IList<String^>^ retList = gcnew List<String^>();

          for(int i =0; i < pdxFieldList->Count; i++)
          {
            PdxFieldType^ currPf = pdxFieldList[i];
            retList->Add(currPf->FieldName);
          }

          return retList;
        }
          
        bool PdxInstanceImpl::IsIdentityField(String^ fieldName)
        {
          PdxType^ pt = getPdxType();
          PdxFieldType^ pft = pt->GetPdxField(fieldName);

          return pft != nullptr && pft->IdentityField;
        }

        Object^ PdxInstanceImpl::GetField(String^ fieldName)
        {
          PdxType^ pt = getPdxType();

          PdxFieldType^ pft = pt->GetPdxField(fieldName);

          if(pft == nullptr)
          {
            // throw gcnew IllegalStateException("PdxInstance doesn't has field " + fieldName);    
            return nullptr;
          }

          {
            DataInput^ dataInput = gcnew DataInput(m_buffer, m_bufferLength);
            dataInput->setPdxdeserialization(true);
            
            int pos = getOffset(dataInput, pt, pft->SequenceId);
            //Log::Debug("PdxInstanceImpl::GetField object pos " + (pos + 8) );
            dataInput->ResetAndAdvanceCursorPdx(pos);

            Object^ tmp = this->readField(dataInput, fieldName, pft->TypeId);

            //dataInput->ResetPdx(0);

            return tmp;
          }
          return nullptr;
        }

        void PdxInstanceImpl::setOffsetForObject(DataInput^ dataInput, PdxType^ pt, int sequenceId)
        {
          int pos = getOffset(dataInput, pt, sequenceId);
          dataInput->ResetAndAdvanceCursorPdx(pos);
        }

        int PdxInstanceImpl::getOffset(DataInput^ dataInput, PdxType^ pt, int sequenceId)
        {
          dataInput->ResetPdx(0);

          int offsetSize = 0;
          int serializedLength = 0;
          int pdxSerializedLength = dataInput->GetPdxBytes();
          if(pdxSerializedLength <= 0xff)
            offsetSize = 1;
          else if(pdxSerializedLength <= 0xffff)
            offsetSize = 2;
          else
            offsetSize = 4;

          if(pt->NumberOfVarLenFields > 0)
            serializedLength = pdxSerializedLength - ((pt->NumberOfVarLenFields -1) * offsetSize);
          else
            serializedLength = pdxSerializedLength; 

          uint8_t* offsetsBuffer = dataInput->GetCursor() + serializedLength;

          return pt->GetFieldPosition(sequenceId, offsetsBuffer, offsetSize, serializedLength);
        }

        int PdxInstanceImpl::getSerializedLength(DataInput^ dataInput, PdxType^ pt)
        {
           dataInput->ResetPdx(0);

          int offsetSize = 0;
          int serializedLength = 0;
          int pdxSerializedLength = dataInput->GetPdxBytes();
          if(pdxSerializedLength <= 0xff)
            offsetSize = 1;
          else if(pdxSerializedLength <= 0xffff)
            offsetSize = 2;
          else
            offsetSize = 4;

          if(pt->NumberOfVarLenFields > 0)
            serializedLength = pdxSerializedLength - ((pt->NumberOfVarLenFields -1) * offsetSize);
          else
            serializedLength = pdxSerializedLength; 

          return serializedLength;
        }

        bool PdxInstanceImpl::Equals(Object^ other)
        {
          if(other == nullptr)
            return false;

          PdxInstanceImpl^ otherPdx = dynamic_cast<PdxInstanceImpl^>(other);
  
          if(otherPdx == nullptr)
            return false;

          PdxType^ myPdxType = getPdxType();
          PdxType^ otherPdxType = otherPdx->getPdxType();

          if(!otherPdxType->PdxClassName->Equals(myPdxType->PdxClassName))
            return false;

          int hashCode = 1;
          
          //PdxType^ pt = getPdxType();
          
          IList<PdxFieldType^>^ myPdxIdentityFieldList = getIdentityPdxFields(myPdxType);
          IList<PdxFieldType^>^ otherPdxIdentityFieldList = otherPdx->getIdentityPdxFields(otherPdxType);

          equatePdxFields(myPdxIdentityFieldList, otherPdxIdentityFieldList);
          equatePdxFields(otherPdxIdentityFieldList, myPdxIdentityFieldList);

          DataInput^ myDataInput = gcnew DataInput(m_buffer, m_bufferLength);
          myDataInput->setPdxdeserialization(true);
          DataInput^ otherDataInput = gcnew DataInput(otherPdx->m_buffer, otherPdx->m_bufferLength);
          otherDataInput->setPdxdeserialization(true);

          bool isEqual = false;
          int fieldTypeId = -1;
          for(int i =0; i< myPdxIdentityFieldList->Count; i++)
          {
            PdxFieldType^ myPFT = myPdxIdentityFieldList[i];
            PdxFieldType^ otherPFT = otherPdxIdentityFieldList[i];

           // Log::Debug("pdxfield " + ((myPFT != Default_PdxFieldType)? myPFT->FieldName: otherPFT->FieldName));
            if(myPFT == Default_PdxFieldType)
            {
              fieldTypeId = otherPFT->TypeId;
              /*Object^ val = otherPdx->GetField(otherPFT->FieldName);
              if(val == nullptr || (int)val == 0 || (bool)val == false)
                continue;*/
            }
            else if(otherPFT == Default_PdxFieldType)
            {
              fieldTypeId = myPFT->TypeId;
              /*Object^ val = this->GetField(myPFT->FieldName);
              if(val == nullptr || (int)val == 0 || (bool)val == false)
                continue;*/
            }
            else
            {
              fieldTypeId = myPFT->TypeId;
            }

            switch (fieldTypeId) 
            {
              case PdxTypes::CHAR:
              case PdxTypes::BOOLEAN:
              case PdxTypes::BYTE:
              case PdxTypes::SHORT:
              case PdxTypes::INT:
              case PdxTypes::LONG:
              case PdxTypes::DATE:
              case PdxTypes::FLOAT:
              case PdxTypes::DOUBLE:
              case PdxTypes::STRING:
              case PdxTypes::BOOLEAN_ARRAY:
              case PdxTypes::CHAR_ARRAY:
              case PdxTypes::BYTE_ARRAY:
              case PdxTypes::SHORT_ARRAY:
              case PdxTypes::INT_ARRAY:
              case PdxTypes::LONG_ARRAY:
              case PdxTypes::FLOAT_ARRAY:
              case PdxTypes::DOUBLE_ARRAY:
              case PdxTypes::STRING_ARRAY:
              case PdxTypes::ARRAY_OF_BYTE_ARRAYS: 
                {
                  if(!compareRawBytes(otherPdx, myPdxType, myPFT,myDataInput, otherPdxType, otherPFT, otherDataInput))
                    return false;
                  break;
                }
              case PdxTypes::OBJECT:
                {
                  Object^ object = nullptr;
                  Object^ otherObject = nullptr;
                  if(myPFT != Default_PdxFieldType)
                  {
                    setOffsetForObject(myDataInput,myPdxType, myPFT->SequenceId);
                    object = myDataInput->ReadObject();
                  }
                  
                  if(otherPFT != Default_PdxFieldType)
                  {
                    otherPdx->setOffsetForObject(otherDataInput, otherPdxType, otherPFT->SequenceId);
                    otherObject = otherDataInput->ReadObject();
                  }


                  if(object!= nullptr )
                  {
                    if(object->GetType()->IsArray)
                    {
                      if(object->GetType()->GetElementType()->IsPrimitive)//primitive type
                      {
                       if(!compareRawBytes(otherPdx, myPdxType, myPFT, myDataInput, otherPdxType, otherPFT, otherDataInput))
                        return false;
                      }
                      else//array of objects
                      {
                        if(!deepArrayEquals(object, otherObject))
                          return false;
                      }
                    }
                    else//object but can be hashtable, list etc
                    {
                      if(!deepArrayEquals(object, otherObject))
                        return false;
                    }
                  }
                  else if(otherObject != nullptr)
                  {
                    return false;
                    //hashCode = 31 * hashCode; // this may be issue 
                  }
                  
                  break;
                }
              case PdxTypes::OBJECT_ARRAY:
                {
                  Object^ objectArray = nullptr;
                  Object^ otherObjectArray = nullptr;

                  if(myPFT != Default_PdxFieldType)
                  {
                    setOffsetForObject(myDataInput, myPdxType, myPFT->SequenceId);
                    objectArray = myDataInput->ReadObjectArray();
                  }

                  if(otherPFT != Default_PdxFieldType)
                  {
                    otherPdx->setOffsetForObject(otherDataInput,otherPdxType, otherPFT->SequenceId);
                    otherObjectArray = otherDataInput->ReadObjectArray();
                  }

                  if(!deepArrayEquals(objectArray, otherObjectArray))
                    return false;
                  break;              
                }
              default:
                {
                  throw gcnew IllegalStateException("PdxInstance not found typeid " + myPFT->TypeId);
                }
            }

          }
          return true;
        }

        bool PdxInstanceImpl::compareRawBytes(PdxInstanceImpl^ other, PdxType^ myPT,  PdxFieldType^ myF,DataInput^ myDataInput, PdxType^ otherPT,  PdxFieldType^ otherF, DataInput^ otherDataInput)
        {
          if(myF != Default_PdxFieldType && otherF != Default_PdxFieldType)
          {
            int pos = getOffset(myDataInput, myPT, myF->SequenceId);
            int nextpos = getNextFieldPosition(myDataInput, myF->SequenceId+1, myPT);
            myDataInput->ResetAndAdvanceCursorPdx(pos);
            
            int otherPos = other->getOffset(otherDataInput, otherPT, otherF->SequenceId);
            int otherNextpos = other->getNextFieldPosition(otherDataInput, otherF->SequenceId+1, otherPT);
            otherDataInput->ResetAndAdvanceCursorPdx(otherPos);
            
            if( (nextpos - pos) != (otherNextpos - otherPos))
              return false;
            
            for(int i = pos; i < nextpos; i++)
            {
              if( myDataInput->ReadSByte() != otherDataInput->ReadSByte())
                return false;
            }
            //Log::Debug("compareRawBytes returns true" );
            return true;
          }
          else
          {
            DataInput^ tmpDI = nullptr;
            if(myF == Default_PdxFieldType)
            {
              int otherPos = other->getOffset(otherDataInput, otherPT, otherF->SequenceId);
              int otherNextpos = other->getNextFieldPosition(otherDataInput, otherF->SequenceId+1, otherPT);
              return hasDefaultBytes(otherF, otherDataInput,otherPos, otherNextpos ); 
            }
            else
            {
              int pos = getOffset(myDataInput, myPT, myF->SequenceId);
              int nextpos = getNextFieldPosition(myDataInput, myF->SequenceId+1, myPT);
              return hasDefaultBytes(myF, myDataInput,pos, nextpos ); 
            }
          }
        }

        void PdxInstanceImpl::equatePdxFields(IList<PdxFieldType^>^ my, IList<PdxFieldType^>^ other)
        {
          //Log::Debug("PdxInstanceImpl::equatePdxFields");

          for(int i = 0; i < my->Count; i++)
          {
            PdxFieldType^ myF = my[i];
            if(myF != Default_PdxFieldType)
            {
              Log::Debug("field name " + myF->ToString());
              int otherIdx = other->IndexOf(myF);

              if(otherIdx == -1)//field not there
              {
                if(i < other->Count)
                {
                  PdxFieldType^ tmp = other[i];
                  other[i] = Default_PdxFieldType;
                  other->Add(tmp);
                }
                else
                {
                  other->Add(Default_PdxFieldType);
                }
              }
              else if(otherIdx != i)
              {
                PdxFieldType^ tmp = other[i];
                other[i] = other[otherIdx];
                other[otherIdx] = tmp;
              }
            }
          }

          //if(my->Count != other->Count)
          //{
          //  for(int i = 0; i < other->Count; i++)
          //  {
          //    PdxFieldType^ otherF = other[i];
          //    int myIdx = my->IndexOf(otherF);
          //    if(myIdx == -1)//this is the field not there
          //    {
          //      my[i] = otherF;
          //    }
          //  }
          //}
        }

        bool PdxInstanceImpl::deepArrayEquals(Object^ obj, Object^ otherObj)
        {
          if(obj == nullptr && otherObj == nullptr)
            return true;
          else if(obj == nullptr && otherObj != nullptr)
            return false;
          else if(obj != nullptr && otherObj == nullptr)
            return false;

          Type^ objT = obj->GetType();
          Type^ otherObjT = otherObj->GetType();
          if(!objT->Equals(otherObjT))
            return false;

          if(objT->IsArray)
          {//array
            return enumerableEquals((System::Collections::IEnumerable^)obj, (System::Collections::IEnumerable^)otherObj);
          }
          else if(objT->GetInterface("System.Collections.IDictionary"))
          {//map
           // Log::Debug(" in map");
            return enumerateDictionaryForEqual((System::Collections::IDictionary^)obj, (System::Collections::IDictionary^)otherObj);
          }
          else if(objT->GetInterface("System.Collections.IList"))
          {//list
           // Log::Debug(" in list");
            return enumerableEquals((System::Collections::IEnumerable^)obj, (System::Collections::IEnumerable^)otherObj);
          }
          else
          {
           
            //  Log::Debug("final object hashcode " + obj->GetHashCode());

            return obj->Equals(otherObj);
          }
        }

        bool PdxInstanceImpl::enumerableEquals(System::Collections::IEnumerable^ enumObj, System::Collections::IEnumerable^ enumOtherObj)
        {
           if(enumObj == nullptr && enumOtherObj == nullptr)
            return true;
          else if(enumObj == nullptr && enumOtherObj != nullptr)
            return false;
          else if(enumObj != nullptr && enumOtherObj == nullptr)
            return false;


          System::Collections::IEnumerator^ my = enumObj->GetEnumerator();
          System::Collections::IEnumerator^ other = enumOtherObj->GetEnumerator();


          while(true)
          {
            bool m = my->MoveNext();
            bool o = other->MoveNext();
            if( m && o)
            {
              if(!my->Current->Equals(other->Current))
                return false;
            }
            else if(!m && !o)
              return true;
            else
              return false;
          }
         // Log::Debug(" in enumerableHashCode FINAL hc " + h);
          return true;
        }

        bool PdxInstanceImpl::enumerateDictionaryForEqual(System::Collections::IDictionary^ iDict, System::Collections::IDictionary^ otherIDict)
        {
           if(iDict == nullptr && otherIDict == nullptr)
            return true;
          else if(iDict == nullptr && otherIDict != nullptr)
            return false;
          else if(iDict != nullptr && otherIDict == nullptr)
            return false;

          if(iDict->Count != otherIDict->Count)
            return false;

          System::Collections::IDictionaryEnumerator^ dEnum = iDict->GetEnumerator();
          for each(System::Collections::DictionaryEntry^ de in iDict)
          {
            Object^ other = nullptr;
            if(otherIDict->Contains(de->Key))
            {
              if(!deepArrayEquals(de->Value, otherIDict[de->Key]))
                return false;
            }
            else
              return false;
          }
         // Log::Debug(" in enumerateDictionary FINAL hc " + h);
          return true;
        }

     

        int PdxInstanceImpl::GetHashCode()
        {
          int hashCode = 1;
          
          PdxType^ pt = getPdxType();
          
          IList<PdxFieldType^>^ pdxIdentityFieldList = getIdentityPdxFields(pt);

          DataInput^ dataInput = gcnew DataInput(m_buffer, m_bufferLength);
          dataInput->setPdxdeserialization(true);

          for(int i =0; i< pdxIdentityFieldList->Count; i++)
          {
            PdxFieldType^ pField = pdxIdentityFieldList[i];

            //Log::Debug("hashcode for pdxfield " + pField->FieldName + " hashcode is " + hashCode);
            switch (pField->TypeId) 
            {
              case PdxTypes::CHAR:
              case PdxTypes::BOOLEAN:
              case PdxTypes::BYTE:
              case PdxTypes::SHORT:
              case PdxTypes::INT:
              case PdxTypes::LONG:
              case PdxTypes::DATE:
              case PdxTypes::FLOAT:
              case PdxTypes::DOUBLE:
              case PdxTypes::STRING:
              case PdxTypes::BOOLEAN_ARRAY:
              case PdxTypes::CHAR_ARRAY:
              case PdxTypes::BYTE_ARRAY:
              case PdxTypes::SHORT_ARRAY:
              case PdxTypes::INT_ARRAY:
              case PdxTypes::LONG_ARRAY:
              case PdxTypes::FLOAT_ARRAY:
              case PdxTypes::DOUBLE_ARRAY:
              case PdxTypes::STRING_ARRAY:
              case PdxTypes::ARRAY_OF_BYTE_ARRAYS: 
                {
                  int retH = getRawHashCode(pt, pField, dataInput);
                  if(retH != 0)
                    hashCode = 31 * hashCode + retH;
                  break;
                }
              case PdxTypes::OBJECT:
                {
                  setOffsetForObject(dataInput, pt, pField->SequenceId);
                  Object^ object = dataInput->ReadObject();

                  if(object!= nullptr )
                  {
                    if(object->GetType()->IsArray)
                    {
                      if(object->GetType()->GetElementType()->IsPrimitive)//primitive type
                      {
                        int retH = getRawHashCode(pt, pField, dataInput);
                        if(retH != 0)
                          hashCode = 31 * hashCode + retH;
                      }
                      else//array of objects
                      {
                        hashCode = 31 * hashCode + deepArrayHashCode(object);
                      }
                    }
                    else//object but can be hashtable, list etc
                    {
                      hashCode = 31 * hashCode + deepArrayHashCode(object);
                    }
                  }
                  else
                  {
                    //hashCode = 31 * hashCode; // this may be issue 
                  }
                  
                  break;
                }
              case PdxTypes::OBJECT_ARRAY:
                {
                  setOffsetForObject(dataInput, pt, pField->SequenceId);
                  Object^ objectArray = dataInput->ReadObjectArray();
                  hashCode = 31 * hashCode + (objectArray!= nullptr) ? deepArrayHashCode(objectArray) : 0 ;
                  break;              
                }
              default:
                {
                  throw gcnew IllegalStateException("PdxInstance not found typeid " + pField->TypeId);
                }
            }

          }
          return hashCode;
        }


        int PdxInstanceImpl::deepArrayHashCode(Object^ obj)
        {
          if(obj == nullptr)
            return 0;

          Type^ objT = obj->GetType();

          /*for each(Type^ tmp in objT->GetInterfaces())
            //Log::Debug("interfaces " + tmp);*/

          if(objT->IsArray)
          {//array
            //if(objT->GetElementType()->IsPrimitive)
            //{//primitive array
            //  return primitiveArrayHashCode((array<int>^)obj);
            //}
            //else
            {//object array
              return enumerableHashCode((System::Collections::IEnumerable^)obj);
            }
          }
          else if(objT->GetInterface("System.Collections.IDictionary"))
          {//map
           // Log::Debug(" in map");
            return enumerateDictionary((System::Collections::IDictionary^)obj);
          }
          else if(objT->GetInterface("System.Collections.IList"))
          {//list
           // Log::Debug(" in list");
            return enumerableHashCode((System::Collections::IEnumerable^)obj);
          }
          else
          {
           
            //  Log::Debug("final object hashcode " + obj->GetHashCode());

            if(obj->GetType()->Equals(DotNetTypes::BooleanType))
            {
             if((bool)obj)
                return 1231;
              else
                return 1237;
            }
            else if(obj->GetType()->Equals(DotNetTypes::StringType))
            {
              String^ str = (String^)obj;
              int prime = 31;
              int h = 0;
              for (int i = 0; i < str->Length; i++) 
                h = prime*h +  str[i];
              return h;
            }

            return obj->GetHashCode();
          }
        }

        int PdxInstanceImpl::enumerableHashCode(System::Collections::IEnumerable^ enumObj)
        {
          int h = 1;
          for each(Object^ o in enumObj)
          {
            h = h*31 + deepArrayHashCode(o);
          //  Log::Debug(" in enumerableHashCode hc " + h);
          }
         // Log::Debug(" in enumerableHashCode FINAL hc " + h);
          return h;
        }

        int PdxInstanceImpl::enumerateDictionary(System::Collections::IDictionary^ iDict)
        {
          int h = 0;
          System::Collections::IDictionaryEnumerator^ dEnum = iDict->GetEnumerator();
          for each(System::Collections::DictionaryEntry^ de in iDict)
          {
            //System::Collections::DictionaryEntry^ de = (System::Collections::DictionaryEntry^)o;
            h = h + ( (deepArrayHashCode(de->Key)) ^ ((de->Value != nullptr )?deepArrayHashCode(de->Value):0 ));
          }
         // Log::Debug(" in enumerateDictionary FINAL hc " + h);
          return h;
        }

        generic <class T>
        int PdxInstanceImpl::primitiveArrayHashCode(T objArray)
        {
          if(objArray == nullptr)
            return 0;

          bool isBooleanType = false;
          if(objArray->Count > 0 && objArray->GetType()->GetElementType()->Equals(DotNetTypes::BooleanType))
            isBooleanType = true;

          //Log::Debug("primitiveArrayHashCode isbool " + isBooleanType);
          int h = 1;
          for each(Object^ o in objArray)
          {
            if(isBooleanType)
            {           
              if((bool)o)
                h = h*31 + 1231;
              else
                h = h*31 + 1237;
            }
            else
              h = h*31 + o->GetHashCode();
          }

            // Log::Debug(" primitiveArrayHashCode final hc " + h);
          
          return h;
        }

        int PdxInstanceImpl::getRawHashCode(PdxType^ pt, PdxFieldType^ pField, DataInput^ dataInput)
        {
          int pos = getOffset(dataInput, pt, pField->SequenceId) ;
          int nextpos = getNextFieldPosition(dataInput, pField->SequenceId+1, pt) ;

          if(hasDefaultBytes(pField, dataInput, pos, nextpos))
            return 0;//matched default bytes

          dataInput->ResetAndAdvanceCursorPdx(nextpos - 1);          

          int h = 1; 
          for(int i = nextpos - 1; i >= pos; i--)
          {
            h = 31 * h + (int)dataInput->ReadSByte();
            dataInput->ResetAndAdvanceCursorPdx(i-1);
          }
          //Log::Debug("getRawHashCode nbytes " + (nextpos - pos) + " final hashcode" + h);
          return h;
        }

        bool PdxInstanceImpl::compareDefaulBytes(DataInput^ dataInput, int start, int end, array<SByte>^ defaultBytes)
        {
          if((end -start) != defaultBytes->Length)
            return false;
          
          dataInput->ResetAndAdvanceCursorPdx(start);
          int j = 0;
          for(int i = start; i< end; i++)
          {
            if(defaultBytes[j++] != dataInput->ReadSByte())
            {              
              return false;
            }
          }
          return true;
        }

        bool PdxInstanceImpl::hasDefaultBytes(PdxFieldType^ pField, DataInput^ dataInput, int start, int end)
        {
          switch(pField->TypeId)
          {
          case PdxTypes::INT:
            {
              return compareDefaulBytes(dataInput, start, end, Int_DefaultBytes);
            }
          case PdxTypes::STRING:
            {
              return compareDefaulBytes(dataInput, start, end, String_DefaultBytes);
            }
          case PdxTypes::BOOLEAN:
            {
              return compareDefaulBytes(dataInput, start, end, Boolean_DefaultBytes);
            }
          case PdxTypes::FLOAT:
            {
              return compareDefaulBytes(dataInput, start, end, Float_DefaultBytes);
            }
          case PdxTypes::DOUBLE:
            {
              return compareDefaulBytes(dataInput, start, end, Double_DefaultBytes);
            }
          case PdxTypes::CHAR:
            {
              return compareDefaulBytes(dataInput, start, end, Char_DefaultBytes);
            }
          case PdxTypes::BYTE:
            {
              return compareDefaulBytes(dataInput, start, end, Byte_DefaultBytes);
            }
          case PdxTypes::SHORT:
            {
              return compareDefaulBytes(dataInput, start, end, Short_DefaultBytes);
            }
          case PdxTypes::LONG:
            {
              return compareDefaulBytes(dataInput, start, end, Long_DefaultBytes);
            }
          case PdxTypes::BYTE_ARRAY:
          case PdxTypes::DOUBLE_ARRAY:
          case PdxTypes::FLOAT_ARRAY:
          case PdxTypes::SHORT_ARRAY:
          case PdxTypes::INT_ARRAY:
          case PdxTypes::LONG_ARRAY:
          case PdxTypes::BOOLEAN_ARRAY:
          case PdxTypes::CHAR_ARRAY:
          case PdxTypes::STRING_ARRAY:
          case PdxTypes::ARRAY_OF_BYTE_ARRAYS:
          case PdxTypes::OBJECT_ARRAY:
            {
              return compareDefaulBytes(dataInput, start, end, NULL_ARRAY_DefaultBytes);
            }
          case PdxTypes::DATE:
           {
              return compareDefaulBytes(dataInput, start, end, Date_DefaultBytes);
           }
          case PdxTypes::OBJECT:
           {
             return compareDefaulBytes(dataInput, start, end, Object_DefaultBytes);
           }            
          default://object
            {
               throw gcnew IllegalStateException("hasDefaultBytes unable to find typeID  " + pField->TypeId); 
            }
          } 
        }

        bool PdxInstanceImpl::isPrimitiveArray(Object^ object)
        {
          Type^ type = object->GetType();

          if(type->IsArray)
          {
            return type->GetElementType()->IsPrimitive;
          }
          return false;
        }

        IList<PdxFieldType^>^ PdxInstanceImpl::getIdentityPdxFields(PdxType^ pt)
        {
          System::Comparison<PdxFieldType^>^ cd = gcnew System::Comparison<PdxFieldType^>( PdxInstanceImpl::comparePdxField);
          IList<PdxFieldType^>^ pdxFieldList = pt->PdxFieldList;
          List<PdxFieldType^>^ retList = gcnew List<PdxFieldType^>();

          for(int i =0; i < pdxFieldList->Count; i++)
          {
            PdxFieldType^ pft = pdxFieldList[i];
            if(pft->IdentityField)
              retList->Add(pft);
          }

          if(retList->Count > 0)
          {
            retList->Sort(cd);
            return retList;
          }

          for(int i =0; i < pdxFieldList->Count; i++)
          {
            PdxFieldType^ pft = pdxFieldList[i];
            retList->Add(pft);
          }

          retList->Sort(cd);
          return retList;
        }
        
        int PdxInstanceImpl::comparePdxField(PdxFieldType^ a, PdxFieldType^ b)
        {
          return a->FieldName->CompareTo(b->FieldName);
        }

        String^ PdxInstanceImpl::ToString()
        {
          PdxType^ pt = getPdxType();

          StringBuilder^ result = gcnew StringBuilder();
          result->Append("PDX[")->Append(pt->TypeId)->Append(",")->Append(pt->PdxClassName)
          ->Append("]{");
          bool firstElement = true;
          for each(PdxFieldType^ fieldType in getIdentityPdxFields(pt))
          {
            if(firstElement) 
            {
              firstElement= false;
            } 
            else 
            {
              result->Append(", ");
            }
            result->Append(fieldType->FieldName);
            result->Append("=");
            try 
            {
              // TODO check to see if getField returned an array and if it did use Arrays.deepToString
              result->Append(GetField(fieldType->FieldName));
            } catch (System::Exception^ e) 
            {
              result->Append(e->Message);
            }
          }
          result->Append("}");
          return result->ToString();
        }
          
        IWritablePdxInstance^ PdxInstanceImpl::CreateWriter()
        {
          //dataInput->ResetPdx(0);
          return gcnew PdxInstanceImpl(m_buffer, m_bufferLength, m_typeId, false);//need to create duplicate byte stream
        }

        void PdxInstanceImpl::SetField(String^ fieldName, Object^ value)
        {
          PdxType^ pt = getPdxType();
          PdxFieldType^ pft = pt->GetPdxField(fieldName);

          if(pft != nullptr && checkType(value->GetType(), pft->TypeId))//TODO::need to check typeas well
          {
            if(m_updatedFields == nullptr)
            {
              m_updatedFields = gcnew Dictionary<String^, Object^>();
            }
            m_updatedFields[fieldName] = value;    
            return;
          }

          throw gcnew IllegalStateException("PdxInstance doesn't has field " + fieldName + " or type of field not matched " + (pft!= nullptr? pft->ToString(): ""));    
        }

        void PdxInstanceImpl::ToData( IPdxWriter^ writer )
        {
          PdxType^ pt = getPdxType();

          IList<PdxFieldType^>^ pdxFieldList = pt->PdxFieldList;

          int position = 0;//ignore typeid and length
          int nextFieldPosition;

          if(m_buffer != NULL)
          {
            uint8_t* copy = m_buffer; 
            
            if(!m_own)
              copy = gemfire::DataInput::getBufferCopy(m_buffer, m_bufferLength);

            DataInput^ dataInput = gcnew DataInput(copy, m_bufferLength);//this will delete buffer
            dataInput->setPdxdeserialization(true);
            //but new stream is set for this from pdxHelper::serialize function

            for(int i =0; i < pdxFieldList->Count; i++)
            {
              PdxFieldType^ currPf = pdxFieldList[i];                        
              
              Object^ value = nullptr;
              m_updatedFields->TryGetValue(currPf->FieldName, value);
              //Log::Debug("field name " + currPf->FieldName);
              if(value != nullptr)
              {//
                //Log::Debug("field updating " + value);
                writeField(writer, currPf->FieldName, currPf->TypeId, value);
                position = getNextFieldPosition(dataInput, i+1, pt);
              }
              else
              {
                if(currPf->IsVariableLengthType)
                {//need to add offset
                  (static_cast<PdxLocalWriter^>(writer))->AddOffset();
                }

                //write raw byte array...
                nextFieldPosition = getNextFieldPosition(dataInput, i+1, pt);
                
                writeUnmodifieldField(dataInput, position, nextFieldPosition, static_cast<PdxLocalWriter^>(writer));

                position = nextFieldPosition;//mark next field;
              }
            }
          }
          else
          {
            for(int i =0; i < pdxFieldList->Count; i++)
            {
              PdxFieldType^ currPf = pdxFieldList[i];                        
              
              Object^ value = m_updatedFields[currPf->FieldName];
              
                //Log::Debug("field updating " + value);
              writeField(writer, currPf->FieldName, currPf->TypeId, value);
            }
          }

          m_updatedFields->Clear();

          //now update the raw data...which will happen in PdxHelper
        }

        void PdxInstanceImpl::cleanup()
        {
          if(m_own)
          {
            m_own = false;
            gemfire::DataOutput::safeDelete(m_buffer);
          }
        }

        void PdxInstanceImpl::updatePdxStream(uint8_t* newPdxStream, int len)
        {
          m_buffer = newPdxStream;
          m_own = true;
          m_bufferLength = len;
        }

        void PdxInstanceImpl::writeUnmodifieldField(DataInput^ dataInput, int startPos, int endPos, PdxLocalWriter^ localWriter)
        {
          //Log::Debug("writeUnmodifieldField startpos " + startPos + " endpos " + endPos);
          dataInput->ResetPdx(startPos);
          for(; startPos < endPos; startPos++)
          {
            localWriter->WriteByte(dataInput->ReadByte());
          }
        }

        int PdxInstanceImpl::getNextFieldPosition(DataInput^ dataInput, int fieldId, PdxType^ pt)
        {
          if(fieldId == pt->Totalfields)
          {//return serialized length
            return getSerializedLength(dataInput, pt);
          }
          else
          {
            return getOffset(dataInput, pt, fieldId);
          }
        }
          
        void PdxInstanceImpl::FromData( IPdxReader^ reader )
        {
          throw gcnew IllegalStateException("PdxInstance::FromData( .. ) shouldn't have called");
        }

        PdxType^ PdxInstanceImpl::getPdxType()
        {
          if(m_typeId == 0)
          {
            if(m_pdxType == nullptr)
            {
              throw gcnew IllegalStateException("PdxType should not be null..");
            }
            return m_pdxType;
          }
          /*m_dataInput->ResetAndAdvanceCursorPdx(0);
          int typeId= Internal::PdxHelper::ReadInt32(m_dataInput->GetCursor() + 4);

          PdxType^ pType = Internal::PdxTypeRegistry::GetPdxType(typeId);*/
          PdxType^ pType = Internal::PdxTypeRegistry::GetPdxType(m_typeId);

          return pType;
        }

        void PdxInstanceImpl::setPdxId(Int32 typeId)
        {
          if(m_typeId == 0)
          {
            m_typeId = typeId;
            m_pdxType = nullptr;
          }
          else
          {
            throw gcnew IllegalStateException("PdxInstance's typeId is already set.");
          }
        }

        Object^ PdxInstanceImpl::readField(DataInput^ dataInput, String^ fieldName, int typeId)
        {
          switch(typeId)
          {
          case PdxTypes::INT:
            {
              return dataInput->ReadInt32();
            }
          case PdxTypes::STRING:
            {
              return dataInput->ReadString();
            }
          case PdxTypes::BOOLEAN:
            {
              return dataInput->ReadBoolean();
            }
          case PdxTypes::FLOAT:
            {
              return dataInput->ReadFloat();
            }
          case PdxTypes::DOUBLE:
            {
              return dataInput->ReadDouble();
            }
          case PdxTypes::CHAR:
            {
              return dataInput->ReadChar();
            }
          case PdxTypes::BYTE:
            {
              return dataInput->ReadSByte();
            }
          case PdxTypes::SHORT:
            {
              return dataInput->ReadInt16();
            }
          case PdxTypes::LONG:
            {
              return dataInput->ReadInt64();
            }
          case PdxTypes::BYTE_ARRAY:
            {
              return dataInput->ReadBytes();
            }
          case PdxTypes::DOUBLE_ARRAY:
            {
              return dataInput->ReadDoubleArray();
            }
          case PdxTypes::FLOAT_ARRAY:
            {
              return dataInput->ReadFloatArray();
            }
          case PdxTypes::SHORT_ARRAY:
            {
              return dataInput->ReadShortArray();
            }
          case PdxTypes::INT_ARRAY:
            {
              return dataInput->ReadIntArray();
            }
          case PdxTypes::LONG_ARRAY:
            {
              return dataInput->ReadLongArray();
            }
          case PdxTypes::BOOLEAN_ARRAY:
            {
              return dataInput->ReadBooleanArray();
            }
          case PdxTypes::CHAR_ARRAY:
            {
              return dataInput->ReadCharArray();
            }
          case PdxTypes::STRING_ARRAY:
            {
              return dataInput->ReadStringArray();
            }
          case PdxTypes::DATE:
            {
              return dataInput->ReadDate();
            }
          case PdxTypes::ARRAY_OF_BYTE_ARRAYS:
            {
              return dataInput->ReadArrayOfByteArrays();
            }
          case PdxTypes::OBJECT_ARRAY:
            {
              return dataInput->ReadObjectArray();
            }            
          default://object
            {
              return dataInput->ReadObject();
               //throw gcnew IllegalStateException("ReadField unable to de-serialize  " 
								//																	+ fieldName + " of " + type); 
            }
          }
          }

        bool PdxInstanceImpl::checkType(Type^ type, int typeId)
        {
         // Log::Fine("PdxInstanceImpl::checkType1 " + type->ToString() + "  " + typeId); 
          switch(typeId)
          {
          case PdxTypes::INT:
            {
             // Log::Fine("PdxInstanceImpl::checkType " + type->ToString() + " : " +DotNetTypes::IntType->ToString());
              return type->Equals(DotNetTypes::IntType);
            }
          case PdxTypes::STRING:
            {
              return type->Equals(DotNetTypes::StringType);
            }
          case PdxTypes::BOOLEAN:
            {
              return type->Equals(DotNetTypes::BooleanType);
            }
          case PdxTypes::FLOAT:
            {
              return type->Equals(DotNetTypes::FloatType);
            }
          case PdxTypes::DOUBLE:
            {
              return type->Equals(DotNetTypes::DoubleType);
            }
          case PdxTypes::CHAR:
            {
              return type->Equals(DotNetTypes::CharType);
            }
          case PdxTypes::BYTE:
            {
              return type->Equals(DotNetTypes::SByteType);
            }
          case PdxTypes::SHORT:
            {
              return type->Equals(DotNetTypes::ShortType);
            }
          case PdxTypes::LONG:
            {
              return type->Equals(DotNetTypes::LongType);
            }
          case PdxTypes::BYTE_ARRAY:
            {
              return type->Equals(DotNetTypes::ByteArrayType);
            }
          case PdxTypes::DOUBLE_ARRAY:
            {
              return type->Equals(DotNetTypes::DoubleArrayType);
            }
          case PdxTypes::FLOAT_ARRAY:
            {
              return type->Equals(DotNetTypes::FloatArrayType);
            }
          case PdxTypes::SHORT_ARRAY:
            {
              return type->Equals(DotNetTypes::ShortArrayType);
            }
          case PdxTypes::INT_ARRAY:
            {
              return type->Equals(DotNetTypes::IntArrayType);
            }
          case PdxTypes::LONG_ARRAY:
            {
              return type->Equals(DotNetTypes::LongArrayType);
            }
          case PdxTypes::BOOLEAN_ARRAY:
            {
              return type->Equals(DotNetTypes::BoolArrayType);
            }
          case PdxTypes::CHAR_ARRAY:
            {
              return type->Equals(DotNetTypes::CharArrayType);
            }
          case PdxTypes::STRING_ARRAY:
            {
              return type->Equals(DotNetTypes::StringArrayType);
            }
          case PdxTypes::DATE:
            {
              return type->Equals(DotNetTypes::DateType);
            }
          case PdxTypes::ARRAY_OF_BYTE_ARRAYS:
            {
              return type->Equals(DotNetTypes::ByteArrayOfArrayType);
            }
          case PdxTypes::OBJECT_ARRAY:
            {
              return type->Equals(DotNetTypes::ObjectArrayType);
            }            
          default://object
            {
              return true;
               //throw gcnew IllegalStateException("ReadField unable to de-serialize  " 
								//																	+ fieldName + " of " + type); 
            }
          }
          }

        void PdxInstanceImpl::writeField(IPdxWriter^ writer, String^ fieldName, int typeId, Object^ value)
        {
          switch(typeId)
          {
          case PdxTypes::INT:
            {
              writer->WriteInt(fieldName, (int)value);
              break;
            }
          case PdxTypes::STRING:
            {
              writer->WriteString(fieldName, (String^)value);
              break;
            }
          case PdxTypes::BOOLEAN:
            {
              writer->WriteBoolean(fieldName, (bool)value);
              break;
            }
          case PdxTypes::FLOAT:
            {
              writer->WriteFloat(fieldName, (float)value);
              break;
            }
          case PdxTypes::DOUBLE:
            {
              writer->WriteDouble(fieldName, (double)value);
              break;
            }
          case PdxTypes::CHAR:
            {
              writer->WriteChar(fieldName, (Char)value);
              break;
            }
          case PdxTypes::BYTE:
            {
              writer->WriteByte(fieldName, (SByte)value);
              break;
            }
          case PdxTypes::SHORT:
            {
              writer->WriteShort(fieldName, (short)value);
              break;
            }
          case PdxTypes::LONG:
            {
              writer->WriteLong(fieldName, (Int64)value);
              break;
            }
          case PdxTypes::BYTE_ARRAY:
            {
              writer->WriteByteArray(fieldName, (array<Byte>^)value);
              break;
            }
          case PdxTypes::DOUBLE_ARRAY:
            {
              writer->WriteDoubleArray(fieldName, (array<double>^)value);
              break;
            }
          case PdxTypes::FLOAT_ARRAY:
            {
              writer->WriteFloatArray(fieldName, (array<float>^)value);
              break;
            }
          case PdxTypes::SHORT_ARRAY:
            {
              writer->WriteShortArray(fieldName, (array<short>^)value);
              break;
            }
          case PdxTypes::INT_ARRAY:
            {
              writer->WriteIntArray(fieldName, (array<int>^)value);
              break;
            }
          case PdxTypes::LONG_ARRAY:
            {
              writer->WriteLongArray(fieldName, (array<Int64>^)value);
              break;
            }
          case PdxTypes::BOOLEAN_ARRAY:
            {
              writer->WriteBooleanArray(fieldName, (array<bool>^)value);
              break;
            }
          case PdxTypes::CHAR_ARRAY:
            {
              writer->WriteCharArray(fieldName, (array<Char>^)value);
              break;
            }
          case PdxTypes::STRING_ARRAY:
            {
              writer->WriteStringArray(fieldName, (array<String^>^)value);
              break;
            }
          case PdxTypes::DATE:
            {
              writer->WriteDate(fieldName, (DateTime)value);
              break;
            }
          case PdxTypes::ARRAY_OF_BYTE_ARRAYS:
            {
              writer->WriteArrayOfByteArrays(fieldName, (array<array<Byte>^>^)value);
              break;
            }
          case PdxTypes::OBJECT_ARRAY:
            {
              writer->WriteObjectArray(fieldName, (List<Object^>^)value);
              break;
            }            
          default:
            {
              writer->WriteObject(fieldName, value);
               //throw gcnew IllegalStateException("ReadField unable to de-serialize  " 
								//																	+ fieldName + " of " + type); 
            }
          }
          }

        }
			}
    }
  }
}
