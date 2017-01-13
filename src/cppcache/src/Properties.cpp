/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <gfcpp/Properties.hpp>
#include <gfcpp/GemfireTypeIds.hpp>

#include <string>
#include <ace/Hash_Map_Manager.h>
#include <ace/Recursive_Thread_Mutex.h>
#include <ace/Guard_T.h>
#include "ace/config-lite.h"
#include "ace/Versioned_Namespace.h"
#include <ace/OS_NS_stdio.h>

using namespace gemfire;

ACE_BEGIN_VERSIONED_NAMESPACE_DECL

template <>
class ACE_Hash<gemfire::CacheableKeyPtr> {
 public:
  u_long operator()(const gemfire::CacheableKeyPtr& key) {
    return key->hashcode();
  }
};

template <>
class ACE_Equal_To<gemfire::CacheableKeyPtr> {
 public:
  int operator()(const gemfire::CacheableKeyPtr& lhs,
                 const gemfire::CacheableKeyPtr& rhs) const {
    return (*lhs.ptr() == *rhs.ptr());
  }
};
ACE_END_VERSIONED_NAMESPACE_DECL

namespace gemfire {

typedef ACE_Hash_Map_Manager_Ex<
    CacheableKeyPtr, CacheablePtr, ACE_Hash<CacheableKeyPtr>,
    ACE_Equal_To<CacheableKeyPtr>, ACE_Recursive_Thread_Mutex>
    CacheableKeyCacheableMap;

typedef ACE_Guard<ACE_Recursive_Thread_Mutex> CacheableKeyCacheableMapGuard;

#define MAP ((CacheableKeyCacheableMap*)m_map)

class PropertiesFile {
  Properties& m_props;
  std::string m_fileName;

 public:
  explicit PropertiesFile(Properties& props);

  void parseLine(const std::string& line);
  void readFile(const std::string& fileName);
  char* nextBufferLine(char** cursor, char* bufbegin, size_t totalLen);
};

PropertiesPtr Properties::create() { return PropertiesPtr(new Properties()); }

Properties::Properties() : Serializable() {
  m_map = (void*)new CacheableKeyCacheableMap();
  MAP->open();
}
Properties::~Properties() {
  if (m_map != NULL) {
    delete MAP;
    m_map = NULL;
  }
}

/** this return value must be stored in a CacheableStringPtr. */
CacheableStringPtr Properties::find(const char* key) {
  if (key == NULL) {
    throw NullPointerException("Properties::find: Null key given.");
  }
  CacheableStringPtr keyptr = CacheableString::create(key);
  CacheableKeyCacheableMapGuard guard(MAP->mutex());
  CacheablePtr value;
  int status = MAP->find(keyptr, value);
  if (status != 0) {
    return NULLPTR;
  }
  if (value == NULLPTR) {
    return NULLPTR;
  }
  return value->toString();
}

CacheablePtr Properties::find(const CacheableKeyPtr& key) {
  if (key == NULLPTR) {
    throw NullPointerException("Properties::find: Null key given.");
  }
  CacheableKeyCacheableMapGuard guard(MAP->mutex());
  CacheablePtr value;
  int status = MAP->find(key, value);
  if (status != 0) {
    return NULLPTR;
  }
  return value;
}

void Properties::insert(const char* key, const char* value) {
  if (key == NULL) {
    throw NullPointerException("Properties::insert: Null key given.");
  }
  CacheableStringPtr keyptr = CacheableString::create(key);
  CacheableStringPtr valptr = (value == NULL ? CacheableString::create("")
                                             : CacheableString::create(value));
  MAP->rebind(keyptr, valptr);
}

void Properties::insert(const char* key, const int value) {
  if (key == NULL) {
    throw NullPointerException("Properties::insert: Null key given.");
  }
  char temp[64];
  ACE_OS::snprintf(temp, 64, "%d", value);
  CacheableStringPtr keyptr = CacheableString::create(key);
  CacheableStringPtr valptr = CacheableString::create(temp);
  MAP->rebind(keyptr, valptr);
}

void Properties::insert(const CacheableKeyPtr& key, const CacheablePtr& value) {
  if (key == NULLPTR) {
    throw NullPointerException("Properties::insert: Null key given.");
  }
  MAP->rebind(key, value);
}

void Properties::remove(const char* key) {
  if (key == NULL) {
    throw NullPointerException("Properties::remove: Null key given.");
  }
  CacheableStringPtr keyptr = CacheableString::create(key);
  MAP->unbind(keyptr);
}

void Properties::remove(const CacheableKeyPtr& key) {
  if (key == NULLPTR) {
    throw NullPointerException("Properties::remove: Null key given.");
  }
  MAP->unbind(key);
}

uint32_t Properties::getSize() const {
  return static_cast<uint32_t>(MAP->current_size());
}

void Properties::foreach (Visitor& visitor) const {
  CacheableKeyCacheableMapGuard guard(MAP->mutex());
  CacheableKeyCacheableMap::iterator iter = MAP->begin();
  while (iter != MAP->end()) {
    CacheableKeyPtr key = (*iter).ext_id_;
    CacheablePtr val = (*iter).int_id_;
    visitor.visit(key, val);
    ++iter;
  }
}

void Properties::addAll(const PropertiesPtr& other) {
  if (other == NULLPTR) return;

  class Copier : public Visitor {
    Properties& m_lhs;

   public:
    explicit Copier(Properties& lhs) : m_lhs(lhs) {}
    void visit(CacheableKeyPtr& key, CacheablePtr& value) {
      m_lhs.insert(key, value);
    }
  } aCopier(*this);

  other->foreach (aCopier);
}

void Properties::load(const char* fileName) {
  GF_R_ASSERT(fileName != NULL);
  PropertiesFile pf(*this);
  pf.readFile(fileName);
}

PropertiesFile::PropertiesFile(Properties& props) : m_props(props) {}

void PropertiesFile::readFile(const std::string& fileName) {
  char buf[8192];
  FILE* fp = fopen(fileName.c_str(), "r");
  if (fp != NULL) {
    size_t len = 0;
    while ((len = fread(buf, 1, 8192, fp)) != 0) {
      /* adongre
       * CID 28894: String not null terminated (STRING_NULL)
       * Function "fread" does not terminate string "*buf".
       */
      buf[len] = '\0';
      char* tmp = buf;
      char* line = 0;
      while ((line = nextBufferLine(&tmp, buf, len)) != NULL) {
        parseLine(line);
      }
    }
    fclose(fp);
  }
}

char* PropertiesFile::nextBufferLine(char** cursor, char* bufbegin,
                                     size_t totalLen) {
  if (static_cast<size_t>((*cursor) - bufbegin) >= totalLen) {
    return 0;
  }

  char* lineBegin = *cursor;
  while (((**cursor) != 10) && ((**cursor) != 13) &&
         (static_cast<size_t>((*cursor) - bufbegin) < totalLen)) {
    (*cursor)++;
  }
  **cursor = '\0';
  (*cursor)++;  // move to begin of next line.
  return lineBegin;
}

void PropertiesFile::parseLine(const std::string& line) {
  if (line.c_str()[0] == '#') {
    return;
  }
  if (line.c_str()[0] == '/') {
    return;
  }

  char key[1024];
  char value[1024];
  int res = sscanf(line.c_str(), "%[^ \t=]%*[ \t=]%[^\n\r]", key, value);
  if (res != 2) {
    // bad parse...
    return;
  }
  // clean up value from trailing whitespace.
  size_t len = strlen(value);
  size_t end = len - 1;
  while (end > 0) {
    if (value[end] != ' ') {
      break;
    }
    value[end] = '\0';
    end--;
  }
  // clean up escape '\' sequences.
  // size_t idx = 0;
  // size_t widx = 0;
  // len = strlen( value );
  // while( idx < (len + 1 /* copy the Null terminator also */ ) ) {
  //  if ( value[idx] == '\\' ) {
  //    idx++;
  //  }
  //  value[widx] = value[idx];
  //  widx++;
  //  idx++;
  //}

  m_props.insert(key, value);
}

Serializable* Properties::createDeserializable() { return new Properties(); }

int32_t Properties::classId() const { return 0; }

int8_t Properties::typeId() const { return GemfireTypeIds::Properties; }

void Properties::toData(DataOutput& output) const {
  CacheableKeyCacheableMapGuard guard(MAP->mutex());
  int32_t mapSize = getSize();
  output.writeArrayLen(mapSize);
  CacheableKeyCacheableMap::iterator iter = MAP->begin();
  while (iter != MAP->end()) {
    // changed
    CacheableString* csPtr =
        dynamic_cast<CacheableString*>(((*iter).ext_id_).ptr());
    if (csPtr == NULL) {
      output.writeObject((*iter).ext_id_);  // changed
    } else {
      output.writeNativeString(csPtr->asChar());
    }

    csPtr = dynamic_cast<CacheableString*>(((*iter).int_id_).ptr());
    if (csPtr == NULL) {
      output.writeObject((*iter).int_id_);  // changed
    } else {
      output.writeNativeString(csPtr->asChar());
    }
    ++iter;
  }
}

Serializable* Properties::fromData(DataInput& input) {
  int32_t mapSize = 0;
  input.readArrayLen(&mapSize);
  for (int i = 0; i < mapSize; i++) {
    CacheableStringPtr key;
    CacheableStringPtr val;
    // TODO: need to look just look typeid if string then convert otherwise
    // continue with readobject

    if (!input.readNativeString(key)) {
      CacheableKeyPtr keyPtr;
      CacheablePtr valPtr;
      input.readObject(keyPtr, true);
      input.readObject(valPtr);
      MAP->rebind(keyPtr, valPtr);
    } else {
      if (!input.readNativeString(val)) {
        CacheablePtr valPtr;
        input.readObject(valPtr);
        MAP->rebind(key, valPtr);
      } else {
        MAP->rebind(key, val);
      }
    }
  }
  return this;
}
}  // namespace gemfire
