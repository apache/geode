#ifndef _PDXWRAPPER_HPP_
#define _PDXWRAPPER_HPP_

/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*========================================================================
*/

#include "PdxSerializer.hpp"
#include "PdxSerializable.hpp"

namespace gemfire {

class CPPCACHE_EXPORT PdxWrapper : public PdxSerializable {
  /**
   * The PdxWrapper class allows domain classes to be used in Region operations.
   * A user domain object should be wrapped in an instance of a PdxWrapper with
* a
* PdxSerializer registered that can handle the user domain class.
   */

 public:
  /**
   * Constructor which takes the address of the user object to contain for PDX
   * serialization.
   * @param userObject the void pointer to an instance of a user object - NOTE:
   * PdxWrapper takes ownership.
   * @param className the fully qualified class name to map this user object to
   * the Java side.
   */
  PdxWrapper(void* userObject, const char* className);

  /**
   * Returns the pointer to the user object which is deserialized with a
   * PdxSerializer.
   * User code (such as in PdxSerializer) should cast it to a pointer of the
   * known user class.
   * @param detach if set to true will release ownership of the object and
   * future calls to getObject() return NULL.
   */
  void* getObject(bool detach = false);

  /**
   * Get the class name for the user domain object.
   */
  const char* getClassName() const;

  /** return true if this key matches other. */
  bool operator==(const CacheableKey& other) const;

  /** return the hashcode for this key. */
  uint32_t hashcode() const;

  /**
  *@brief serialize this object in gemfire PDX format
  *@param PdxWriter to serialize the PDX object
  **/
  void toData(PdxWriterPtr output);
  /**
  *@brief Deserialize this object
  *@param PdxReader to Deserialize the PDX object
  **/
  void fromData(PdxReaderPtr input);
  /**
  *@brief serialize this object
  **/
  void toData(DataOutput& output) const;
  /**
  *@brief deserialize this object, typical implementation should return
  * the 'this' pointer.
  **/
  Serializable* fromData(DataInput& input);
  /**
  *@brief return the classId of the instance being serialized.
  * This is used by deserialization to determine what instance
  * type to create and derserialize into.
  */
  int32_t classId() const { return 0; }
  /**
  *@brief return the size in bytes of the instance being serialized.
  * This is used to determine whether the cache is using up more
  * physical memory than it has been configured to use. The method can
  * return zero if the user does not require the ability to control
  * cache memory utilization.
  * Note that you must implement this only if you use the HeapLRU feature.
  */
  uint32_t objectSize() const;
  /**
  * Display this object as 'string', which depends on the implementation in
  * the subclasses.
  * The default implementation renders the classname.
  *
  * The return value may be a temporary, so the caller has to ensure that
  * the SharedPtr count does not go down to zero by storing the result
  * in a variable or otherwise.
  */
  CacheableStringPtr toString() const;

  virtual ~PdxWrapper();

 private:
  /** hide default constructor */
  PdxWrapper();
  PdxWrapper(const char* className);

  void* m_userObject;
  PdxSerializerPtr m_serializer;
  UserDeallocator m_deallocator;
  UserObjectSizer m_sizer;
  char* m_className;

  friend class SerializationRegistry;

  PdxWrapper(const PdxWrapper&);

  const PdxWrapper& operator=(const PdxWrapper&);
};

} /* namespace gemfire */
#endif /* _PDXWRAPPER_HPP_ */
