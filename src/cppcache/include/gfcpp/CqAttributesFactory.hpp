#ifndef __GEMFIRE_CQ_ATTRIBUTES_FACTORY_H__
#define __GEMFIRE_CQ_ATTRIBUTES_FACTORY_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
#include "CqAttributes.hpp"
#include "CqListener.hpp"

/**
 * @file
 */

namespace gemfire {

/**
 * @class CqAttributesFactory CqAttributesFactory.hpp
 *
 * The factory class for the CqAttributes instance. This provides the CqListener
 * setter methods. This class maintains state for and creates new instances of
 * the
 * CqAttributes interface for new CqQuery instances.
 * If you create a factory with the default constructor, then the factory is set
 * up to create attributes with all default settings. You can also create a
 * factory
 * by providing a <code>CqAttributes</code>, which will set up the new factory
 * with
 * the settings provided in that attributes instance.
 *
 * <p>Once a <code>CqAttributes</code> is created, it can only be modified
 * after it has been used to create a <code>CqQuery</code>.
 *
 * @see CqAttributes
 *
 */
class CPPCACHE_EXPORT CqAttributesFactory : public SharedBase {
 public:
  /**
   * Creates a new instance of AttributesFactory ready to create a
   * <code>CqAttributes</code> with default settings.
   */
  CqAttributesFactory();

  /**
   * Creates a new instance of CqAttributesFactory ready to create a
   * <code>CqAttributes</code> with the same settings as those in the
   * specified <code>CqAttributes</code>.
   *
   * @param cqAttributes
   *          the <code>CqAttributes</code> used to initialize this
   *          AttributesFactory
   */
  CqAttributesFactory(CqAttributesPtr& cqAttributes);

  /**
   * Adds a CQ listener to the end of the list of cq listeners on this factory.
   * @param cqListener the CqListener to add to the factory.
   * @throws IllegalArgumentException if <code>cqListener</code> is NULLPTR
   */
  void addCqListener(const CqListenerPtr& cqListener);

  /**
   * Removes all Cq listeners and then adds each listener in the specified
   * array.
   * @param cqListeners a possibly empty array of listeners to add to this
   * factory.
   * @throws IllegalArgumentException if the <code>cqListeners</code> array has
   * a
   * NULLPTR element
   */
  void initCqListeners(VectorOfCqListener& cqListeners);

  /**
   * Creates a <code>CqAttributes</code> with the current settings.
   * @return the newly created <code>CqAttributes</code>
   */
  CqAttributesPtr create();

 private:
  CqAttributesPtr m_cqAttributes;
};

}  // namespace gemfire

#endif  // ifndef __GEMFIRE_CQ_ATTRIBUTES_FACTORY_H__
