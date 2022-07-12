/*
 * Copyright 2022 VMware, Inc.
 * https://network.tanzu.vmware.com/legal_documents/vmware_eula
 */
package org.apache.geode.modules.session.internal.filter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import javax.servlet.ServletContext;

import org.junit.Test;

import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.internal.cache.eviction.EvictionCounters;
import org.apache.geode.internal.cache.eviction.HeapLRUController;
import org.apache.geode.modules.session.internal.filter.attributes.DeltaQueuedSessionAttributes;
import org.apache.geode.modules.session.internal.filter.attributes.DeltaSessionAttributes;

public class GemfireHttpSessionTest {

  @Test
  public void getSizeInBytesAccountsForId() {
    GemfireHttpSession session1 = new GemfireHttpSession("id", mock(ServletContext.class));
    GemfireHttpSession session2 =
        new GemfireHttpSession("BIGGGGGGGGGGGGGG id", mock(ServletContext.class));
    int sizeWithSmallId = session1.getSizeInBytes();
    int sizeWithBigId = session2.getSizeInBytes();
    assertThat(sizeWithBigId).isGreaterThan(sizeWithSmallId);
  }

  @Test
  public void getSizeInBytesAccountsForDeltaSessionAttributes() {
    GemfireHttpSession session1 = new GemfireHttpSession("id", mock(ServletContext.class));
    GemfireHttpSession session2 = new GemfireHttpSession("id", mock(ServletContext.class));
    session2.setManager(mock(SessionManager.class));
    DeltaSessionAttributes attributes = new DeltaSessionAttributes();
    attributes.setSession(session2);
    session2.setAttributes(attributes);
    session2.putValue("attributeName", "attributeValue");
    int sizeWithNoAttributes = session1.getSizeInBytes();
    int sizeWithAttributes = session2.getSizeInBytes();
    assertThat(sizeWithAttributes).isGreaterThan(sizeWithNoAttributes);
  }

  @Test
  public void getSizeInBytesAccountsForDeltaQueuedSessionAttributes() {
    GemfireHttpSession session1 = new GemfireHttpSession("id", mock(ServletContext.class));
    GemfireHttpSession session2 = new GemfireHttpSession("id", mock(ServletContext.class));
    session2.setManager(mock(SessionManager.class));
    DeltaQueuedSessionAttributes attributes = new DeltaQueuedSessionAttributes();
    attributes.setSession(session2);
    session2.setAttributes(attributes);
    session2.putValue("attributeName", "attributeValue");
    int sizeWithNoAttributes = session1.getSizeInBytes();
    int sizeWithAttributes = session2.getSizeInBytes();
    assertThat(sizeWithAttributes).isGreaterThan(sizeWithNoAttributes);
  }

  @Test
  public void verifyHeapLRUControllerWillDetectSizeChanges() {
    GemfireHttpSession session = new GemfireHttpSession("id", mock(ServletContext.class));
    session.setManager(mock(SessionManager.class));
    DeltaSessionAttributes attributes = new DeltaSessionAttributes();
    attributes.setSession(session);
    session.setAttributes(attributes);
    HeapLRUController controller = new HeapLRUController(mock(EvictionCounters.class),
        EvictionAction.DEFAULT_EVICTION_ACTION, null,
        EvictionAlgorithm.LRU_HEAP);
    int sizeWithNoAttributes = controller.entrySize("key", session);
    session.putValue("attributeName", "attributeValue");
    int sizeWithAttributes = controller.entrySize("key", session);
    assertThat(sizeWithAttributes).isGreaterThan(sizeWithNoAttributes);
  }
}
