/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.modules.session.catalina;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.http.HttpSessionAttributeListener;
import javax.servlet.http.HttpSessionBindingEvent;

import org.apache.catalina.Context;
import org.apache.juli.logging.Log;
import org.junit.Test;

import org.apache.geode.internal.util.BlobHelper;

public class DeltaSessionTest extends AbstractDeltaSessionTest {

  @Test
  public void serializedAttributesNotLeakedInAttributeReplaceEvent() throws IOException {

    final Object value1 = "value1";
    final byte[] serializedValue1 = BlobHelper.serializeToBlob(value1);
    AtomicReference<Object> eventValue = new AtomicReference<>();

    HttpSessionAttributeListener listener = new HttpSessionAttributeListener() {
      @Override
      public void attributeAdded(HttpSessionBindingEvent event) {}

      @Override
      public void attributeRemoved(HttpSessionBindingEvent event) {}

      @Override
      public void attributeReplaced(HttpSessionBindingEvent event) {
        eventValue.set(event.getValue());
      }
    };

    final Context context = mock(Context.class);
    when(manager.getContext()).thenReturn(context);
    when(context.getApplicationEventListeners()).thenReturn(new Object[] {listener});
    when(context.getLogger()).thenReturn(mock(Log.class));

    DeltaSession session = spy(new DeltaSession(manager));
    session.setValid(true);
    // simulates initial deserialized state with serialized attribute values.
    final String attributeName = "attribute";
    session.getAttributes().put(attributeName, serializedValue1);

    final Object value2 = "value2";
    session.setAttribute(attributeName, value2);
    assertThat(eventValue.get()).isEqualTo(value1);
  }

  @Test
  public void serializedAttributesNotLeakedInAttributeRemovedEvent() throws IOException {

    final Object value1 = "value1";
    final byte[] serializedValue1 = BlobHelper.serializeToBlob(value1);
    AtomicReference<Object> eventValue = new AtomicReference<>();

    HttpSessionAttributeListener listener = new HttpSessionAttributeListener() {
      @Override
      public void attributeAdded(HttpSessionBindingEvent event) {}

      @Override
      public void attributeRemoved(HttpSessionBindingEvent event) {
        eventValue.set(event.getValue());
      }

      @Override
      public void attributeReplaced(HttpSessionBindingEvent event) {}
    };

    final Context context = mock(Context.class);
    when(manager.getContext()).thenReturn(context);
    when(context.getApplicationEventListeners()).thenReturn(new Object[] {listener});
    when(context.getLogger()).thenReturn(mock(Log.class));

    DeltaSession session = spy(new DeltaSession(manager));
    session.setValid(true);
    // simulates initial deserialized state with serialized attribute values.
    final String attributeName = "attribute";
    session.getAttributes().put(attributeName, serializedValue1);

    session.removeAttribute(attributeName);
    assertThat(eventValue.get()).isEqualTo(value1);
  }

}
