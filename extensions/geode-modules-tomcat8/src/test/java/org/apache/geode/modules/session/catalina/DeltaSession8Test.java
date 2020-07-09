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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;

import javax.servlet.http.HttpSessionAttributeListener;
import javax.servlet.http.HttpSessionBindingEvent;

import org.apache.catalina.Context;
import org.apache.catalina.Manager;
import org.apache.juli.logging.Log;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.internal.util.BlobHelper;

public class DeltaSession8Test extends AbstractDeltaSessionTest<DeltaSession8> {
  final HttpSessionAttributeListener listener = mock(HttpSessionAttributeListener.class);

  @Before
  @Override
  public void setup() {
    super.setup();

    final Context context = mock(Context.class);
    when(manager.getContext()).thenReturn(context);
    when(context.getApplicationEventListeners()).thenReturn(new Object[] {listener});
    when(context.getLogger()).thenReturn(mock(Log.class));
  }

  @Override
  protected DeltaSession8 newDeltaSession(Manager manager) {
    return new DeltaSession8(manager);
  }

  @Test
  public void serializedAttributesNotLeakedInAttributeReplaceEvent() throws IOException {
    final DeltaSession8 session = spy(new DeltaSession8(manager));
    session.setValid(true);
    final String name = "attribute";
    final Object value1 = "value1";
    final byte[] serializedValue1 = BlobHelper.serializeToBlob(value1);
    // simulates initial deserialized state with serialized attribute values.
    session.getAttributes().put(name, serializedValue1);

    final Object value2 = "value2";
    session.setAttribute(name, value2);

    final ArgumentCaptor<HttpSessionBindingEvent> event =
        ArgumentCaptor.forClass(HttpSessionBindingEvent.class);
    verify(listener).attributeReplaced(event.capture());
    verifyNoMoreInteractions(listener);
    assertThat(event.getValue().getValue()).isEqualTo(value1);
  }

  @Test
  public void serializedAttributesNotLeakedInAttributeRemovedEvent() throws IOException {
    final DeltaSession8 session = spy(new DeltaSession8(manager));
    session.setValid(true);
    final String name = "attribute";
    final Object value1 = "value1";
    final byte[] serializedValue1 = BlobHelper.serializeToBlob(value1);
    // simulates initial deserialized state with serialized attribute values.
    session.getAttributes().put(name, serializedValue1);

    session.removeAttribute(name);

    final ArgumentCaptor<HttpSessionBindingEvent> event =
        ArgumentCaptor.forClass(HttpSessionBindingEvent.class);
    verify(listener).attributeRemoved(event.capture());
    verifyNoMoreInteractions(listener);
    assertThat(event.getValue().getValue()).isEqualTo(value1);
  }

  @Test
  public void serializedAttributesLeakedInAttributeReplaceEventWhenPreferDeserializedFormFalse()
      throws IOException {
    setPreferDeserializedFormFalse();

    final DeltaSession8 session = spy(new DeltaSession8(manager));
    session.setValid(true);
    final String name = "attribute";
    final Object value1 = "value1";
    final byte[] serializedValue1 = BlobHelper.serializeToBlob(value1);
    // simulates initial deserialized state with serialized attribute values.
    session.getAttributes().put(name, serializedValue1);

    final Object value2 = "value2";
    session.setAttribute(name, value2);

    final ArgumentCaptor<HttpSessionBindingEvent> event =
        ArgumentCaptor.forClass(HttpSessionBindingEvent.class);
    verify(listener).attributeReplaced(event.capture());
    verifyNoMoreInteractions(listener);
    assertThat(event.getValue().getValue()).isInstanceOf(byte[].class);
  }

  @Test
  public void serializedAttributesLeakedInAttributeRemovedEventWhenPreferDeserializedFormFalse()
      throws IOException {
    setPreferDeserializedFormFalse();

    final DeltaSession8 session = spy(new DeltaSession8(manager));
    session.setValid(true);
    final String name = "attribute";
    final Object value1 = "value1";
    final byte[] serializedValue1 = BlobHelper.serializeToBlob(value1);
    // simulates initial deserialized state with serialized attribute values.
    session.getAttributes().put(name, serializedValue1);

    session.removeAttribute(name);

    final ArgumentCaptor<HttpSessionBindingEvent> event =
        ArgumentCaptor.forClass(HttpSessionBindingEvent.class);
    verify(listener).attributeRemoved(event.capture());
    verifyNoMoreInteractions(listener);
    assertThat(event.getValue().getValue()).isInstanceOf(byte[].class);
  }

  @SuppressWarnings("deprecation")
  protected void setPreferDeserializedFormFalse() {
    when(manager.getPreferDeserializedForm()).thenReturn(false);
  }

}
