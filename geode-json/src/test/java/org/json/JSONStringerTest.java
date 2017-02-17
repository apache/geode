/*
 * Copyright (C) 2010 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.json;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * This black box test was written without inspecting the non-free org.json sourcecode.
 */
public class JSONStringerTest {

    @Test
    public void testJSONFunctionHackTest() {
        JSONStringer stringer = new JSONStringer();
        stringer.object();
        stringer.key("key");
        stringer.value(new JSONFunctionTestObject("window.test('foo' + \"bar\")"));
        stringer.endObject();
        assertEquals("{\"key\":window.test('foo' + \"bar\")}", stringer.toString());
    }

    @Test
    public void testEmptyStringer() {
        // why isn't this the empty string?
        assertNull(new JSONStringer().toString());
    }

    @Test
    public void testValueJSONNull() throws JSONException {
        JSONStringer stringer = new JSONStringer();
        stringer.array();
        stringer.value(JSONObject.NULL);
        stringer.endArray();
        assertEquals("[null]", stringer.toString());
    }

    @Test
    public void testEmptyObject() throws JSONException {
        JSONStringer stringer = new JSONStringer();
        stringer.object();
        stringer.endObject();
        assertEquals("{}", stringer.toString());
    }

    @Test
    public void testEmptyArray() throws JSONException {
        JSONStringer stringer = new JSONStringer();
        stringer.array();
        stringer.endArray();
        assertEquals("[]", stringer.toString());
    }

    @Test
    public void testArray() throws JSONException {
        JSONStringer stringer = new JSONStringer();
        stringer.array();
        stringer.value(false);
        stringer.value(5.0);
        stringer.value(5L);
        stringer.value("five");
        stringer.value(null);
        stringer.endArray();
        assertEquals("[false,5,5,\"five\",null]", stringer.toString());
    }

    @Test
    public void testValueObjectMethods() throws JSONException {
        JSONStringer stringer = new JSONStringer();
        stringer.array();
        stringer.value(Boolean.FALSE);
        stringer.value(Double.valueOf(5.0));
        stringer.value(Long.valueOf(5L));
        stringer.endArray();
        assertEquals("[false,5,5]", stringer.toString());
    }

    @Test
    public void testKeyValue() throws JSONException {
        JSONStringer stringer = new JSONStringer();
        stringer.object();
        stringer.key("a").value(false);
        stringer.key("b").value(5.0);
        stringer.key("c").value(5L);
        stringer.key("d").value("five");
        stringer.key("e").value(null);
        stringer.endObject();
        assertEquals("{\"a\":false," +
                "\"b\":5," +
                "\"c\":5," +
                "\"d\":\"five\"," +
                "\"e\":null}", stringer.toString());
    }

    /**
     * Test what happens when extreme values are emitted. Such values are likely
     * to be rounded during parsing.
     */
    @Test
    public void testNumericRepresentations() throws JSONException {
        JSONStringer stringer = new JSONStringer();
        stringer.array();
        stringer.value(Long.MAX_VALUE);
        stringer.value(Double.MIN_VALUE);
        stringer.endArray();
        assertEquals("[9223372036854775807,4.9E-324]", stringer.toString());
    }

    @Test
    public void testWeirdNumbers() throws JSONException {
        try {
            new JSONStringer().array().value(Double.NaN);
            fail();
        } catch (JSONException ignored) {
        }
        try {
            new JSONStringer().array().value(Double.NEGATIVE_INFINITY);
            fail();
        } catch (JSONException ignored) {
        }
        try {
            new JSONStringer().array().value(Double.POSITIVE_INFINITY);
            fail();
        } catch (JSONException ignored) {
        }

        JSONStringer stringer = new JSONStringer();
        stringer.array();
        stringer.value(-0.0d);
        stringer.value(0.0d);
        stringer.endArray();
        assertEquals("[-0,0]", stringer.toString());
    }

    @Test
    public void testMismatchedScopes() {
        try {
            new JSONStringer().key("a");
            fail();
        } catch (JSONException ignored) {
        }
        try {
            new JSONStringer().value("a");
            fail();
        } catch (JSONException ignored) {
        }
        try {
            new JSONStringer().endObject();
            fail();
        } catch (JSONException ignored) {
        }
        try {
            new JSONStringer().endArray();
            fail();
        } catch (JSONException ignored) {
        }
        try {
            new JSONStringer().array().endObject();
            fail();
        } catch (JSONException ignored) {
        }
        try {
            new JSONStringer().object().endArray();
            fail();
        } catch (JSONException ignored) {
        }
        try {
            new JSONStringer().object().key("a").key("a");
            fail();
        } catch (JSONException ignored) {
        }
        try {
            new JSONStringer().object().value(false);
            fail();
        } catch (JSONException ignored) {
        }
    }

    @Test
    public void testNullKey() {
        try {
            new JSONStringer().object().key(null);
            fail();
        } catch (JSONException ignored) {
        }
    }

    @Test
    public void testRepeatedKey() throws JSONException {
        JSONStringer stringer = new JSONStringer();
        stringer.object();
        stringer.key("a").value(true);
        stringer.key("a").value(false);
        stringer.endObject();
        // JSONStringer doesn't attempt to detect duplicates
        assertEquals("{\"a\":true,\"a\":false}", stringer.toString());
    }

    @Test
    public void testEmptyKey() throws JSONException {
        JSONStringer stringer = new JSONStringer();
        stringer.object();
        stringer.key("").value(false);
        stringer.endObject();
        assertEquals("{\"\":false}", stringer.toString()); // legit behaviour!
    }

    @Test
    public void testEscaping() throws JSONException {
        assertEscapedAllWays("a", "a");
        assertEscapedAllWays("a\\\"", "a\"");
        assertEscapedAllWays("\\\"", "\"");
        assertEscapedAllWays(":", ":");
        assertEscapedAllWays(",", ",");
        assertEscapedAllWays("\\b", "\b");
        assertEscapedAllWays("\\f", "\f");
        assertEscapedAllWays("\\n", "\n");
        assertEscapedAllWays("\\r", "\r");
        assertEscapedAllWays("\\t", "\t");
        assertEscapedAllWays(" ", " ");
        assertEscapedAllWays("\\\\", "\\");
        assertEscapedAllWays("{", "{");
        assertEscapedAllWays("}", "}");
        assertEscapedAllWays("[", "[");
        assertEscapedAllWays("]", "]");
        assertEscapedAllWays("\\u0000", "\0");
        assertEscapedAllWays("\\u0019", "\u0019");
        assertEscapedAllWays(" ", "\u0020");
        assertEscapedAllWays("<\\/foo>", "</foo>");
    }

    private void assertEscapedAllWays(String escaped, String original) throws JSONException {
        assertEquals("{\"" + escaped + "\":false}",
                new JSONStringer().object().key(original).value(false).endObject().toString());
        assertEquals("{\"a\":\"" + escaped + "\"}",
                new JSONStringer().object().key("a").value(original).endObject().toString());
        assertEquals("[\"" + escaped + "\"]",
                new JSONStringer().array().value(original).endArray().toString());
        assertEquals("\"" + escaped + "\"", JSONObject.quote(original));
    }

    @Test
    public void testJSONArrayAsValue() throws JSONException {
        JSONArray array = new JSONArray();
        array.put(false);
        JSONStringer stringer = new JSONStringer();
        stringer.array();
        stringer.value(array);
        stringer.endArray();
        assertEquals("[[false]]", stringer.toString());
    }

    @Test
    public void testJSONObjectAsValue() throws JSONException {
        JSONObject object = new JSONObject();
        object.put("a", false);
        JSONStringer stringer = new JSONStringer();
        stringer.object();
        stringer.key("b").value(object);
        stringer.endObject();
        assertEquals("{\"b\":{\"a\":false}}", stringer.toString());
    }

    @Test
    public void testArrayNestingMaxDepthSupports20() throws JSONException {
        JSONStringer stringer = new JSONStringer();
        for (int i = 0; i < 20; i++) {
            stringer.array();
        }
        for (int i = 0; i < 20; i++) {
            stringer.endArray();
        }
        assertEquals("[[[[[[[[[[[[[[[[[[[[]]]]]]]]]]]]]]]]]]]]", stringer.toString());

        stringer = new JSONStringer();
        for (int i = 0; i < 20; i++) {
            stringer.array();
        }
    }

    @Test
    public void testObjectNestingMaxDepthSupports20() throws JSONException {
        JSONStringer stringer = new JSONStringer();
        for (int i = 0; i < 20; i++) {
            stringer.object();
            stringer.key("a");
        }
        stringer.value(false);
        for (int i = 0; i < 20; i++) {
            stringer.endObject();
        }
        assertEquals("{\"a\":{\"a\":{\"a\":{\"a\":{\"a\":{\"a\":{\"a\":{\"a\":{\"a\":{\"a\":" +
                "{\"a\":{\"a\":{\"a\":{\"a\":{\"a\":{\"a\":{\"a\":{\"a\":{\"a\":{\"a\":false" +
                "}}}}}}}}}}}}}}}}}}}}", stringer.toString());

        stringer = new JSONStringer();
        for (int i = 0; i < 20; i++) {
            stringer.object();
            stringer.key("a");
        }
    }

    @Test
    public void testMixedMaxDepthSupports20() throws JSONException {
        JSONStringer stringer = new JSONStringer();
        for (int i = 0; i < 20; i += 2) {
            stringer.array();
            stringer.object();
            stringer.key("a");
        }
        stringer.value(false);
        for (int i = 0; i < 20; i += 2) {
            stringer.endObject();
            stringer.endArray();
        }
        assertEquals("[{\"a\":[{\"a\":[{\"a\":[{\"a\":[{\"a\":" +
                "[{\"a\":[{\"a\":[{\"a\":[{\"a\":[{\"a\":false" +
                "}]}]}]}]}]}]}]}]}]}]", stringer.toString());

        stringer = new JSONStringer();
        for (int i = 0; i < 20; i += 2) {
            stringer.array();
            stringer.object();
            stringer.key("a");
        }
    }

    @Test
    public void testMaxDepthWithArrayValue() throws JSONException {
        JSONArray array = new JSONArray();
        array.put(false);

        JSONStringer stringer = new JSONStringer();
        for (int i = 0; i < 20; i++) {
            stringer.array();
        }
        stringer.value(array);
        for (int i = 0; i < 20; i++) {
            stringer.endArray();
        }
        assertEquals("[[[[[[[[[[[[[[[[[[[[[false]]]]]]]]]]]]]]]]]]]]]", stringer.toString());
    }

    @Test
    public void testMaxDepthWithObjectValue() throws JSONException {
        JSONObject object = new JSONObject();
        object.put("a", false);
        JSONStringer stringer = new JSONStringer();
        for (int i = 0; i < 20; i++) {
            stringer.object();
            stringer.key("b");
        }
        stringer.value(object);
        for (int i = 0; i < 20; i++) {
            stringer.endObject();
        }
        assertEquals("{\"b\":{\"b\":{\"b\":{\"b\":{\"b\":{\"b\":{\"b\":{\"b\":{\"b\":{\"b\":" +
                "{\"b\":{\"b\":{\"b\":{\"b\":{\"b\":{\"b\":{\"b\":{\"b\":{\"b\":{\"b\":" +
                "{\"a\":false}}}}}}}}}}}}}}}}}}}}}", stringer.toString());
    }

    @Test
    public void testMultipleRoots() throws JSONException {
        JSONStringer stringer = new JSONStringer();
        stringer.array();
        stringer.endArray();
        try {
            stringer.object();
            fail();
        } catch (JSONException ignored) {
        }
    }

    @Test
    public void testEnums() {
        JSONObject x = new JSONObject();
        x.put("a", TimeUnit.SECONDS);
        x.put("b", "xyx");
        JSONStringer s = new JSONStringer();
        s.array();
        s.value(x);
        s.endArray();
        assertEquals("[{\"a\":\"SECONDS\",\"b\":\"xyx\"}]", s.toString());
    }

    @Test
    public void testJsonString() {
        JSONObject x = new JSONObject();
        x.put("a", new Goo());
        JSONStringer s = new JSONStringer();
        s.array();
        s.value(x);
        s.endArray();
        // note lack of quotes
        assertEquals("[{\"a\":fffooo}]", s.toString());
    }

    private static class Goo implements JSONString {
        @Override
        public String toJSONString() {
            return "fffooo";
        }
    }
}
