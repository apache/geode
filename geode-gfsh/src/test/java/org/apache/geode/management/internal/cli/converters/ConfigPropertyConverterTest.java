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
package org.apache.geode.management.internal.cli.converters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.geode.cache.configuration.JndiBindingsType.JndiBinding.ConfigProperty;

/**
 * Unit tests for {@link ConfigPropertyConverter}.
 *
 * <p>
 * Tests the Spring Shell 3.x converter for parsing datasource configuration properties from
 * JSON-style syntax into ConfigProperty arrays.
 */
public class ConfigPropertyConverterTest {

  private ConfigPropertyConverter converter;

  @BeforeEach
  public void setUp() {
    converter = new ConfigPropertyConverter();
  }

  /**
   * Tests conversion of a single ConfigProperty with all three fields (name, type, value).
   */
  @Test
  public void testConvertSinglePropertyWithType() {
    String input = "{'name':'prop1','type':'java.lang.String','value':'value1'}";

    ConfigProperty[] result = converter.convert(input);

    assertThat(result).hasSize(1);
    assertThat(result[0].getName()).isEqualTo("prop1");
    assertThat(result[0].getType()).isEqualTo("java.lang.String");
    assertThat(result[0].getValue()).isEqualTo("value1");
  }

  /**
   * Tests conversion of a single ConfigProperty with only required fields (name, value).
   * The type field is optional and should default to null.
   */
  @Test
  public void testConvertSinglePropertyWithoutType() {
    String input = "{'name':'prop1','value':'value1'}";

    ConfigProperty[] result = converter.convert(input);

    assertThat(result).hasSize(1);
    assertThat(result[0].getName()).isEqualTo("prop1");
    assertThat(result[0].getType()).isNull();
    assertThat(result[0].getValue()).isEqualTo("value1");
  }

  /**
   * Tests conversion of multiple ConfigProperty objects separated by commas.
   */
  @Test
  public void testConvertMultipleProperties() {
    String input =
        "{'name':'prop1','type':'java.lang.String','value':'value1'},{'name':'prop2','type':'java.lang.Integer','value':'42'}";

    ConfigProperty[] result = converter.convert(input);

    assertThat(result).hasSize(2);

    assertThat(result[0].getName()).isEqualTo("prop1");
    assertThat(result[0].getType()).isEqualTo("java.lang.String");
    assertThat(result[0].getValue()).isEqualTo("value1");

    assertThat(result[1].getName()).isEqualTo("prop2");
    assertThat(result[1].getType()).isEqualTo("java.lang.Integer");
    assertThat(result[1].getValue()).isEqualTo("42");
  }

  /**
   * Tests conversion with mixed properties (some with type, some without).
   */
  @Test
  public void testConvertMixedPropertiesWithAndWithoutType() {
    String input =
        "{'name':'prop1','value':'value1'},{'name':'prop2','type':'java.lang.String','value':'value2'}";

    ConfigProperty[] result = converter.convert(input);

    assertThat(result).hasSize(2);

    assertThat(result[0].getName()).isEqualTo("prop1");
    assertThat(result[0].getType()).isNull();
    assertThat(result[0].getValue()).isEqualTo("value1");

    assertThat(result[1].getName()).isEqualTo("prop2");
    assertThat(result[1].getType()).isEqualTo("java.lang.String");
    assertThat(result[1].getValue()).isEqualTo("value2");
  }

  /**
   * Tests flexible field order - fields can appear in any order within an object.
   */
  @Test
  public void testConvertFlexibleFieldOrder() {
    // Test different orderings: name/value/type, value/name/type, type/name/value
    String input1 = "{'name':'prop1','value':'value1','type':'java.lang.String'}";
    String input2 = "{'value':'value2','name':'prop2','type':'java.lang.String'}";
    String input3 = "{'type':'java.lang.String','name':'prop3','value':'value3'}";

    ConfigProperty[] result1 = converter.convert(input1);
    ConfigProperty[] result2 = converter.convert(input2);
    ConfigProperty[] result3 = converter.convert(input3);

    assertThat(result1[0].getName()).isEqualTo("prop1");
    assertThat(result2[0].getName()).isEqualTo("prop2");
    assertThat(result3[0].getName()).isEqualTo("prop3");

    assertThat(result1[0].getValue()).isEqualTo("value1");
    assertThat(result2[0].getValue()).isEqualTo("value2");
    assertThat(result3[0].getValue()).isEqualTo("value3");
  }

  /**
   * Tests conversion with whitespace variations (extra spaces around colons and commas).
   */
  @Test
  public void testConvertWithWhitespace() {
    String input = "{ 'name' : 'prop1' , 'type' : 'java.lang.String' , 'value' : 'value1' }";

    ConfigProperty[] result = converter.convert(input);

    assertThat(result).hasSize(1);
    assertThat(result[0].getName()).isEqualTo("prop1");
    assertThat(result[0].getType()).isEqualTo("java.lang.String");
    assertThat(result[0].getValue()).isEqualTo("value1");
  }

  /**
   * Tests that empty string input returns an empty array.
   */
  @Test
  public void testConvertEmptyString() {
    ConfigProperty[] result = converter.convert("");

    assertThat(result).isEmpty();
  }

  /**
   * Tests that null input returns an empty array.
   */
  @Test
  public void testConvertNullString() {
    ConfigProperty[] result = converter.convert(null);

    assertThat(result).isEmpty();
  }

  /**
   * Tests error handling for missing required 'name' field.
   */
  @Test
  public void testConvertMissingNameField() {
    String input = "{'type':'java.lang.String','value':'value1'}";

    assertThatThrownBy(() -> converter.convert(input))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid config property format")
        .hasMessageContaining("Required fields: 'name', 'value'");
  }

  /**
   * Tests error handling for missing required 'value' field.
   */
  @Test
  public void testConvertMissingValueField() {
    String input = "{'name':'prop1','type':'java.lang.String'}";

    assertThatThrownBy(() -> converter.convert(input))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid config property format")
        .hasMessageContaining("Required fields: 'name', 'value'");
  }

  /**
   * Tests error handling for completely malformed input (no valid objects found).
   */
  @Test
  public void testConvertMalformedInput() {
    String input = "not-valid-json";

    assertThatThrownBy(() -> converter.convert(input))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid datasource-config-properties format");
  }

  /**
   * Tests real-world example from DescribeJndiBindingCommandDUnitTest.
   * This is the actual syntax used in distributed tests.
   */
  @Test
  public void testConvertRealWorldExample() {
    String input =
        "{'name':'prop1','value':'value1','type':'java.lang.String'},"
            + "{'name':'databaseName','value':'newDB','type':'java.lang.String'},"
            + "{'name':'createDatabase','value':'create','type':'java.lang.String'}";

    ConfigProperty[] result = converter.convert(input);

    assertThat(result).hasSize(3);

    assertThat(result[0].getName()).isEqualTo("prop1");
    assertThat(result[0].getValue()).isEqualTo("value1");
    assertThat(result[0].getType()).isEqualTo("java.lang.String");

    assertThat(result[1].getName()).isEqualTo("databaseName");
    assertThat(result[1].getValue()).isEqualTo("newDB");
    assertThat(result[1].getType()).isEqualTo("java.lang.String");

    assertThat(result[2].getName()).isEqualTo("createDatabase");
    assertThat(result[2].getValue()).isEqualTo("create");
    assertThat(result[2].getType()).isEqualTo("java.lang.String");
  }

  /**
   * Tests conversion with special characters in values (e.g., JDBC URLs with colons and slashes).
   */
  @Test
  public void testConvertWithSpecialCharactersInValue() {
    String input =
        "{'name':'url','value':'jdbc:derby:newDB;create=true','type':'java.lang.String'}";

    ConfigProperty[] result = converter.convert(input);

    assertThat(result).hasSize(1);
    assertThat(result[0].getName()).isEqualTo("url");
    assertThat(result[0].getValue()).isEqualTo("jdbc:derby:newDB;create=true");
    assertThat(result[0].getType()).isEqualTo("java.lang.String");
  }
}
