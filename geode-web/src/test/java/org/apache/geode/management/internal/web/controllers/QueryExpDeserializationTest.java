package org.apache.geode.management.internal.web.controllers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;

import javax.management.Query;
import javax.management.QueryExp;

import org.apache.commons.io.serialization.ValidatingObjectInputStream;
import org.junit.Test;

public class QueryExpDeserializationTest {
  @Test
  public void testQueryExp() throws Exception {
    QueryExp query = Query.eq(Query.attr("Name"), Query.value("mock"));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(query);
    oos.close();

    byte[] decoded = baos.toByteArray();
    ValidatingObjectInputStream ois =
        new ValidatingObjectInputStream(new ByteArrayInputStream(decoded));
    ois.accept("javax.management.*", "java.lang.*", "java.util.*");

    QueryExp q = (QueryExp) ois.readObject();
    System.out.println(q);
  }
}
