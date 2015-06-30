/*=========================================================================
 * (c)Copyright 2002-2011, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.query.export.xml;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionResult;
import com.gemstone.gemfire.mgmt.DataBrowser.query.export.QueryDataExportHandler;

public class XMLDocExportHandler implements QueryDataExportHandler {

  private FileWriter file_buffer = null;
  private PrintWriter writer = null;

  public XMLDocExportHandler(File docFile) throws IOException {
   this.file_buffer = new FileWriter(docFile);
   this.writer = new PrintWriter(file_buffer);
  }

  public void handleEndDocument() {
    writer.println("</QueryResult>");
  }

  public void handleStartDocument() {
    writer.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    writer.println();
    writer.println("<QueryResult xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"");
    writer.println("             xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\">");
  }

  public void handleStartCollectionElement(String name, IntrospectionResult metaInfo, Object element) {
   //Do nothing.
  }


  public void handleEndCollectionElement(String name) {
   // Do nothing.
  }

  public void handleEndCompositeType(String name, IntrospectionResult metaInfo, Object value) {
    writer.println("</" + name + ">");
  }

  public void handleStartCompositeType(String name, IntrospectionResult metaInfo, Object value) {
    String xmlType = metaInfo.getJavaType().getSimpleName();
    String nm = ( null != name ) ? name : xmlType;

    writer.print("<" + nm);
    writer.print(" xsi:type=\""+xmlType+"\"");
    writer.println(">");
  }

  public void handleStartStructType(String name, IntrospectionResult metaInfo, Object val) {
    writer.print("<" + name);
    writer.print(" xsi:type=\""+STRUCT_TYPE_NAME+"\"");
    writer.println(">");
  }

  public void handleEndStructType(String name, IntrospectionResult metaInfo, Object val) {
    writer.println("</" + name + ">");
  }

  public Object getResultDocument() {
    return this.file_buffer;
  }

  public void close() throws IOException {
    this.file_buffer.flush();
    this.file_buffer.close();
  }

  public void handlePrimitiveType(String name, Class type, Object value) {
    if(value == null) {
     return;
    }

    String xmlType = TypeConversion.getXmlType(type);

    writer.print("<"+name);
    if(type != null)
     writer.print(" xsi:type=\""+xmlType+"\"");
     writer.print(">");
     writer.print(String.valueOf(value));
     writer.print("</"+name+">");
     writer.println();
  }

  public void handleEndCollectionType(String name, String typeName, Object value) {
    if(!QUERY_RESULT_NAME.equals(name)) {
      writer.println("</" + name + ">");
    }
  }

  public void handleStartCollectionType(String name, String typeName, Object value)  {
    if(!QUERY_RESULT_NAME.equals(name)) {
      writer.println("<" + name + " xsi:type=\""+COLLECTION_TYPE_NAME+"\">");
    }
  }

  public void handleEndPdxType(String name, IntrospectionResult metaInfo,
      Object value) {
    writer.println("</" + name + ">");
  }

  public void handleStartPdxType(String name, IntrospectionResult metaInfo,
      Object value) {
    String xmlType = metaInfo.getJavaTypeName();
    String nm = ( null != name ) ? name : xmlType;

    writer.print("<" + nm);
    writer.print(" xsi:type=\""+xmlType+"\"");
    writer.println(">");
  }
}
