/*=========================================================================
 * (c)Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.query.export.xml;

import java.io.PrintWriter;
import java.io.StringWriter;

import com.gemstone.gemfire.mgmt.DataBrowser.query.ColumnNotFoundException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionResult;
import com.gemstone.gemfire.mgmt.DataBrowser.query.QueryUtil;
import com.gemstone.gemfire.mgmt.DataBrowser.query.export.QuerySchemaExportHandler;


public class XMLSchemaExportHandler implements QuerySchemaExportHandler {

  private static final String STRUCT_TYPE_SCHEMA_NAME = "StructType";
  private static final String COLLECTION_TYPE_SCHEMA_NAME = "CollectionType";

  private StringWriter schema_buffer;
  private PrintWriter schema_writer;
  private boolean isCollectionTypeDefined;

  public XMLSchemaExportHandler() {
    this.schema_buffer = new StringWriter();
    this.schema_writer = new PrintWriter(this.schema_buffer);
    this.isCollectionTypeDefined = false;
  }

  public String getSchema() {
    return this.schema_buffer.toString();
  }

  public void handleEndSchema() {
    schema_writer.println();
    schema_writer.println("</xsd:schema>");
    schema_writer.println();
  }

  public void handleStartSchema() {
    schema_writer.println("<xsd:schema xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\">");
    schema_writer.println();

    schema_writer.println("<xsd:element name=\"QueryResult\" type=\"CollectionType\"/>");
    schema_writer.println();
  }

  public void handleCollectionType(IntrospectionResult metaInfo) {
    if(!this.isCollectionTypeDefined) {
      schema_writer.println("<xsd:complexType name=\""+COLLECTION_TYPE_SCHEMA_NAME+"\" >");
      schema_writer.println("<xsd:sequence>");
      schema_writer.println("<xsd:element name=\"element\"   type=\"xsd:anyType\"  minOccurs=\"0\" maxOccurs=\"unbounded\" />");
      schema_writer.println("</xsd:sequence>");
      schema_writer.println("</xsd:complexType>");
      schema_writer.println();

      this.isCollectionTypeDefined = true;
    }
  }

  public void handleCompositeType(IntrospectionResult metaInfo) throws ColumnNotFoundException {
    schema_writer.print("<xsd:complexType name=\"");
    schema_writer.print(metaInfo.getJavaType().getSimpleName());
    schema_writer.println("\">");

    schema_writer.println("<xsd:complexContent>");

    //In case of Composite type, check if the type has a custom super type (which should not be
    //equal to java.lang.Object.class). In this case we prepare the schema definition using the
    //extension mechanism. Else, we use XML schema restriction mechanism.
    boolean extenstion = false;
    Class superClass = metaInfo.getJavaType().getSuperclass();
    if(superClass != null && !java.lang.Object.class.equals(superClass)) {
      extenstion = true;
      String superType = superClass.getSimpleName();
      schema_writer.println("<xsd:extension base=\""+superType+"\">");

    } else {
      extenstion = false;
      schema_writer.println("<xsd:restriction base=\"xsd:anyType\">");
    }

    schema_writer.println("<xsd:sequence>");

    for(int k = 0 ; k < metaInfo.getColumnCount() ; k++) {
      int columnType = metaInfo.getColumnType(k);
      String colName = metaInfo.getColumnName(k);
      String xmlType = null;
      boolean isOptional = TypeConversion.isOptional(metaInfo.getColumnClass(k));
      int minOccur = (isOptional) ? 0 : -1;

      //If this column is not declared in this type, then do not redefine the same,
      //since it will always be present in the super-type declaration.
      if(!QueryUtil.isColumnDeclaredInType(metaInfo, k)) {
       continue;
      }

      if(columnType == IntrospectionResult.PRIMITIVE_TYPE_COLUMN) {
        xmlType = TypeConversion.getXmlType(metaInfo.getColumnClass(k));
      } else if (columnType == IntrospectionResult.COMPOSITE_TYPE_COLUMN) {
        xmlType = metaInfo.getColumnClass(k).getSimpleName();
     }  else if(columnType == IntrospectionResult.COLLECTION_TYPE_COLUMN) {
        xmlType = "CollectionType";
     }  else {
        xmlType = metaInfo.getColumnClass(k).getSimpleName();
     }

      handleSchemaElement(colName, xmlType, minOccur, false, -1);
    }

    schema_writer.println("</xsd:sequence>");

    if(extenstion) {
      schema_writer.println("</xsd:extension>");
    } else {
      schema_writer.println("</xsd:restriction>");
    }

    schema_writer.println("</xsd:complexContent>");
    schema_writer.println("</xsd:complexType>");
    schema_writer.println();
  }

  public void handleMapType(IntrospectionResult metaInfo) {
    //Do nothing.
  }

  public void handlePrimitiveType(IntrospectionResult metaInfo) throws ColumnNotFoundException {
    String javaType = metaInfo.getColumnClass(0).getSimpleName();
    String colName = metaInfo.getColumnName(0);
    String elementName = javaType+colName;
    String xmlType = TypeConversion.getXmlType(metaInfo.getColumnClass(0));
    boolean isOptional = TypeConversion.isOptional(metaInfo.getColumnClass(0));
    int minOccur = (isOptional) ? 0 : -1;

    handleSchemaElement(elementName, xmlType, minOccur, true, -1);
  }

  public void handleStructType(IntrospectionResult metaInfo) throws ColumnNotFoundException {
    schema_writer.print("<xsd:complexType name=\"");
    schema_writer.print(STRUCT_TYPE_SCHEMA_NAME);
    schema_writer.println("\">");

    schema_writer.println("<xsd:sequence>");

    for(int k = 0 ; k < metaInfo.getColumnCount() ; k++) {
      String colName = metaInfo.getColumnName(k);
      handleSchemaElement(colName, "xsd:anyType", 0, false, -1);
    }

    schema_writer.println("</xsd:sequence>");
    schema_writer.println("</xsd:complexType>");
    schema_writer.println();
  }

  private void handleSchemaElement(String name, String type, int minOccur, boolean includeMaxOccur, int maxOccur) {
    schema_writer.print("<xsd:element name=\"");
    schema_writer.print(name);
    schema_writer.print("\"   type=\"");
    schema_writer.print(type);
    schema_writer.print("\" ");
    if(minOccur >= 0) {
      schema_writer.print(" minOccurs=\""+minOccur+"\" ");
    }
    if(includeMaxOccur){
      if(maxOccur == -1) {
        schema_writer.print(" maxOccurs=\"unbounded\"");
      } else {
        schema_writer.print(" maxOccurs=\""+maxOccur+"\" ");
      }
    }

    schema_writer.println(" />");
  }

  @Override
  public String toString() {
    return this.schema_buffer.toString();
  }

}
