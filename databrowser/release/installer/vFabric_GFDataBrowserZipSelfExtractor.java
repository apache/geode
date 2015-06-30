import java.io.IOException;

public class vFabric_GFDataBrowserZipSelfExtractor extends ZipSelfExtractor {

  protected void createJars() throws IOException {
    //blank for now
  }
  
  protected String getProductJarName() {
    throw new IllegalStateException();
  }
  
  protected String getInstallDirProperty() {
    return "gemstone.installer.directory";
  }
 
  protected String getInstallOpenSourceProperty() {
    return "gemstone.installer.opensource";
  }

  vFabric_GFDataBrowserZipSelfExtractor() {
    super();
    //Do not allow open source to be installed for tools at present
    System.setProperty(getInstallOpenSourceProperty(), "false");
  }
  
  public static void main(String[] args) throws Throwable
  {
    ZipSelfExtractor zse = new vFabric_GFDataBrowserZipSelfExtractor();
    zse.installProduct();
  }
}
