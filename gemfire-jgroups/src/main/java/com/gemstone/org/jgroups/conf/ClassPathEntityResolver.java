/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: ClassPathEntityResolver.java,v 1.3 2005/08/08 14:58:32 belaban Exp $

package com.gemstone.org.jgroups.conf;

/**
 * 
 * @author Filip Hanik (<a href="mailto:filip@filip.net">filip@filip.net)
 * @author Bela Ban
 * @version $Id: ClassPathEntityResolver.java,v 1.3 2005/08/08 14:58:32 belaban Exp $
 */

import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.gemstone.org.jgroups.util.Util;

import java.io.InputStream;
import java.io.IOException;
import java.net.URL;

public class ClassPathEntityResolver implements EntityResolver {
    public String mDefaultJGroupsDTD="com/gemstone/gemfire/distributed/internal/javagroups-protocol.dtd"; //GemStoneAddition
    //public String mDefaultJGroupsDTD="jgroups-protocol.dtd";

    public ClassPathEntityResolver() {
    }

    public ClassPathEntityResolver(String dtdName) {
        mDefaultJGroupsDTD=dtdName;
    }

    public InputSource resolveEntity(String publicId, String systemId) throws SAXException, IOException {
        InputSource source = new InputSource(getInputStream(mDefaultJGroupsDTD)); // GemStoneAddition
        //InputSource source=new InputSource(getInputStream(systemId));
        return source;
    }

    protected InputStream getInputStream(String dtdurl)
            throws java.io.IOException {
        String url=dtdurl;
        if(url == null) url=mDefaultJGroupsDTD;
        //1. first try to load the DTD from an actual URL
        try {
            URL inurl=new URL(url);
            return inurl.openStream();
        }
        catch(Exception ignore) {
        }
        //2. then try to load it from the classpath
        
        InputStream stream=Util.getResourceAsStream(url, this.getClass());
        if(stream == null) {
            throw new IOException("Could not locate the DTD with name:[" + url + "] in the classpath.");
        }
        else
            return stream;
    }
}
