package com.gemstone.gemfire.codeAnalysis.decode.cp;
import java.io.*;

import com.gemstone.gemfire.codeAnalysis.decode.CompiledClass;


public class CpFieldref extends Cp {
    int class_index;
    int name_and_type_index;
    CpFieldref( DataInputStream source ) throws IOException {
        class_index = source.readUnsignedShort();
        name_and_type_index = source.readUnsignedShort();
    }
    public String returnType(CompiledClass info) {
        return "not yet implemented";
    }
}