package utils.common;/*
 * Copyright (C) 2022, Zirui Hu, Rong Yu, Jinkai Xu, Yao Luo, Qingshuai Wang
 */

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class FileUtil {

    public void copyFile(File in, File out)
            throws IOException {
        FileInputStream strIn = new FileInputStream(in);
        FileOutputStream strOut = new FileOutputStream(out);
        byte[] buf = new byte[65536];
        int len = strIn.read(buf);
        while (len > 0) {
            strOut.write(buf, 0, len);
            len = strIn.read(buf);
        }
        strOut.close();
        strIn.close();
    }
}
