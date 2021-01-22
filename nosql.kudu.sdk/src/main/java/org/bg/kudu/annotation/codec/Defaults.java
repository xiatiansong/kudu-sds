package org.bg.kudu.annotation.codec;

import org.bg.kudu.annotation.DataType;

/**
 * @author: xiatiansong
 * @create: 2019-01-02 14:43
 **/
public class Defaults {

    /**
     * A fake codec implementation to use as the default in mapping annotations.
     */
    public static abstract class NoCodec extends TypeCodec<String> {
        private NoCodec() {
            super(DataType.INT, String.class);
        }
    }
}