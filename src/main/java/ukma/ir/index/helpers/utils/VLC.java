package ukma.ir.index.helpers.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class VLC {
    /**
     * encodes passed <code>number</code> to variable length code representation and writes it to file
     * @param out - stream to file where <code>number</code> must be stored
     * @param number - non-negative integer value to be written as VLC
     * @return amount of bytes actually written to file
     * @throws IOException - IO operation with <code>OutputStream</code> occurs
     */
    public static long writeVLC(OutputStream out, int number) throws IOException {
        byte[] vlc = new byte[5]; // 2^(4*7) < 2^31 - 1 < 2^(5*7)

        byte front = 5;
        while (true) {
            vlc[--front] = (byte) (number % 128);
            if (number < 128) break;
            number /= 128; // >> 7 for unsigned only
        }
        vlc[vlc.length - 1] |= 0b10000000; // |= 0b10000000 same as += 128

        out.write(vlc, front, vlc.length - front);
        return vlc.length - front;
    }

    /**
     * reads variable length code representation of a number and restores an initial value
     * @param in - stream from file where VLC is stored
     * @return restored number
     * @throws IOException - IO operation with <code>InputStream</code> occurs
     */
    public static int readVLC(InputStream in) throws IOException {
        int res = 0;
        while (true) {
            byte next = (byte) in.read();
            if (next >= 0) res = res * 128 + next;
            else return res * 128 + (next & 0b01111111);
        }
    }
}
