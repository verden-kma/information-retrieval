package ukma.ir.index.helpers;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;

public class VLC {
    public static long writeVLC(BufferedOutputStream fleck, int number) throws IOException {
        byte[] vlc = new byte[5]; // 2^(4*7) < 2^31 - 1 < 2^(5*7)

        byte front = 5;
        while (true) {
            vlc[--front] = (byte) (number % 128);
            if (number < 128) break;
            number /= 128; // >> 7 for unsigned only
        }
        vlc[vlc.length - 1] |= 0b10000000; // |= 0b10000000 += 128

        fleck.write(vlc, front, vlc.length - front);
        return vlc.length - front;
    }

    static int readVLC(BufferedInputStream bis) throws IOException {
        int res = 0;
        while (true) {
            byte next = (byte) bis.read();
            if (next >= 0) res = res * 128 + next;
            else return res * 128 + (next & 0b01111111);
        }
    }
}
