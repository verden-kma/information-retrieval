package ukma.ir;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ukma.ir.index.helpers.utils.VLC;

import java.io.*;
import java.nio.file.Files;
import java.util.Arrays;

public class TestVLC {
    @TempDir
    File vlcDir;
    static final String FILE_NAME = "vlcFile.bin";
    OutputStream output;
    InputStream input;

    @BeforeEach
    void initStreams() throws FileNotFoundException {
        output = new FileOutputStream(new File(vlcDir, FILE_NAME));
        input = new FileInputStream(new File(vlcDir, FILE_NAME));
    }

    @Test
    void testCorrectNumbers() throws IOException {
        int[] values = new int[]{0, 5, 25, 625, 390_625};
        for (int n : values) {
            VLC.writeVLC(output, n);
        }
        int[] read = new int[values.length];
        for (int i = 0; i < read.length; i++) {
            read[i] = VLC.readVLC(input);
        }
        Assertions.assertArrayEquals(values, read);
    }

    @Test
    void testNegativeNumbers() throws IOException {
        int[] values = new int[]{0, -5, -25, -625, -390_625};
        for (int n : values) {
            VLC.writeVLC(output, n);
        }
        int[] read = new int[values.length];
        for (int i = 0; i < read.length; i++) {
            read[i] = VLC.readVLC(input);
        }
        Assertions.assertFalse(Arrays.equals(values, read));
    }

    @AfterEach
    void clear() throws IOException {
        output.close();
        input.close();
        Files.delete(new File(vlcDir, FILE_NAME).toPath());
    }
}
