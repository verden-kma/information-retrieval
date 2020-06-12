package ukma.ir.index.helpers;

import java.io.IOException;

public class CacheException extends IOException {
    public CacheException(String message, Exception e) {
        super(message, e);
    }

    public CacheException(String message) {
        super(message);
    }
}
