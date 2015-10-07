package commitlog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public interface CommitLog {
    void store(ByteBuffer entry) throws IOException, InterruptedException;
    Iterator<ByteBuffer> read() throws IOException;

    void start();
    void shutdown() throws IOException, InterruptedException;
}
