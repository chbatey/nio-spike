package commitlog.custom;

import commitlog.CommitLog;
import commitlog.CommitLogImpl;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;

// not a good idea :)
public class BasicBenchmark {
    public static void main(String[] args) throws Exception {
        Path path = Paths.get("/Users/chbatey/tmp/commitlog");
        CommitLog commitLog = new CommitLogImpl(path, 2 << 29);
        commitLog.start();

        for (int i = 0; i < 1000000; i++) {
            final ByteBuffer wrap = ByteBuffer.wrap(String.valueOf(i).getBytes());
            commitLog.store(wrap);
        }

        commitLog.shutdown();
    }
}
