package commitlog.jmh;

import commitlog.CommitLog;
import commitlog.CommitLogImpl;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;

public class CommitLogBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        final Path path;
        final CommitLog commitLog;

        public BenchmarkState() {
            try {
//                path = Files.createTempFile("commit-log-benchmark", "");
                path = Paths.get("/Users/chbatey/tmp/commitlog");
                commitLog = new CommitLogImpl(path, 2 << 29);
                commitLog.start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @TearDown
        public void tearDown() throws IOException, InterruptedException {
            System.out.println("Stopping");
            commitLog.shutdown();
        }
    }

    @State(Scope.Thread)
    public static class ThreadState {
        long toWrite = 0;
    }

    @Benchmark
    public void wellHelloThere(ThreadState ts, BenchmarkState benchmarkState) throws IOException, InterruptedException {
        final ByteBuffer allocate = ByteBuffer.allocate(8);
        allocate.putLong(ts.toWrite++);
        allocate.flip();
        benchmarkState.commitLog.store(allocate);
    }
}
