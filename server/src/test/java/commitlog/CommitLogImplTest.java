package commitlog;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;

import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class CommitLogImplTest {
    private Path tempPath;
    private CommitLog commitLog;

    @Before
    public void setUp() throws Exception {
        tempPath = Files.createTempDirectory("commit-log-test");
        commitLog = new CommitLogImpl(tempPath);
        commitLog.start();
    }

    @After
    public void tearDown() throws Exception {
        commitLog.shutdown();
        tempPath.toFile().delete();
    }

    @Test
    public void replay_commit_log_entries() throws Exception {
        byte[] firstPayload = "First Write".getBytes();
        byte[] secondPayload = "Second Write".getBytes();

        commitLog.store(ByteBuffer.wrap(firstPayload));
        commitLog.store(ByteBuffer.wrap(secondPayload));
        final Iterator<ByteBuffer> read = commitLog.read();

        final byte[] firstResult = toArray(read.next());
        assertArrayEquals(Arrays.toString(firstResult), firstPayload, firstResult);
        final byte[] secondResult = toArray(read.next());
        assertArrayEquals(Arrays.toString(secondResult), secondPayload, secondResult);
        assertFalse(read.hasNext());
    }

    @Test
    public void crossing_segments() throws Exception {
        byte[] firstPayload = new byte[1019];
        byte[] secondPayload = new byte[] {3,3};

        commitLog.store(ByteBuffer.wrap(firstPayload));
        commitLog.store(ByteBuffer.wrap(secondPayload));
        final Iterator<ByteBuffer> read = commitLog.read();

        final byte[] firstResult = toArray(read.next());
        assertArrayEquals(Arrays.toString(firstResult), firstPayload, firstResult);
        final byte[] secondResult = toArray(read.next());
        assertArrayEquals(Arrays.toString(secondResult), secondPayload, secondResult);
        assertFalse(read.hasNext());
    }

    @Ignore
    @Test
    public void test_sandbox() throws Exception {
        Path tempFile = Paths.get("/Users/chbatey/tmp/test");
        CommitLog commitLog = new CommitLogImpl(tempFile);
        commitLog.start();
        final ByteBuffer bb = ByteBuffer.allocate(8).putLong(1);
        bb.flip();
        commitLog.store(bb);
    }

    private static byte[] toArray(ByteBuffer bb) {
        byte[] array = new byte[bb.remaining()];
        bb.get(array);
        return array;
    }
}