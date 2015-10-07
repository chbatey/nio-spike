package commitlog;

import org.HdrHistogram.Histogram;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.StreamSupport;

public class CommitLogImpl implements CommitLog {

    private static Histogram histogram = new Histogram(3600000000000L, 3);

    private final Path path;
    private final long segmentSize;
    private final Queue<Segment> oldSegments = new ConcurrentLinkedQueue<>();
    private final ReentrantLock newSegmentLock = new ReentrantLock();

    private volatile Segment currentSegment;
    private int currentIndex = 1;

    public CommitLogImpl(Path path, long segmentSize) throws IOException {
        this.path = path;
        this.segmentSize = segmentSize;
        File file = this.path.toFile();
        if (!file.exists()) {
            file.mkdir();
        }
        final Path path1 = newSegmentPath(file);
        this.currentSegment = new Segment(path1, segmentSize);
    }

    private Path newSegmentPath(File file) {
        return Paths.get(file.getAbsolutePath(), "/", currentIndex + ".db");
    }

    public CommitLogImpl(Path path) throws IOException {
        this(path, 1024);
    }

    @Override
    public void store(ByteBuffer entry) throws IOException, InterruptedException {
        long start = System.nanoTime();
        int size = entry.remaining();
        // in a while loop as other threads could be filling this up before we get back
        while (!currentSegment.store(entry)) {
            createNewSegment(size);
        }
        histogram.recordValue(System.nanoTime() - start);
    }

    private void createNewSegment(int size) throws IOException {
        newSegmentLock.lock();
        try {
            if (currentSegment.hasSpace(size)) {
                // some one else has created the segment
                return;
            } else {
                oldSegments.add(currentSegment);
                currentIndex++;
                currentSegment = new Segment(newSegmentPath(path.toFile()), segmentSize);
            }
        } finally {
            newSegmentLock.unlock();
        }
    }


    // doesn't happen concurrently with writes
    @Override
    public Iterator<ByteBuffer> read() throws IOException {
        List<Segment> all = new ArrayList<>();
        all.addAll(oldSegments);
        all.add(currentSegment);
        final Iterable<ByteBuffer> concat = concat(all);
        return concat.iterator();
    }

    private static <T> Iterable<T> concat(List<? extends Iterable<T>> foo) {
        return () -> StreamSupport.stream(foo.spliterator(), false)
                .flatMap(i -> StreamSupport.stream(i.spliterator(), false))
                .iterator();
    }

    @Override
    public void start() {
    }

    @Override
    public void shutdown() throws IOException, InterruptedException {
        currentSegment.close();
        histogram.outputPercentileDistribution(System.out, 1000.0);
    }
}
