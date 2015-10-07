package commitlog;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

class Segment implements Iterable<ByteBuffer> {

    public static final int LENGTH_SIZE = 4;
    private final Path path;
    private final long segmentSize;
    private final FileChannel file;
    private final MappedByteBuffer map;
    private final AtomicInteger offset = new AtomicInteger(0);

    public Segment(Path path, long segmentSize) throws IOException {
        this.path = path;
        this.segmentSize = segmentSize;
        File file = this.path.toFile();
        if (!file.exists()) {
            file.createNewFile();
        }
        this.file = FileChannel.open(path, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.READ, StandardOpenOption.WRITE);
        map = this.file.map(FileChannel.MapMode.READ_WRITE, 0, segmentSize);
    }

    public boolean store(ByteBuffer entry) {
        int currentOffset = offset.get();
        int bufferSize = entry.remaining();
        boolean applied = false;
        while (!applied) {
            currentOffset = offset.get();
            if ((currentOffset + bufferSize + LENGTH_SIZE) > segmentSize) {
                return false;
            }
            applied = offset.compareAndSet(currentOffset, currentOffset + bufferSize + LENGTH_SIZE);
        }
        final ByteBuffer duplicate = map.duplicate();
        duplicate.position(currentOffset);
        duplicate.putInt(bufferSize);
        duplicate.put(entry);
        map.force();
        return true;
    }

    public boolean hasSpace(int size) {
        return !((offset.get() + size + LENGTH_SIZE) > segmentSize);
    }

    public void close() throws IOException {
        file.close();
    }

    @Override
    public Iterator<ByteBuffer> iterator() {
        return new ByteBufferIterator(path);
    }

    private static class ByteBufferIterator implements Iterator<ByteBuffer> {
        private int currentLength;
        private final DataInputStream inputStream;

        public ByteBufferIterator(Path path)  {
            try {
                inputStream = new DataInputStream(Files.newInputStream(path, StandardOpenOption.READ));
                currentLength = inputStream.readInt();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean hasNext() {
            return currentLength != 0;
        }

        @Override
        public ByteBuffer next() {
            if (!hasNext()) throw new NoSuchElementException();

            byte[] nextArray = new byte[currentLength];
            try {
                inputStream.read(nextArray);
                if (inputStream.available() > 4) {
                    currentLength = inputStream.readInt();
                } else {
                    currentLength = 0;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return ByteBuffer.wrap(nextArray);
        }
    }
}
