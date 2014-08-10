package buffer;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class ReadPage {
	
	private File cacheFile;

	private PageMetadata metaData;

	private MappedByteBuffer readBuffer;


	ReadPage(File file) {
		this.cacheFile = file;
		this.metaData = new PageMetadata();
		unlockMetadata();
	}

	ReadPage open() {
		if (readBuffer != null) {
			throw new CacheException("read buffer already open for: '" + cacheFile + "'");
		}
		initBuffer();
		return this;
	}
	
	long getIndex() {
		return metaData.getPageIndex();
	}
	
	long getTimestamp() {
		return metaData.getTimestamp();
	}

    boolean isReadComplete() {
        readBuffer.mark();
        int nextLimit = readBuffer.getInt();
        readBuffer.reset();
        return nextLimit == PageMetadata.EOF;
    }
    
	ByteBuffer read() {	
		readBuffer.mark();
        // slice a chunk
        int chunkSize = readBuffer.getInt();
        if (chunkSize == PageMetadata.EOF) {
            chunkSize = 0;
        }
        if (chunkSize == 0) {
        	readBuffer.reset();
        }
        readBuffer.limit(readBuffer.position() + chunkSize);
        // this will be a zero size slice if we hit EOF or the next buffers length is 0
        final ByteBuffer result = readBuffer.slice();

        // prepare for the next read
        readBuffer.position(readBuffer.position() + chunkSize);
        readBuffer.limit(readBuffer.capacity());
        return result;
	}
	
	ReadPage close() {	
		if (readBuffer == null) {
			throw new CacheException("error buffer already null");
		}
        PageMetadata.Cleaner.clean(readBuffer);
        readBuffer = null;
		return this;
	}
	
	public void delete() {
		cacheFile.delete();
        metaData.close();
        metaData = null;
        cacheFile = null;
	}

	private void initBuffer() {
		try (RandomAccessFile rand = new RandomAccessFile(cacheFile, "r");
		     FileChannel channel = rand.getChannel()) {
			readBuffer = channel.map(READ_ONLY, PageMetadata.METADATA_SIZE, 
												metaData.getFileSize() - PageMetadata.METADATA_SIZE);	
		} catch (IOException ex) {
			throw new CacheException("error reading metadata: '" + cacheFile + "'", ex);
		} 
	}

	private void unlockMetadata() {
		if (!cacheFile.exists()) {
			throw new CacheException("page file does not exists: '" + cacheFile + "'");			
		}	
		try (RandomAccessFile rand = new RandomAccessFile(cacheFile, "rw"); // needed for the lock
			 FileChannel channel = rand.getChannel()) {
			metaData.read(channel.map(READ_ONLY, 0, PageMetadata.METADATA_SIZE));

		} catch (IOException ex) {
			throw new CacheException("error reading metadata: '" + cacheFile + "'", ex);
		}
	}

}
