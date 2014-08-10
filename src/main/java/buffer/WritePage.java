package buffer;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;




public class WritePage {

	private File cacheFile;

	private PageMetadata metaData;

	private MappedByteBuffer writeBuffer;


	WritePage(File file, long fileSize, long timestamp, long pageIndex) {
		this.cacheFile = file;
		this.metaData = new PageMetadata();
		metaData.setFileSize(fileSize);
		metaData.setTimestamp(timestamp);
		metaData.setPageIndex(pageIndex);
		writeMetadata();
	}

	// attach the write buffer and apply the final size of the page
	WritePage open() {
		if (writeBuffer != null) {
    		throw new CacheException("write buffer already open");
		}
		initBuffer();
		return this;
	}
	
	// atomic move makes the page available to readers, make the file available to readers after
	//  - metadata have been written
	//  - the first buffer size have been written 
	//  - the file size is correct so readers can mapp the content to memory
	WritePage atomicMove(File file) {
		try {
			Files.move(cacheFile.toPath(), file.toPath(), ATOMIC_MOVE);
			return this;
		} catch (IOException ex) {
			throw new CacheException("error writing metadata: '" + cacheFile + "'", ex);
		} 
	}
	
    long remainingForWrite() {
        writeBuffer.mark();
        int marker = writeBuffer.getInt();
        writeBuffer.reset();
        if (marker == PageMetadata.EOF) {
            return 0;
        }
        return writeBuffer.remaining() 
        		- PageMetadata.INT_SIZE  // the size just read
                - PageMetadata.INT_SIZE; // the EOF 
    }

    void write(ByteBuffer incoming) {
    	if (writeBuffer == null) {
    		throw new CacheException("write buffer is closed");
    	}
        int chunksize = incoming.limit() - incoming.position();
        if (remainingForWrite() < chunksize) {
            writeBuffer.mark();
            writeBuffer.putInt(PageMetadata.EOF);
            writeBuffer.reset();
            writeBuffer.force();
        } else {
            int offsetChunksize = writeBuffer.position();
            writeBuffer.putInt(0);  // will be overwritten
            writeBuffer.put(incoming);
            writeBuffer.force();
            writeBuffer.putInt(offsetChunksize, chunksize);
            writeBuffer.force();
        }
    }

	WritePage close() {	
		if (writeBuffer == null) {
			throw new CacheException("page file already closed: '" + cacheFile + "'");			
		}
        writeBuffer.putInt(PageMetadata.EOF);
        writeBuffer.force();
        PageMetadata.Cleaner.clean(writeBuffer);
        writeBuffer = null;
        metaData.close();
        metaData = null;
        cacheFile = null;
		return this;
	}

	// the metadata including the first buffer offset needs to be written when
	// this page is not accessible to readers
	private void writeMetadata() {
		if (cacheFile.exists()) {
			throw new CacheException("page file already exists: '" + cacheFile + "'");			
		}	
		try (RandomAccessFile rand = new RandomAccessFile(cacheFile, "rw");
				FileChannel channel = rand.getChannel()) {
			metaData.write(channel.map(READ_WRITE, 0, PageMetadata.METADATA_SIZE + PageMetadata.INT_SIZE));
			metaData.force();
		} catch (IOException ex) {
			throw new CacheException("error writing metadata: '" + cacheFile + "'", ex);
		} 
	}

	private void initBuffer() {
		try (RandomAccessFile rand = new RandomAccessFile(cacheFile, "rw");
				FileChannel channel = rand.getChannel()) {
			writeBuffer = channel.map(READ_WRITE, PageMetadata.METADATA_SIZE, 
												  metaData.getFileSize() - PageMetadata.METADATA_SIZE);	
		} catch (IOException ex) {
			throw new CacheException("error reading metadata: '" + cacheFile + "'", ex);
		} 
	}

}
