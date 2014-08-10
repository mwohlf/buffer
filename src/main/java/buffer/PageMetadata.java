package buffer;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

public class PageMetadata {
	
	public static final int INT_SIZE = 4;

    static final int EOF = Integer.MIN_VALUE;
    
    static final int FILE_SIZE_POS = 8;
    static final int TIMESTAMP_POS = 16;
    static final int PAGE_INDEX_POS = 24;

	static final int METADATA_SIZE = 32;

	
	private MappedByteBuffer buffer;

	private long fileSize;

	private long timestamp;

	private long pageIndex;
	
	
	public void read(MappedByteBuffer readBuffer) {
		this.buffer = readBuffer;
	}
	
	public void write(MappedByteBuffer writeBuffer) {
		this.buffer = writeBuffer;		
	}

	public void force() {
		buffer.putLong(FILE_SIZE_POS, fileSize);
		buffer.putLong(TIMESTAMP_POS, timestamp);
		buffer.putLong(PAGE_INDEX_POS, pageIndex);
		buffer.putInt(METADATA_SIZE, 0); // first chunk size
		buffer.force();
	}

	public void close() {
        PageMetadata.Cleaner.clean(buffer);
        buffer = null;
	}

	public void setFileSize(long fileSize) {
		this.fileSize = fileSize;
	}
	
	public long getFileSize() {
		return buffer.getLong(FILE_SIZE_POS);
	}	

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;	
	}
	
	public long getTimestamp() {
		return buffer.getLong(TIMESTAMP_POS);
	}
	
	public void setPageIndex(long pageIndex) {
		this.pageIndex = pageIndex;			
	}
	
	public long getPageIndex() {
		return buffer.getLong(PAGE_INDEX_POS);
	}
	
	// run a cleaner on the ByteBuffer, this seems to be common practice...
    // see: http://stackoverflow.com/questions/1854398/how-to-garbage-collect-a-direct-buffer-java
    //      https://github.com/bulldog2011/bigqueue/blob/master/src/main/java/com/leansoft/bigqueue/page/MappedPageImpl.java
    static class Cleaner {
        private static final boolean CLEAN_SUPPORTED;
        private static final Method directBufferCleaner;
        private static final Method directBufferCleanerClean;

        static {
            Method directBufferCleanerX = null;
            Method directBufferCleanerCleanX = null;
            boolean v;
            try {
                directBufferCleanerX = Class.forName("java.nio.DirectByteBuffer").getMethod("cleaner");
                directBufferCleanerX.setAccessible(true);
                directBufferCleanerCleanX = Class.forName("sun.misc.Cleaner").getMethod("clean");
                directBufferCleanerCleanX.setAccessible(true);
                v = true;
            } catch (Exception ex) {
                v = false;
            }
            directBufferCleaner = directBufferCleanerX;
            directBufferCleanerClean = directBufferCleanerCleanX;
            CLEAN_SUPPORTED = v && directBufferCleaner != null && directBufferCleanerClean != null;
        }

        public static void clean(ByteBuffer buffer) {
            if (CLEAN_SUPPORTED && buffer.isDirect()) {
                try {
                    Object cleaner = directBufferCleaner.invoke(buffer);
                    if (cleaner != null) {
                        directBufferCleanerClean.invoke(cleaner);
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
            System.gc();
        }
    }

}
