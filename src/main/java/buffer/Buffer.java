package buffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Buffer {
	
	File directory;
	
	long pageSize = 10000;
	
	final WriteStream writeStream = new WriteStream();
	final WritePageFactory writePageFactory = new WritePageFactory();

	final ReadStream readStream = new ReadStream();
	final ReadPageFactory readPageFactory = new ReadPageFactory();
	

	public void setPageSize(int size) {
		writePageFactory.setPageSize(size);
	}
	
	public void setCacheDir(File cacheDir) {
		readPageFactory.setCacheDir(cacheDir);
		writePageFactory.setCacheDir(cacheDir);
	}
			
	IWriteStream getWriteStream(long timestamp) {
		if (writeStream.open) {
			throw new CacheException("WriteStream is already open");
		}
		writeStream.open = true;
		writePageFactory.initialize();
		writeStream.currentPage = writePageFactory.create(timestamp);
		return writeStream;
	}
	

	// this might block if nothing has been written yet
	IReadStream getReadStream(long timestamp) {
		if (readStream.open) {
			throw new CacheException("WriteStream is already open");
		}
		readStream.open = true;
		readPageFactory.initialize();
		readStream.currentPage = readPageFactory.findPageBefore(timestamp);
		return readStream;
	}

	
	
	
	class ReadStream implements IReadStream {

		boolean open;
		ReadPage currentPage;
		
		@Override
		public ByteBuffer read() {
			ByteBuffer result = currentPage.read();
			while (result.remaining() == 0) {
				if (currentPage.isReadComplete()) {
					ReadPage nextPage = readPageFactory.getNextPage(currentPage);
					currentPage.close();
					currentPage = nextPage;
					currentPage.open();
				}
				try {
					Thread.sleep(1);  
				} catch (InterruptedException ex) {
					throw new CacheException(ex);
				}
				result = currentPage.read();				
			}
			return result;
		}
		
		@Override
		public void close() throws IOException {
			open = false;
			currentPage.close();
			readPageFactory.close();
		}
		
	}
	
	
	
	class WriteStream implements IWriteStream {

		private boolean open;
		WritePage currentPage;

		
		@Override
		public void write(long timestamp, ByteBuffer buffer) {
			if (currentPage.remainingForWrite() < buffer.remaining()) {
				currentPage.close();
				currentPage = writePageFactory.create(timestamp);
			}
			if (currentPage.remainingForWrite() < buffer.remaining()) {
				throw new CacheException("buffer too big for a new page");
			}
			currentPage.write(buffer);
		}

		@Override
		public void close() throws IOException {
			open = false;
			currentPage.close();
			writePageFactory.close();
		}
		
	}

}
