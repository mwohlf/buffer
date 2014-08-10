package buffer;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ReadProcess {

	public static void main(String[] args) throws Exception {

		final String cachedir = "/home/michael/cache";
		//final String cachedir = "/tmp/cache";
		final int pagesize = 500 * 1024 * 1024;
		final int maxPageCount = 5;
		final int chuckSize =  1024 * 8;
		final long chunkCount = 10 * 1024 * 1024;

		// set to -1 to loop forever,
		// make sure to set a max page value otherwise your disk will fill up
		//final long totalDataCount = -1; // chunkCount * chuckSize;
		final long totalDataCount = chunkCount * chuckSize;

		final Buffer buff = new Buffer();
		buff.setCacheDir(new File(cachedir));
		buff.setPageSize(pagesize);

		// check if the filesys has enough space left...
		long needed = pagesize * maxPageCount;
		if (!new File(cachedir).exists()) {
			new File(cachedir).mkdirs();
		}
		long free = new File(cachedir).getFreeSpace();
		System.out.println("Filesystem check...");
		System.out.println(" free:   " + free);
		System.out.println(" needed: " + needed);
		if (free < needed) {
			System.out.println(" not enough free space for cache");
			return;
		}
		
		// create some buffer data
		int idx = 0;
		StringBuilder content = new StringBuilder();
		while (content.length() < chuckSize) {
			content.append("blabla");
			content.append(idx++);
		}
		// exact length
		String bufferData = content.substring(0, chuckSize);



		// run a reader
		final AtomicLong incomingAmount = new AtomicLong();
		final AtomicInteger incomingCount = new AtomicInteger();

		IReadStream readStream = buff.getReadStream(0);
		final long start = System.currentTimeMillis();
		while ((incomingAmount.longValue() < totalDataCount)
				|| (totalDataCount < 0)) {
			String result = str(readStream.read());
			if (!bufferData.equals(result)) {
				System.err.println("buffer corrupted!");
			}
						
			incomingAmount.addAndGet(result.length());
			incomingCount.getAndIncrement();

			// print stats for the reading side:
			if ((incomingCount.get() % 10_000) == 0) {
				double mByte = incomingAmount.get() / (double)(1024 * 1024);
				double mBps = mByte / (double) ((System.currentTimeMillis() - start) / 1000d);
				System.out.printf(" Speed: %.2f [MByte/sec] "
						+ "time: %d bytes: %d buffers: %d\n", mBps, System.currentTimeMillis(), incomingAmount.get(), incomingCount.get());
			}
		}

	}
	
	static String str(ByteBuffer buffer) {
		int size = buffer.limit() - buffer.position();
		byte[] bytes = new byte[size];
		buffer.get(bytes, 0, size);
		return new String(bytes);
	}
	
}
