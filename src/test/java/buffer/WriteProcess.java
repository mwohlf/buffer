package buffer;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class WriteProcess {

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
		ByteBuffer bufferData = bb(content.substring(0, chuckSize));

		
		
		if (totalDataCount < 0) {
			System.out.println("Writing continuously...");
		} else {
			System.out.println("Writing " + totalDataCount + " Byte...");
		}

		final IWriteStream writeStream = buff.getWriteStream(0);

		final AtomicLong outgoingAmount = new AtomicLong();
		final AtomicInteger outgoingCount = new AtomicInteger();
		long start = System.currentTimeMillis();
		while ((outgoingAmount.longValue() < totalDataCount)
				|| (totalDataCount < 0)) {
			outgoingAmount.addAndGet(bufferData.remaining());
			outgoingCount.getAndIncrement();
			writeStream.write(System.currentTimeMillis(), bufferData);
			bufferData.position(0); // reset

			// print stats for the reading side:
			if ((outgoingCount.get() % 10_000) == 0) {
				double mByte = outgoingAmount.get() / (double)(1024 * 1024);
				double mBps = mByte / (double) ((System.currentTimeMillis() - start) / 1000d);
				System.out.printf("Write Speed: %.2f [MByte/sec] "
						+ "time: %d bytes: %d buffers: %d\n", mBps, System.currentTimeMillis(), outgoingAmount.get(), outgoingCount.get());
			}
		}
		System.out.println(" writer finished, time: " + (System.currentTimeMillis() - start) + "[ms]");
		System.out.println("    items written: " + outgoingCount.get());
		System.out.println("    data written: " + outgoingAmount.get());

		System.out.println("(please delete the page data from your filesystem)");

	}

	// offset = pos = 0; cap = limit = size;
	static ByteBuffer bb(String string) {
		ByteBuffer buffer = ByteBuffer.wrap(string.getBytes());
		buffer.rewind();
		return buffer;
	}

}
