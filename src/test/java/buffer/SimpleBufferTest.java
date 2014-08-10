package buffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import buffer.Buffer.ReadStream;

public class SimpleBufferTest {

	private File cacheDir;

	@Before
	public void prepareFilename() throws IOException {
		cacheDir = File.createTempFile(
				getClass().getCanonicalName(),
				String.valueOf(Thread.currentThread().getId()));
		cacheDir.delete(); // created when the cache is initialized
		cacheDir.mkdir();
	}

	@After
	public void cleanup() {
        if (cacheDir != null) {
            try {
                cacheDir.delete();
                File[] files = cacheDir.listFiles();
                if(files!=null) { //some JVMs return null for empty dirs
                    for(File f: files) {
                        f.delete();
                    }
                }
                cacheDir.delete();
            } catch (Exception ex) {
                // ignore
            }
        }
	}

    @Test
    public void smokeTest() throws IOException, InterruptedException {
        Buffer buffer = new Buffer();
        buffer.setCacheDir(cacheDir);
        
        IWriteStream writer = buffer.getWriteStream(0);
        writer.write(0, bb("test"));
        writer.close();
        
        IReadStream reader = buffer.getReadStream(0);
        ByteBuffer result = reader.read();
        
        assertBufferEquals(bb("test"), result);
        reader.close();
    }

    @Test
    public void twoBufferTest() throws IOException, InterruptedException {
        Buffer buffer = new Buffer();
        buffer.setCacheDir(cacheDir);
        
        IWriteStream writer = buffer.getWriteStream(0);
        writer.write(0, bb("test1"));
        writer.write(0, bb("test2"));
        writer.close();
        
        IReadStream reader = buffer.getReadStream(0);        
        assertBufferEquals(bb("test1"), reader.read());
        assertBufferEquals(bb("test2"), reader.read());
        reader.close();
    }

    @Test
    public void concurrentTest() throws IOException, InterruptedException {
    	int iter = 10;
    	
        Buffer buffer = new Buffer();
        buffer.setCacheDir(cacheDir);
        
        WriterThread writer = new WriterThread("abc:", iter, buffer, 10);
        ReaderThread reader = new ReaderThread(iter, buffer, 10);
        reader.start();
        writer.start();
        writer.join();
        reader.join();

        String[] c = reader.result.toString().split(":");
        assertEquals(iter, c.length);
        for (String s : c) {
            assertEquals("abc", s);
        }
    }
   
    @Test
    public void concurrentSlowWriterStartTest() throws IOException, InterruptedException {
    	int iter = 10;
    	
        Buffer buffer = new Buffer();
        buffer.setCacheDir(cacheDir);
        
        WriterThread writer = new WriterThread("abc:", iter, buffer, 10);
        ReaderThread reader = new ReaderThread(iter, buffer, 10);
        reader.start();
        Thread.sleep(20);
        writer.start();
        writer.join();
        reader.join();

        String[] c = reader.result.toString().split(":");
        assertEquals(iter, c.length);
        for (String s : c) {
            assertEquals("abc", s);
        }
    }
    
    @Test
    public void concurrentSlowReaderStartTest() throws IOException, InterruptedException {
    	int iter = 10;
    	
        Buffer buffer = new Buffer();
        buffer.setCacheDir(cacheDir);
        
        WriterThread writer = new WriterThread("abc:", iter, buffer, 10);
        ReaderThread reader = new ReaderThread(iter, buffer, 10);
        writer.start();
        Thread.sleep(20);
        reader.start();
        writer.join();
        reader.join();

        String[] c = reader.result.toString().split(":");
        assertEquals(iter, c.length);
        for (String s : c) {
            assertEquals("abc", s);
        }
    }
 
    @Test
    public void concurrentMultipageTest() throws IOException, InterruptedException {
    	int iter = 100;

        Buffer buffer = new Buffer();
        buffer.setCacheDir(cacheDir);
        buffer.setPageSize(70);
        
        WriterThread writer = new WriterThread("abcdefghijh:", iter, buffer, 10);
        ReaderThread reader = new ReaderThread(iter, buffer, 10);

        reader.start();

        writer.start();
        writer.join();
        reader.join();

        String[] c = reader.result.toString().split(":");
        assertEquals(iter, c.length);
        for (String s : c) {
            assertEquals("abcdefghijh", s);
        }
    }

	
	
	
    static class ReaderThread extends Thread {
        private final Random random = new Random();
        private volatile StringBuilder result = new StringBuilder();
        private volatile int interations;
        private volatile Buffer buffer;
		private int lazyness;

        ReaderThread(int interations, Buffer buffer, int lazyness) {
        	this.interations = interations;
            this.buffer = buffer;
            this.lazyness = lazyness;
            setName("BufferReaderThread");
        }

        @Override
        public void run() {
            try {
                String incoming = "";
                IReadStream reader = buffer.getReadStream(0);
                do {
                    sleep(Math.abs(random.nextInt())%lazyness);
                    incoming = str(reader.read());
                    result.append(incoming);
                    if (incoming.length() > 0) {
                    	interations--;
                    }
                } while (interations > 0);
                reader.close();
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            } catch (IOException e) {
				e.printStackTrace();
			}
        }
    }

    static class WriterThread extends Thread {
        private final Random random = new Random();
        private final String payload;
        private final int interations;
        private final Buffer buffer;
		private int lazyness;

        WriterThread(String payload, int interations, Buffer buffer, int lazyness) {
            this.payload = payload;
            this.interations = interations;
            this.buffer = buffer;
            this.lazyness = lazyness;
            setName("BufferWriterThread");
        }

        @Override
        public void run() {
            try {
            	IWriteStream writer = buffer.getWriteStream(0);
                for (int i = 0; i < interations; i++) {
                    sleep(Math.abs(random.nextInt())%lazyness);
                    writer.write(i, bb(payload));
                }
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }
    }
 
    // offset = pos = 0; cap = limit = size;
	static ByteBuffer bb(String string) {
		ByteBuffer buffer = ByteBuffer.wrap(string.getBytes());
		buffer.rewind();
		return buffer;
	}

	static String str(ByteBuffer buffer) {
		int size = buffer.limit() - buffer.position();
		byte[] bytes = new byte[size];
		buffer.get(bytes, 0, size);
		return new String(bytes);
	}

	void assertBufferEquals(ByteBuffer expected, ByteBuffer actual) {
		assertEquals("buffer position is different", expected.position(), actual.position());
		assertEquals("buffer limit is different", expected.limit(), actual.limit());
		assertEquals("buffer capacity is different", expected.capacity(), actual.capacity());

		byte[] expContent = new byte[expected.capacity()];
		byte[] actualContent = new byte[actual.capacity()];

		expected.get(expContent);
		actual.get(actualContent);

		assertArrayEquals(expContent, actualContent);
	}

	
	 public static void main(String[] args) throws Exception {

	        final String cachedir = "/home/michael/cache";
	        //final String cachedir = "/tmp/cache";
	        final int pagesize = 100 * 1024 * 1024;
	        final int maxPageCount = 5;
	        final int chuckSize =  1024;
	        final long chunkCount = 10 * 1024 * 1024;

	        // set to -1 to loop forever,
	        // make sure to set a max page value otherwise your disk will fill up
	        final long totalDataCount = -1; // chunkCount * chuckSize;
	        //final long totalDataCount =  chunkCount * chuckSize;

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

	        // run a reader
	        final AtomicLong incomingAmount = new AtomicLong();
	        final AtomicInteger incomingCount = new AtomicInteger();
	        Thread reader = new Thread() {
	            @Override
	            public void run() {
	                try {
	                	IReadStream readStream = buff.getReadStream(0);
	                    while ((incomingAmount.longValue() < totalDataCount)
	                            || (totalDataCount < 0)) {
	                        String result = str(readStream.read());
	                        incomingAmount.addAndGet(result.length());
	                        incomingCount.getAndIncrement();
	                    }
	                } catch (InterruptedException ex) {
	                    ex.printStackTrace();
	                }
	            }
	        };
	        reader.start();

	        if (totalDataCount < 0) {
	            System.out.println("Writing continuously...");
	        } else {
	            System.out.println("Writing " + totalDataCount + " Byte...");
	        }

	        final IWriteStream writeStream = buff.getWriteStream(0);

	        long start = System.currentTimeMillis();
	        final AtomicLong outgoingAmount = new AtomicLong();
	        final AtomicInteger outgoingCount = new AtomicInteger();
	        while ((outgoingAmount.longValue() < totalDataCount)
	                || (totalDataCount < 0)) {
	            outgoingAmount.addAndGet(bufferData.remaining());
	            outgoingCount.getAndIncrement();
	            writeStream.write(System.currentTimeMillis(), bufferData);
	            bufferData.position(0); // reset

	            if ((incomingCount.get() % 100_000) == 0) {
	                double mByte = incomingAmount.get() / (double)(1024 * 1024);
	                double mBps = mByte / (double) ((System.currentTimeMillis() - start) / 1000d);
	                System.out.printf(" Speed: %.2f [MByte/sec]\n", mBps);
	            }
	        }
	        System.out.println(" writer finished, time: " + (System.currentTimeMillis() - start) + "[ms]");
	        System.out.println("    items written: " + outgoingCount.get());
	        System.out.println("    data written: " + outgoingAmount.get());

	        reader.join();
	        System.out.println(" reader finished, time: " + (System.currentTimeMillis() - start) + "[ms]");
	        System.out.println("    items read: " + incomingCount.get());
	        System.out.println("    data read: " + incomingAmount.get());

	        System.out.println("(please delete the page data from your filesystem)");

	    }

}
