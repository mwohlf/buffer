package buffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SinglePageTest {

	private File file;

	@Before
	public void prepareFilename() throws IOException {
		file = File.createTempFile(
				getClass().getCanonicalName(),
				String.valueOf(Thread.currentThread().getId()));
		file.delete(); // created when the cache is initialized
	}

	@After
	public void cleanup() {
		if (file != null) {
			try {
				file.delete();
			} catch (Exception ex) {
				// ignore
			}
		}
	}

	@Test
	public void smokeTest() throws IOException {
		WritePage write = new WritePage(file, PageMetadata.METADATA_SIZE + 70, 1, 1);
		write.open();
		write.write(bb("blablablabla23"));
		assertEquals(70 
				- PageMetadata.INT_SIZE               // size
				- "blablablabla23".getBytes().length  // content
				- PageMetadata.INT_SIZE               // next size
				- PageMetadata.INT_SIZE,              // potential EOF
				write.remainingForWrite());
		write.close();

		ReadPage read = new ReadPage(file);
		read.open();
		assertBufferEquals(bb("blablablabla23"), read.read());
		read.close();
	}


	@Test
	public void doubleWrite() throws IOException {
		WritePage write = new WritePage(file, PageMetadata.METADATA_SIZE + 70, 1, 1);
		write.open();
		write.write(bb("test1data"));
		assertEquals(70 
				- PageMetadata.INT_SIZE
				- "test1data".getBytes().length 
				- PageMetadata.INT_SIZE
				- PageMetadata.INT_SIZE, 
				write.remainingForWrite());
		write.write(bb("2"));
		assertEquals(70 
				- PageMetadata.INT_SIZE
				- "test1data".getBytes().length 
				- PageMetadata.INT_SIZE
				- "2".getBytes().length 
				- PageMetadata.INT_SIZE
				- PageMetadata.INT_SIZE,
				write.remainingForWrite());
		write.close();

		ReadPage read = new ReadPage(file);
		read.open();
		assertBufferEquals(bb("test1data"), read.read());
		assertBufferEquals(bb("2"), read.read());
		read.close();
	}

	@Test
	public void doubleRead() throws IOException {
		WritePage write = new WritePage(file, PageMetadata.METADATA_SIZE + 70, 1, 1);
		write.open();
		write.write(bb("testdatabla"));
		write.close();

		ReadPage read = new ReadPage(file);
		read.open();
		assertEquals("testdatabla", str(read.read()));
		read.close();

		// same data should be returned
		read = new ReadPage(file);
		read.open();
		assertEquals("testdatabla", str(read.read()));
		read.close();
	}


	@Test
	public void doubleOpenException() throws IOException {
		WritePage write = new WritePage(file, PageMetadata.METADATA_SIZE + 70, 1, 1);
		write.open();
		write.write(bb("testda2ta"));
		write.close();    // writes EOF

		// data should not be appended
		try {
			write = new WritePage(file, PageMetadata.METADATA_SIZE + 70, 1, 1);
			write.open();
			fail();
		} catch (CacheException ex) {

		}

		ReadPage read = new ReadPage(file);
		read.open();
		assertEquals("testda2ta", str(read.read()));
		read.close();
	}

	@Test
	public void splitWrite2() throws IOException {
		WritePage write = new WritePage(file, PageMetadata.METADATA_SIZE + 70, 1, 1);
		write.open();

		ByteBuffer param = bb("tesdatad");
		assertEquals(8, param.remaining());
		assertEquals(0, param.position());
		assertEquals(8, param.capacity());
		assertEquals(8, param.limit());
		write.write(param);
		assertEquals(0, param.remaining());
		assertEquals(8, param.position());
		assertEquals(8, param.capacity());
		assertEquals(8, param.limit());

		// rewrite already written buffer shouldn't do anything
		write.write(param);
		assertEquals(0, param.remaining());
		write.close();
	}

	@Test
	public void writeClosedExeption() {
		try {
			WritePage write1 = new WritePage(file, PageMetadata.METADATA_SIZE + 70, 1, 1);
			write1.open();
			write1.write(bb("testdata"));
			write1.close();

			write1.write(bb("bla"));
			fail();
		} catch (CacheException ex) {
			// ignore
		}
	}

	@Test
	public void sequentialReadWrite1() throws IOException {
		WritePage write = new WritePage(file, PageMetadata.METADATA_SIZE + 250, 1, 1);
		write.open();
		write.write(bb("hüzelbrützel"));

		ReadPage read = new ReadPage(file);
		read.open();
		assertEquals("hüzelbrützel", str(read.read()));
		read.close();

		write.write(bb("maultaschen"));
		write.close();   // this writes the EOF

		read = new ReadPage(file);
		read.open();
		assertEquals("hüzelbrützel", str(read.read()));
		assertEquals("maultaschen", str(read.read()));
		read.close();
	}

	@Test
	public void sequentialReadWrite2() throws IOException {

		WritePage write = new WritePage(file, PageMetadata.METADATA_SIZE + 250, 1, 1);
		write.open();
		write.write(bb("hüzelbrützel"));
		write.write(bb("maultaschen"));

		ReadPage read = new ReadPage(file);
		read.open();
		assertEquals("hüzelbrützel", str(read.read()));
		assertEquals("maultaschen", str(read.read()));
		read.close();

		read = new ReadPage(file);
		read.open();
		assertEquals("hüzelbrützel", str(read.read()));
		assertEquals("maultaschen", str(read.read()));
		read.close();

		write.write(bb("bretzelbrötchen"));
		write.close();

		read = new ReadPage(file);
		read.open();
		assertEquals("hüzelbrützel", str(read.read()));
		assertEquals("maultaschen", str(read.read()));
		assertEquals("bretzelbrötchen", str(read.read()));
		read.close();
	}


	@Test
	public void writeIntoFullPage1() throws IOException {
		String data = "12345678901234567890"; // 20 Byte

		// stuff in the file:
		// PageMetadata.METADATA_SIZE
		// + fore each entry: 4 Byte chunksize,
		// + 4 Byte EOF (once)
		WritePage write = new WritePage(file,
				PageMetadata.METADATA_SIZE + data.length(),
				1, 1);
		write.open();
		ByteBuffer param = bb(data);
		write.write(param);
		assertEquals(data.length(), param.remaining()); // nothing written
	}

	@Test
	public void writeIntoFullPage2() throws IOException {
		String data = "12345678901234567890"; // 20 Byte
		String string;

		// stuff in the file:
		// PageMetadata.METADATA_SIZE
		// + fore each entry: 4 Byte chunksize,
		// + 4 Byte EOF (once)
		WritePage write = new WritePage(file,
				PageMetadata.METADATA_SIZE + data.length(),
				1, 1);
		write.open();
		string = data.substring(1);
		ByteBuffer param = bb(string);
		write.write(param);
		assertEquals(string.length(), param.remaining());  // nothing written
	}    

	@Test
	public void writeIntoFullPage3() throws IOException {
		String data = "12345678901234567890"; // 20 Byte
		String string;

		// stuff in the file:
		// PageMetadata.METADATA_SIZE
		// + fore each entry: 4 Byte chunksize,
		// + 4 Byte EOF (once)
		WritePage write = new WritePage(file,
				PageMetadata.METADATA_SIZE + data.length(),
				1, 1);
		write.open();
		// we need 4 for the limit pointer
		string = data.substring(PageMetadata.INT_SIZE);
		ByteBuffer param = bb(string);
		write.write(param);
		assertEquals(string.length(), param.remaining());  // nothing written
	}
	
	@Test
	public void writeIntoFullPage4() throws IOException {
		String data = "12345678901234567890"; // 20 Byte
		String string;

		// stuff in the file:
		// PageMetadata.METADATA_SIZE
		// + fore each entry: 4 Byte chunksize,
		// + 4 Byte EOF (once)
		WritePage write = new WritePage(file,
				PageMetadata.METADATA_SIZE + data.length(),
				1, 1);
		write.open();
		// we need another 4 for the EOF marker
		string = data.substring(PageMetadata.INT_SIZE + PageMetadata.INT_SIZE);
		ByteBuffer param = bb(string);
		write.write(param);
		assertEquals(0, param.remaining());  // this should fit but we already wrote the EOF
	}

	@Test
	public void writeIntoFullPage7() throws IOException {
		String data = "12345678901234567890"; // 20 Byte
		String string;

		// stuff in the file:
		// PageMetadata.METADATA_SIZE
		// + fore each entry: 4 Byte chunksize,
		// + 4 Byte EOF (once)
		WritePage write = new WritePage(file,
				PageMetadata.METADATA_SIZE + data.length(),
				1, 1);
		write.open();
		string = data.substring(PageMetadata.INT_SIZE + PageMetadata.INT_SIZE);
		ByteBuffer param = bb(string);
		write.write(param);
		assertEquals(0, param.remaining());  // this should fit and no EOF this time

		// no more room to write
		String s = ".";
		param = bb(s);
		write.write(param);
		assertEquals(s.toCharArray().length, param.remaining());

		// check if we can read the stuff again
		ReadPage read = new ReadPage(file);
		read.open();
		assertBufferEquals(bb(string), read.read());

		// still no more room to write?
		String t = ".";
		param = bb(t);
		write.write(param);
		assertEquals(t.toCharArray().length, param.remaining());
		write.close();
	}

	@Test
	public void checkUnderflow() throws IOException {
		ByteBuffer param;

		WritePage write = new WritePage(file, PageMetadata.METADATA_SIZE + 20, 1, 1);
		write.open();

		write.write(param = bb("1"));
		assertEquals(0, param.remaining());  // 15 = 20 - (4+1)

		write.write(param = bb("2"));
		assertEquals(0, param.remaining());  // 10 = 15 - (4+1)

		write.write(param = bb("3"));
		assertEquals(0, param.remaining());  // 5 = 10 - (4+1)

		write.write(param = bb("4"));
		assertEquals(1, param.remaining());  // 5 left but we need 4 for the EOF --> full

		write.write(param = bb("5"));
		assertEquals(1, param.remaining());

	}

	@Test
	public void readEmpty() throws IOException, InterruptedException {
		WritePage write = new WritePage(file, PageMetadata.METADATA_SIZE + 70, 1, 1);
		write.open();

		ReadPage read = new ReadPage(file);
		read.open();
		assertEquals("", str(read.read()));
		assertEquals("", str(read.read()));

		read.close();
		write.close();
	}

	@Test
	public void doubleCreateExceptionRead() throws IOException, InterruptedException {
		new WritePage(file, PageMetadata.METADATA_SIZE + 70, 1, 1).open().atomicMove(file);

		ReadPage page = new ReadPage(file);
		page.open();
		try {
			page.open();
			fail("two write instances allowed");
		} catch (CacheException ex) {
			// expected
		}
	}

	@Test
	public void doubleCloseExceptionRead() throws IOException, InterruptedException {
		new WritePage(file, PageMetadata.METADATA_SIZE + 70, 1, 1).open().atomicMove(file);

		ReadPage page = new ReadPage(file);
		page.open();
		page.close();
		try {
			page.close();
			fail("two write instances closed");
		} catch (CacheException ex) {
			// expected
		}
	}

	@Test
	public void doubleCreateExceptionWrite() throws IOException, InterruptedException {
		WritePage page = new WritePage(file, 70, 1, 1);
		page.open();

		try {
			page.open();
			fail("two write instances allowed");
		} catch (CacheException ex) {
			// expected
		}
	}

	@Test
	public void doubleCloseExceptionWrite() throws IOException, InterruptedException {
		WritePage page = new WritePage(file, 70, 1, 1);
		page.open();
		page.close();
		try {
			page.close();
			fail("two write instances closed");
		} catch (CacheException ex) {
			// expected
		}
	}

	@Test
	public void concurrentReadWrite() throws IOException, InterruptedException {

		WritePage writePage = new WritePage(file, 1024, 1, 1);
		ReadPage readPage = new ReadPage(file);

		writePage.open();
		readPage.open();


		WriterThread writer = new WriterThread("abc:", 100, writePage,10);
		ReaderThread reader = new ReaderThread(readPage,10);

		writer.start();
		reader.start();
		writer.join();
		reader.writerrunning = false;

		String content = "";
		String incoming = "";

		ReadPage read = new ReadPage(file);
		read.open();
		do {
			incoming = str(read.read());
			content += incoming;
		} while (incoming.length() > 0); // writer finished we shouldn't have any trouble here


		String[] c = content.split(":");
		assertEquals(100, c.length);
		for (String s : c) {
			assertEquals("abc", s);
		}
		reader.join();

		assertEquals(content, reader.result.toString());
	}

	@Test
	public void concurrentLazyRead() throws IOException, InterruptedException {

		WritePage writePage = new WritePage(file, 1024, 1, 1);
		ReadPage readPage = new ReadPage(file);

		writePage.open();
		readPage.open();

		WriterThread writer = new WriterThread("abc:", 100, writePage,10);
		ReaderThread reader = new ReaderThread(readPage,100);

		writer.start();
		reader.start();
		writer.join();
		reader.writerrunning = false;
		reader.join();

		String content = "";
		String incoming = "";

		ReadPage read = new ReadPage(file);
		read.open();
		do {
			incoming = str(read.read());
			content += incoming;
		} while (incoming.length() > 0); // writer finished we shouldn't have any trouble here


		String[] c = content.split(":");
		assertEquals(100, c.length);
		for (String s : c) {
			assertEquals("abc", s);
		}

		assertEquals(content, reader.result.toString());
	}

	@Test
	public void concurrentLazyWrite() throws IOException, InterruptedException {

		WritePage writePage = new WritePage(file, 1024, 1, 1);
		ReadPage readPage = new ReadPage(file);

		writePage.open();
		readPage.open();


		WriterThread writer = new WriterThread("abc:", 100, writePage,100);
		ReaderThread reader = new ReaderThread(readPage,10);

		writer.start();
		reader.start();
		writer.join();
		reader.writerrunning = false;

		String content = "";
		String incoming = "";

		ReadPage read = new ReadPage(file);
		read.open();
		do {
			incoming = str(read.read());
			content += incoming;
		} while (incoming.length() > 0); // writer finished we shouldn't have any trouble here


		String[] c = content.split(":");
		assertEquals(100, c.length);
		for (String s : c) {
			assertEquals("abc", s);
		}
		reader.join();

		assertEquals(content, reader.result.toString());
	}

	static class ReaderThread extends Thread {
		private final Random random = new Random();
		private volatile ReadPage source;
		private volatile StringBuilder result = new StringBuilder();
		private volatile boolean writerrunning= true;
		private int lazyness;

		ReaderThread(ReadPage source, int lazyness) {
			this.source = source;
			this.lazyness = lazyness;
		}

		@Override
		public void run() {
			try {
				String incoming = "";
				do {
					sleep(Math.abs(random.nextInt())%lazyness);
					incoming = str(source.read());
					result.append(incoming);
				} while (incoming.length() > 0 || writerrunning);
			} catch (InterruptedException ex) {
				ex.printStackTrace();
			}
		}
	}

	static class WriterThread extends Thread {
		private final Random random = new Random();
		private final WritePage target;
		private final String payload;
		private final int interations;
		private int lazyness;

		WriterThread(String payload, int interations, WritePage target, int lazyness) {
			this.payload = payload;
			this.interations = interations;
			this.target = target;
			this.lazyness = lazyness;
		}

		@Override
		public void run() {
			try {
				for (int i = 0; i < interations; i++) {
					sleep(Math.abs(random.nextInt())%lazyness);
					target.write(bb(payload));
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

}
