package buffer;

import java.io.File;
import java.io.FilenameFilter;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class WritePageFactory {

	// 50MByte page size is probably the best choice
	private static final int DEFAULT_FILE_SIZE = (1024 * 1024 * 50);
	// private static final String TIMESTAMP_FORMAT = "yyyy.MM.dd-HH:mm:ss-SSS-z"; // use UTC
	private static final String TIMESTAMP_FORMAT = "yyyy.MM.dd-HH:mm:ss";
	static final String PAGEFILE_POSTFIX = ".page";
	private static final String FILENAME_TMP_POSTFIX = ".tmp";
	//private static final DateTimeFormatter DATE_FORMAT = DateTimeFormat.forPattern(TIMESTAMP_FORMAT).withZoneUTC();
	private static final DateTimeFormatter DATE_FORMAT = DateTimeFormat.forPattern(TIMESTAMP_FORMAT);

	
	private File cacheDir;

	private int filesize = DEFAULT_FILE_SIZE;

	private long currentPageIndex = -1;
    
	
	public void setCacheDir(File cacheDir) {
		this.cacheDir = cacheDir;
	}

	public void setPageSize(int size) {
		this.filesize = size;
	}

	private String filename(long timestamp, long index) {
		return DATE_FORMAT.print(timestamp)
				+ "-" + String.format("%02d", currentPageIndex)
				+ PAGEFILE_POSTFIX;
	}
	
	private String tempFilename(long timestamp, long index) {
		return DATE_FORMAT.print(timestamp)
				+ "-" + String.format("%02d", currentPageIndex)
				+ FILENAME_TMP_POSTFIX;
	}


	public void initialize() {
		if (!cacheDir.exists()) {
			throw new CacheException("cache dir does not exist: '" + cacheDir + "'");
		}
		if (!cacheDir.isDirectory()) {
			throw new CacheException("cache dir is not a directory: '" + cacheDir + "'");
		}
		
		
		final File[] files = cacheDir.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(PAGEFILE_POSTFIX);
			}
			
		});
		if (files == null) {
			throw new CacheException("IO Error opening cache directory, listFiles returns null "
					+ " cacheDir is configured to '" + cacheDir + "'");
		}
		currentPageIndex = 1;
		for (File file : files) {
			currentPageIndex = Math.max(currentPageIndex, new ReadPage(file).getIndex());
		}
	}
	
	// returns an already opened write page that is visible to readers
	public WritePage create(long timestamp) {
		assert currentPageIndex > 0: "page index not initialized";
		currentPageIndex++;
		final File tmpfile = new File(cacheDir, tempFilename(timestamp, currentPageIndex));
		final File file = new File(cacheDir, filename(timestamp, currentPageIndex));
		return new WritePage(tmpfile, filesize, timestamp, currentPageIndex)
			.open()
			.atomicMove(file);
	}

	public void close() {
		// TODO Auto-generated method stub
		
	}
	
}
