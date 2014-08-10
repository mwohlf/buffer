package buffer;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;
import it.unimi.dsi.fastutil.longs.Long2ObjectAVLTreeMap;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ReadPageFactory {

	private File cacheDir;

	private PageIndexWatcher pageIndexWatcher;

	ReentrantLock lock = new ReentrantLock();
	Condition morePages = lock.newCondition();
	private final Long2ObjectAVLTreeMap<ReadPage> pageCache = new Long2ObjectAVLTreeMap<>();


	public void setCacheDir(File cacheDir) {
		this.cacheDir = cacheDir;
	}

	public ReadPage findPageBefore(long timestamp) {
		lock.lock();
		try {			
			while (pageCache.size() == 0) {
				System.out.println("before morePages.await()");
				morePages.await();   // deadlock here!
				System.out.println("after morePages.await()");
			}			
			long start = pageCache.firstLongKey();
			if (timestamp < pageCache.get(start).getTimestamp()) {
				throw new CacheException("timestamp no longer available");
			}
			long end = pageCache.lastLongKey();
			if (pageCache.get(end).getTimestamp() < timestamp) {
				return getNextPage(pageCache.get(end)).open(); // blocks till the page is available
			}				
			// walk backwards till we get a smaller timestamp
			for (long index = end; index > start; index--) {
				if (pageCache.get(index).getTimestamp() < timestamp) {
					return pageCache.get(index).open();
				}
			}
			// if the first timestamp is equal to the timestamp we use it 
			if (pageCache.get(start).getTimestamp() == timestamp) {
				return pageCache.get(start).open();				
			}
		} catch (InterruptedException ex) {
			throw new CacheException(ex);
		} finally {
			lock.unlock();
		}
		throw new CacheException("timestamp not found");
	}

	public ReadPage getNextPage(ReadPage lastPage) {
		lock.lock();
		try {	
			long lastIndex = lastPage.getIndex();
			if (lastIndex < pageCache.firstLongKey()) {
				throw new CacheException("last page is too old, can't find next page since it was already removed");
			}
			if (lastIndex > pageCache.lastLongKey()) {
				throw new CacheException("page is out of order");
			}

			while (lastIndex == pageCache.lastLongKey()) {
				morePages.await();
			}
			return pageCache.get(lastIndex + 1); 
		} catch (InterruptedException ex) {
			throw new CacheException(ex);
		} finally {
			lock.unlock();
		}
	}
	
	public void deletePage(ReadPage page) {
		unregister(page);
		page.delete();
	}

	public void initialize() {
		if (!cacheDir.exists()) {
			throw new CacheException("cache dir does not exist: '" + cacheDir + "'");
		}
		if (!cacheDir.isDirectory()) {
			throw new CacheException("cache dir is not a directory: '" + cacheDir + "'");
		}

		try {
			WatchService watcher = FileSystems.getDefault().newWatchService();
			cacheDir.toPath().register(watcher, ENTRY_CREATE, ENTRY_DELETE);
			pageIndexWatcher = new PageIndexWatcher(watcher);
			pageIndexWatcher.start();
		} catch (IOException ex) {
			throw new CacheException("error registering watcher for: '" + cacheDir + "'");
		}

		final File[] files = cacheDir.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(WritePageFactory.PAGEFILE_POSTFIX);
			}

		});
		if (files == null) {
			throw new CacheException("IO Error opening cache directory, listFiles returns null "
					+ " cacheDir is configured to '" + cacheDir + "'");
		}
		for (File file : files) {
			register(new ReadPage(file));
		}
	}

	public void close() {
		pageIndexWatcher.terminate();
	}


	private void register(ReadPage readPage) {
		lock.lock();
		try {	
			pageCache.put(readPage.getIndex(), readPage);
			morePages.signal();
		} finally {
			lock.unlock();
		}
	}

	private void unregister(ReadPage readPage) {
		lock.lock();
		try {	
			pageCache.remove(readPage.getIndex());
		} finally {
			lock.unlock();
		}	
	}

	private class PageIndexWatcher extends Thread {

		private WatchService watcher;

		private volatile boolean stop = false;


		PageIndexWatcher(WatchService watcher) {
			this.watcher = watcher;
			this.stop = false;
			this.setName("CachePageWatchdog");
		}

		public void terminate() {
			stop = true;
		}

		@Override
		public void run() {
			try {
				while (!stop) {
					listen();
				}
			} catch (InterruptedException ex) {
				ex.printStackTrace();
			}		
		}

		@SuppressWarnings("unchecked")
		private void listen() throws InterruptedException {
			WatchKey watchKey = watcher.poll(5L, TimeUnit.SECONDS);
			if (stop || watchKey == null) {
				return;
			}
			for (WatchEvent<?> event : watchKey.pollEvents()) {
				if (stop || event.kind() == OVERFLOW) {
					continue;
				}
				File file = new File(cacheDir, ((WatchEvent<Path>)event).context().toFile().getName());
				if (event.kind() == ENTRY_CREATE) {
					if (file.getName().endsWith(WritePageFactory.PAGEFILE_POSTFIX)) {
						register(new ReadPage(file));
					} 
					continue;
				}
			}
			// reset the key
			boolean valid = watchKey.reset();
			if (!valid) {
				throw new CacheException("watch key became invalid");
			}
		}

	}

}
