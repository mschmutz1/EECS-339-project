package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from disk.
 * Access methods call into it to retrieve pages, and it fetches pages from the
 * appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches a
 * page, BufferPool checks that the transaction has the appropriate locks to
 * read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
	/** Bytes per page, including header. */
	private static final int DEFAULT_PAGE_SIZE = 4096;

	private static int pageSize = DEFAULT_PAGE_SIZE;

	/**
	 * Default number of pages passed to the constructor. This is used by other
	 * classes. BufferPool should use the numPages argument to the constructor
	 * instead.
	 */
	public static final int DEFAULT_PAGES = 50;


	/*Lock type enumeration*/
	public static final int NO_LOCK = 0;
	public static final int SHARED_LOCK = 1;
	public static final int EXCL_LOCK = 2;

	/* Cached pages */
	private static ConcurrentHashMap<PageId, Page> cached_pages;
	private Queue<PageId> lruStore;
	final int numPages;
	private static ConcurrentHashMap<TransactionId, ConcurrentHashMap<PageId, Integer>> pageLocks;

	/**
	 * Creates a BufferPool that caches up to numPages pages.
	 *
	 * @param numPages
	 *            maximum number of pages in this buffer pool.
	 */
	public BufferPool(int numPages) {
		this.cached_pages = new ConcurrentHashMap<PageId, Page>();
		this.lruStore = new LinkedList<PageId>();
		this.numPages = numPages;
		this.pageLocks = new ConcurrentHashMap<TransactionId,ConcurrentHashMap<PageId, Integer>>();
	}

	public static int getPageSize() {
		return pageSize;
	}

	// THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
	public static void setPageSize(int pageSize) {
		BufferPool.pageSize = pageSize;
	}

	// THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
	public static void resetPageSize() {
		BufferPool.pageSize = DEFAULT_PAGE_SIZE;
	}

	/**
	 * Retrieve the specified page with the associated permissions. Will acquire a
	 * lock and may block if that lock is held by another transaction.
	 * <p>
	 * The retrieved page should be looked up in the buffer pool. If it is present,
	 * it should be returned. If it is not present, it should be added to the buffer
	 * pool and returned. If there is insufficient space in the buffer pool, a page
	 * should be evicted and the new page should be added in its place.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the page
	 * @param pid
	 *            the ID of the requested page
	 * @param perm
	 *            the requested permissions on the page
	 */
	public Page getPage(TransactionId tid, PageId pid, Permissions perm)
			throws TransactionAbortedException, DbException, InterruptedException {

		Page p = this.cached_pages.get(pid);
		if (p == null) {
			if (this.cached_pages.size() >= numPages) {
				evictPage();
			}
			DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
			boolean canAccess = false;
			int newLock = NO_LOCK;
			int lock = NO_LOCK;
			Iterator<ConcurrentHashMap<PageId, Integer>> transIter = this.pageLocks.values().iterator();

			int count = 0;
			while(!canAccess){
				if (count > 0){
					System.out.print("     LOCKING UP      ");
					wait();
				}
				//check each transaction to see if it holds a lock for page
				while (transIter.hasNext() || lock != NO_LOCK){
					ConcurrentHashMap<PageId, Integer> transLocks = transIter.next();

					if (transLocks.get(pid) != null){
						lock = transLocks.get(pid);
					}
				}

				if (perm == Permissions.READ_ONLY){
					if (lock != EXCL_LOCK){
						canAccess = true;
						newLock = SHARED_LOCK;
					}
				}
				else if (perm == Permissions.READ_WRITE){
					System.out.print(" CHECKING-2 ");
					if (lock == NO_LOCK){
						canAccess = true;
						newLock = EXCL_LOCK;
					}
				}
				count += 1;
			}

			ConcurrentHashMap<PageId, Integer> myLocks = this.pageLocks.get(tid);
			if (myLocks == null){
				System.out.print(newLock);
				myLocks = new ConcurrentHashMap<PageId, Integer>();
			}
			myLocks.put(pid,newLock);
			this.pageLocks.put(tid,myLocks);

			p = file.readPage(pid);
			this.cached_pages.put(pid, p);

		}else {
			lruStore.remove(pid);
		}
		
		lruStore.add(pid);
		return p;

	}

	/**
	 * Releases the lock on a page. Calling this is very risky, and may result in
	 * wrong behavior. Think hard about who needs to call this and why, and why they
	 * can run the risk of calling it.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 * @param pid
	 *            the ID of the page to unlock
	 */
	public void releasePage(TransactionId tid, PageId pid) {
		ConcurrentHashMap<PageId, Integer> locks = this.pageLocks.get(tid);
		locks.remove(pid);
		this.pageLocks.put(tid,locks);
	}

	/**
	 * Release all locks associated with a given transaction.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 */
	public void transactionComplete(TransactionId tid) throws IOException {
		this.pageLocks.remove(tid);
	}

	/** Return true if the specified transaction has a lock on the specified page */
	public boolean holdsLock(TransactionId tid, PageId p) {
		ConcurrentHashMap<PageId, Integer> locks = this.pageLocks.get(tid);
		Integer tLock = locks.get(p);
		if (tLock != null){
			return true;
		}
		else{
			return false;
		}
	}

	/**
	 * Commit or abort a given transaction; release all locks associated to the
	 * transaction.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 * @param commit
	 *            a flag indicating whether we should commit or abort
	 */
	public void transactionComplete(TransactionId tid, boolean commit) throws IOException {
		// some code goes here
		// not necessary for lab1|lab2
	}

	/**
	 * Add a tuple to the specified table on behalf of transaction tid. Will acquire
	 * a write lock on the page the tuple is added to and any other pages that are
	 * updated (Lock acquisition is not needed for lab2). May block if the lock(s)
	 * cannot be acquired.
	 * 
	 * Marks any pages that were dirtied by the operation as dirty by calling their
	 * markDirty bit, and adds versions of any pages that have been dirtied to the
	 * cache (replacing any existing versions of those pages) so that future
	 * requests see up-to-date pages.
	 *
	 * @param tid
	 *            the transaction adding the tuple
	 * @param tableId
	 *            the table to add the tuple to
	 * @param t
	 *            the tuple to add
	 */
	public void insertTuple(TransactionId tid, int tableId, Tuple t)
			throws DbException, IOException, TransactionAbortedException {

		// grab file and insert tuple into file
		DbFile table = Database.getCatalog().getDatabaseFile(tableId);
		ArrayList<Page> updatedPages = table.insertTuple(tid, t);

		// mark any old versions of this page in cache as dirty, and replace them with
		// the updated page
		for (int i = 0; i < updatedPages.size(); i++) {
			PageId pid = updatedPages.get(i).getId();
			Page newPage = updatedPages.get(i);
			newPage.markDirty(true,tid);
			this.cached_pages.put(pid, newPage);
		}
	}

	/**
	 * Remove the specified tuple from the buffer pool. Will acquire a write lock on
	 * the page the tuple is removed from and any other pages that are updated. May
	 * block if the lock(s) cannot be acquired.
	 *
	 * Marks any pages that were dirtied by the operation as dirty by calling their
	 * markDirty bit, and adds versions of any pages that have been dirtied to the
	 * cache (replacing any existing versions of those pages) so that future
	 * requests see up-to-date pages.
	 *
	 * @param tid
	 *            the transaction deleting the tuple.
	 * @param t
	 *            the tuple to delete
	 */
	public void deleteTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {

		// grab file and insert tuple into file
		DbFile table = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
		ArrayList<Page> updatedPages = table.deleteTuple(tid, t);

		// mark any old versions of this page in cache as dirty, and replace them with
		// the updated page
		for (int i = 0; i < updatedPages.size(); i++) {
			PageId pid = updatedPages.get(i).getId();
			Page newPage = updatedPages.get(i);
			newPage.markDirty(true,tid);
			this.cached_pages.put(pid, newPage);
		}
	}

	/**
	 * Flush all dirty pages to disk. NB: Be careful using this routine -- it writes
	 * dirty data to disk so will break simpledb if running in NO STEAL mode.
	 */
	public synchronized void flushAllPages() throws IOException {
		Iterator<PageId> pagesIter = this.cached_pages.keySet().iterator();
		while (pagesIter.hasNext()) {
			flushPage(pagesIter.next());
		}
	}

	/**
	 * Remove the specific page id from the buffer pool. Needed by the recovery
	 * manager to ensure that the buffer pool doesn't keep a rolled back page in its
	 * cache.
	 * 
	 * Also used by B+ tree files to ensure that deleted pages are removed from the
	 * cache so they can be reused safely
	 */
	public synchronized void discardPage(PageId pid) {
		this.cached_pages.remove(pid);
		lruStore.remove(pid);
	}

	/**
	 * Flushes a certain page to disk
	 * 
	 * @param pid
	 *            an ID indicating the page to flush
	 */
	private synchronized void flushPage(PageId pid) throws IOException {
		Page page = this.cached_pages.get(pid);
		if (page.isDirty() != null) {
			DbFile table = Database.getCatalog().getDatabaseFile(pid.getTableId());
			table.writePage(page);
			page.markDirty(false, null);
		}
	}

	/**
	 * Write all pages of the specified transaction to disk.
	 */
	public synchronized void flushPages(TransactionId tid) throws IOException {
		Iterator<PageId> pagesIter = this.cached_pages.keySet().iterator();
		while (pagesIter.hasNext()) {
			PageId curr = pagesIter.next();
			TransactionId currTid = this.cached_pages.get(curr).isDirty();
			if (currTid != null && currTid == tid) {
				flushPage(pagesIter.next());
			}	
		}
	}

	/**
	 * Discards a page from the buffer pool. Flushes the page to disk to ensure
	 * dirty pages are updated on disk.
	 */
	private synchronized void evictPage() throws DbException {
		try {
			PageId eviction = lruStore.peek();
			flushPage(eviction);
			discardPage(eviction);
		}catch (IOException e) {
			
		}
		
	}

}
