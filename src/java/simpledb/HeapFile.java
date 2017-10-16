package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc td;
    private final int id;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.td = td;
        this.id = f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return this.id;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {

        if (pid.getTableId() != this.getId()){
            throw new IllegalArgumentException();
        }

        //create new Heap Page ID for construction new Heap Page
        HeapPageId heapPageID = new HeapPageId(pid.getTableId(), pid.getPageNumber());
        RandomAccessFile randAccessFile;

        try{
            //open random access connection to our file
            randAccessFile = new RandomAccessFile(this.file,"r");
        }
        catch (FileNotFoundException ex){
            return null;
        }
        try{
            //create array of page size to store data and read page starting at correct offset
            int offset = pid.getPageNumber() * BufferPool.getPageSize();
            int pageSize = BufferPool.getPageSize();
            // if (((int)this.file.length() - offset) < pageSize){
            //     pageSize = (int)this.file.length() - offset;
            // }
            byte[] data = new byte[pageSize];
            randAccessFile.seek(offset);
            randAccessFile.readFully(data);
            randAccessFile.close();
            return new HeapPage(heapPageID, data);
        }
        catch (IOException ex){
            System.out.print(" exception  ");
            return null; 
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        RandomAccessFile randAccessFile;
        try{
            //open random access connection to our file
            randAccessFile = new RandomAccessFile(this.file,"rw");
        }
        catch (FileNotFoundException ex){
            throw new IOException();
        }
        //find offset corresponding to page number, and write page accordingly
        byte[] pageData = page.getPageData();
        int offset = page.getId().getPageNumber() * BufferPool.getPageSize();
        randAccessFile.seek(offset);
        randAccessFile.write(pageData);
        randAccessFile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int)Math.ceil(this.file.length()/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        int pageNo = 0;

        //iterate over each page and insert tuple if an empty slot exists
        while (pageNo < this.numPages()){
            HeapPageId pageID = new HeapPageId(this.getId(),pageNo);
            HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pageID, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() != 0){
                page.insertTuple(t);
                return new ArrayList<Page>(Arrays.asList(page));
            }
            pageNo++;
        }

        //if no page with empty slots exist, create new empty page, insert tuple and return
        byte[] emptyData = HeapPage.createEmptyPageData();
        HeapPageId pageID = new HeapPageId(this.getId(),this.numPages());
        HeapPage page = new HeapPage(pageID,emptyData);
        page.insertTuple(t);
        this.writePage(page);
        return new ArrayList<Page>(Arrays.asList(page));
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException, IOException {
        HeapPageId pageID = (HeapPageId)t.getRecordId().getPageId();
        if (pageID.getTableId() != this.getId()){
            throw new DbException("Tuple not present in file");
        }
        HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pageID, Permissions.READ_WRITE);
        if (page != null){
            page.deleteTuple(t);
            this.writePage(page);
            return new ArrayList<Page>(Arrays.asList(page));
        }
        throw new DbException("Tuple not present in file");
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        HeapFile heapFile = this;

        return new DbFileIterator(){
            private int pageNumber = 0;
            private Iterator<Tuple> pageIterator;
            private boolean opened = false;

            private Iterator<Tuple> getPageIterator(int pageNo) throws TransactionAbortedException, DbException {
                //build new page ID for given page in this file
                HeapPageId pageID = new HeapPageId(heapFile.getId(),pageNo);
                //get page from buffer
                HeapPage fromBuffer = (HeapPage)Database.getBufferPool().getPage(tid, pageID, Permissions.READ_ONLY);
                return fromBuffer.iterator();
            }
            
            //open and grab first page iterator if file has any pages
            public void open() throws DbException, TransactionAbortedException {
                opened = true;
                if (heapFile.numPages() > 0){
                    pageIterator = getPageIterator(0);
                }
            }

            public boolean hasNext() throws DbException, TransactionAbortedException {
               
               if (!opened){
                    return false;
               }

               int tempPageNumber = pageNumber;
               Iterator<Tuple> tempPageIter = pageIterator;

               //check if current page iterator has another tuple and return true if it does
                while (!tempPageIter.hasNext()){
                    //otherwise check if this file has other pages
                    if (++tempPageNumber >= heapFile.numPages()){
                        return false;
                    }
                    else{
                        //if it does, then grab the next page iterator
                        tempPageIter = getPageIterator(tempPageNumber);
                    }
                }
                return true;
            }

            public void close(){
                opened = false;
                pageNumber = 0;
                pageIterator = null;
            }

            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!opened){
                    throw new NoSuchElementException();
                }

                //check if current page has another tuple and return if so
                while (!pageIterator.hasNext()){
                    //otherwise check if file has another page - if not, return exception
                    if (++pageNumber >= heapFile.numPages()){
                        throw new NoSuchElementException();
                    }
                    else{
                        //if there is another page, grab it's iterator
                        pageIterator = getPageIterator(pageNumber); 
                    }
                }
                return pageIterator.next();
            }

            //reset page and page iterator to first page of this file
            public void rewind() throws DbException, TransactionAbortedException{
                if (opened && heapFile.numPages() > 0){
                    pageNumber = 0;
                    pageIterator = getPageIterator(pageNumber);
                }
            }
        };
    }

}

