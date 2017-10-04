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
            byte[] data = new byte[BufferPool.getPageSize()];
            int offset = pid.getPageNumber() * BufferPool.getPageSize();
            randAccessFile.seek(offset);
            randAccessFile.readFully(data);
            randAccessFile.close();
            return new HeapPage(heapPageID, data);
        }
        catch (IOException ex){
            return null; 
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
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
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        HeapFile heapFile = this;

        return new DbFileIterator(){
            private HeapPage currPage;
            private Iterator<Tuple> pageIterator;
            private boolean opened = false;

            private HeapPage getPage(int pageNo) throws TransactionAbortedException, DbException {
                 //build new page ID for given page in this file
                HeapPageId pageID = new HeapPageId(heapFile.getId(),pageNo);
                //get page from buffer
                Page fromBuffer = Database.getBufferPool().getPage(tid, pageID, Permissions.READ_ONLY);
                return (HeapPage)fromBuffer;
            }
            
            //open and grab first page, page iterator if file has any pages
            public void open() throws DbException, TransactionAbortedException {
                opened = true;
                if (heapFile.numPages() > 0){
                    currPage = getPage(0);
                    pageIterator = currPage.iterator();
                }
            }

            public boolean hasNext() throws DbException, TransactionAbortedException {
               
               if (!opened){
                    return false;
               }

               HeapPage tempPage = currPage;
               Iterator<Tuple> tempPageIter = pageIterator;

               //check if current page has another tuple and return true if it does
                while (!tempPageIter.hasNext()){
                    //otherwise check if this file has other pages
                    if ((tempPage.getId().getPageNumber() + 1) >= heapFile.numPages()){
                        return false;
                    }
                    else{
                        //if it does, then grab the next page and corresponding iterator
                        tempPage = getPage(tempPage.getId().getPageNumber() + 1);
                        tempPageIter = tempPage.iterator();
                    }
                }
                return true;
            }

            public void close(){
                opened = false;
            }

            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!opened){
                    throw new NoSuchElementException();
                }

                //check if current page has another tuple and return if so
                while (!pageIterator.hasNext()){
                    //otherwise check if file has another page - if not, return exception
                    if ((currPage.getId().getPageNumber() + 1) >= heapFile.numPages()){
                        throw new NoSuchElementException();
                    }
                    else{
                        //if there is another page, grab it and it's iterator
                        currPage = getPage(currPage.getId().getPageNumber() + 1); 
                        pageIterator = currPage.iterator();
                    }
                }
                return pageIterator.next();
            }

            //reset page and page iterator to first page of this file
            public void rewind() throws DbException, TransactionAbortedException{
                if (opened && heapFile.numPages() > 0){
                    currPage = getPage(0);
                    pageIterator = currPage.iterator();
                }
            }
        };
    }

}

