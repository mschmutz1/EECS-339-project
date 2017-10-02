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

    private File file;
    private TupleDesc td;

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
        return this.file.getAbsoluteFile().hashCode();
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

        HeapPageId heapPageID = new HeapPageId(pid.getTableId(), pid.getPageNumber());
        RandomAccessFile randAccessFile;

        try{
            randAccessFile = new RandomAccessFile(this.file,"r");
        }
        catch (FileNotFoundException ex){
            return null;
        }
        try{
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
            private Page currPage;
            private Iterator<Tuple> pageIterator;
            
            public void open(){
                // if (heapFile.numPages() > 0){
                    //get first page and iterator for that page
                    // currPage = BufferPool.getPage(tid,new HeapPageId(heapFile.getId(),0), Permissions.READ_ONLY);
                    // pageIterator = currPage.iterator();
                //}
            }
            public boolean hasNext(){
               // Page tempPage = currPage;
               // Iterator<Tuple> tempPageIter = pageIterator;
               // //iterate through pages until you find one with an element or you run out of pages
               //  while (!tempPageIter.hasNext()){
               //      if ((tempPage.getId().getPageNumber() + 1) >= heapFile.numPages()){
               //          return false;
               //      }
               //      else{
               //          tempPage = getPage(tempPage.getId().getPageNumber() + 1);
               //          tempPageIter = tempPage.iterator();
               //      }
               //  }
                return false;
            }

            public void close(){

            }

            public Tuple next(){
                //iterate through pages until you find one with an element or you run out of pages
                // while (!pageIterator.hasNext()){
                //     if ((currPage.getId().getPageNumber() + 1) >= heapFile.numPages()){
                //         return null;
                //     }
                //     else{
                //         currPage = getPage(currPage.getId().getPageNumber() + 1);
                //         pageIterator = currPage.iterator();
                //     }
                // }
                // return pageIterator.next();
                return null;
            }

            public void rewind(){

            }

            private Page getPage(int pageNo){
                //return BufferPool.getPage(tid,new HeapPageId(heapFile.getId(),pageNo), Permissions.READ_ONLY);
                return null;
            }
        };
    }

}

