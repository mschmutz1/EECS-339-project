package simpledb;
import java.io.*;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;

    private int tableId;

    private TransactionId t;

    private boolean inserted;

    public static TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE});
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        this.child = child;
        this.tableId = tableId;
        this.t = t;
        this.inserted = false;
    }

    public TupleDesc getTupleDesc() {
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        this.child.open();
    }

    public void close() {
        super.close();
        this.child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
         this.child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        
        if (this.inserted){
            return null;
        }

        BufferPool buffer = Database.getBufferPool();
        int insertedCount = 0;
        while (this.child.hasNext()){
            Tuple next = this.child.next();
            try{
                buffer.insertTuple(this.t,this.tableId,next);
                insertedCount++;
            }
            catch(IOException ex){
                return null;
            }
        }
        this.inserted = true;
        Tuple numInserted = new Tuple(this.td);
        numInserted.setField(0,new IntField(insertedCount));
        return numInserted;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return null;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    }
}
