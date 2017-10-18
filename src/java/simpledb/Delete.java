package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;

    private TransactionId t;

    public static TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE});

    private boolean deleted; 

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        this.child = child;
        this.t = t;
        this.deleted = false;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {

        if (this.deleted){
            return null;
        }

        BufferPool buffer = Database.getBufferPool();
        int deletedCount = 0;
        while (this.child.hasNext()){
            Tuple next = this.child.next();
            try{
                buffer.deleteTuple(this.t,next);
                deletedCount++;
            }
            catch(IOException ex){
                return null;
            }
        }
        this.deleted = true;
        Tuple numDeleted = new Tuple(this.td);
        numDeleted.setField(0,new IntField(deletedCount));
        return numDeleted;
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
