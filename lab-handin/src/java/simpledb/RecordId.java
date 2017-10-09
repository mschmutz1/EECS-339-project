package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    private final PageId p_id;
    private final int tuple_no;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        this.p_id = pid;
        this.tuple_no = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        return this.tuple_no;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        return this.p_id;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        if ((o == null) || (!(o instanceof RecordId))){
            return false;
        }
        else{
            RecordId obj = (RecordId)o;

            if (this.p_id.equals(obj.getPageId()) && (obj.getTupleNumber() == this.tuple_no)){
                return true;
            }
            else{
                return false;
            }
        }
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        return Integer.parseInt(Integer.toString(this.p_id.hashCode()) + Integer.toString(this.tuple_no));
    }

}
