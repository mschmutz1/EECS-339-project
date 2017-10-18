package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int groupFieldNum;
    private Type groupFieldType;
    private int aggregationFieldNum;
    private Op operator;
    private List<Tuple> aggFinal;
    private TupleDesc newTD = null;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Aggregator.Op.COUNT) throw new IllegalArgumentException();
        
        groupFieldNum = gbfield;
        groupFieldType = gbfieldtype;
        aggregationFieldNum = afield;
        operator = what;
        
            aggFinal = new ArrayList<Tuple>();
        
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        
        
        if (groupFieldNum != NO_GROUPING) {
                Field matchAgg = tup.getField(groupFieldNum);
                
                for (int i = 0; i<aggFinal.size(); i++) {
                    Tuple agg = aggFinal.get(i);
                    if (agg.getField(0).equals(matchAgg)) {
                        int currAggValue = ((IntField) agg.getField(1)).getValue();
                        agg.setField(1, new IntField(currAggValue + 1));
                        aggFinal.set(i, agg);
                        return;
                    }
                }
                
                if (newTD == null) {
                    Type[] typeAR = new Type[] {groupFieldType, Type.INT_TYPE};
                    String[] fieldAR = new String[] {tup.getTupleDesc().getFieldName(aggregationFieldNum), operator.toString()};
                    newTD = new TupleDesc(typeAR, fieldAR);
                }
                
                Tuple newTup = new Tuple(newTD);
                newTup.setField(0, matchAgg);
                newTup.setField(1, new IntField(1));
                aggFinal.add(newTup);
                
        }else {
                if (aggFinal.isEmpty()) {
                    newTD = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {operator.toString()});
                    Tuple newTup = new Tuple(newTD);
                    newTup.setField(0, new IntField(1));
                    aggFinal.add(newTup);
                    return;
                }
                
                Tuple agg = aggFinal.get(0);
                int currAggValue = ((IntField) agg.getField(0)).getValue();
                agg.setField(0, new IntField(currAggValue + 1));
                aggFinal.set(0, agg);

        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
            return new TupleIterator(newTD, aggFinal);
    }

}
