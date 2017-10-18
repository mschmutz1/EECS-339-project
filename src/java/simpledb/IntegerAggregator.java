package simpledb;

import java.util.*;
/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int groupFieldNum;
    private Type groupFieldType;
    private int aggregationFieldNum;
    private Op operator;
    private List<Tuple> aggFinal;
    private List<Integer> counts;
    private List<Integer> sums;
    public TupleDesc newTD = null;
    
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        groupFieldNum = gbfield;
        groupFieldType = gbfieldtype;
        aggregationFieldNum = afield;
        operator = what;
        
            aggFinal = new ArrayList<Tuple>();
            counts = new ArrayList<Integer>();
        
            if (operator == Aggregator.Op.AVG) {
                sums = new ArrayList<Integer>();
            }
        

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {    
        if (groupFieldNum != NO_GROUPING) {
                Field matchAgg = tup.getField(groupFieldNum);

                for (int i = 0; i<aggFinal.size(); i++) {
                    if (aggFinal.get(i).getField(0).equals(matchAgg)) {
                        aggFinal.set(i, updateAgg(aggFinal.get(i),tup, i, 1));
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
                
                if (operator == Aggregator.Op.COUNT) {
                    newTup.setField(1, new IntField(1));
                }else if (operator == Aggregator.Op.AVG){
                    sums.add(((IntField) tup.getField(aggregationFieldNum)).getValue());
                    newTup.setField(1, tup.getField(aggregationFieldNum));
                }else {
                    newTup.setField(1, tup.getField(aggregationFieldNum));
                }

                aggFinal.add(newTup);
                counts.add(1);
                
        }else {
                if (aggFinal.isEmpty()) {
                    newTD = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {operator.toString()});
                    Tuple newTup = new Tuple(newTD);
                    
                    if (operator == Aggregator.Op.COUNT) {
                        newTup.setField(0, new IntField(1));
                    }else if (operator == Aggregator.Op.AVG){
                        sums.add(((IntField) tup.getField(aggregationFieldNum)).getValue());
                        newTup.setField(0, tup.getField(aggregationFieldNum));
                    }else {
                        newTup.setField(0, tup.getField(aggregationFieldNum));
                    }
                    
                aggFinal.add(newTup);
                    counts.add(1);
                    return;
                }else {
                    aggFinal.set(0, updateAgg(aggFinal.get(0), tup, 0, 0));
                }
    
                
        }
    }

    private Tuple updateAgg(Tuple agg, Tuple newAdd, int countIndex, int aggUpdateIndex) {
            int currAggValue = ((IntField) agg.getField(aggUpdateIndex)).getValue();
            int newAddValue = ((IntField) newAdd.getField(aggregationFieldNum)).getValue();
            if (operator == Aggregator.Op.AVG) {
                int currSum = sums.get(countIndex) + newAddValue;
                sums.set(countIndex, currSum);
                counts.set(countIndex, counts.get(countIndex) + 1);
                agg.setField(aggUpdateIndex, new IntField(currSum/counts.get(countIndex)));
            }else if (operator == Aggregator.Op.MAX) {
                if (newAddValue > currAggValue) {
                    agg.setField(aggUpdateIndex, new IntField(newAddValue));
                }
            }else if (operator == Aggregator.Op.MIN) {
                if (newAddValue < currAggValue) {
                    agg.setField(aggUpdateIndex, new IntField(newAddValue));
                }
            }else if (operator == Aggregator.Op.SUM) {
                agg.setField(aggUpdateIndex, new IntField(currAggValue + newAddValue));
            }else {
                agg.setField(aggUpdateIndex, new IntField(currAggValue + 1));
            }
            
            return agg;
    }
    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        return new TupleIterator(newTD, aggFinal);
    }

}

