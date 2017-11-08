package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    public double[] ranges;
    public double[] counts;
    private double bucketRange;
    private int numTuples = 0;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	this.counts = new double[buckets];
        this.ranges = new double[buckets];
        this.bucketRange = ((double)(max - min))/((double)buckets);
        double bucketMin = min;
        for (int i = 0; i < buckets; i++){
            this.ranges[i] = bucketMin;
            bucketMin += this.bucketRange;
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        int i = 0;
    	while ((i < (this.ranges.length - 1)) && (v >= this.ranges[i+1])){
            i += 1;
        }
        this.counts[i] += 1;
        this.numTuples++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        int currVal = (int)this.ranges[0];
        IntField value = new IntField(v);
        double selectivity = 0;
        int bucket = 0;
        while (currVal <= (this.ranges[0] + this.ranges.length*this.bucketRange)){
            IntField currValField = new IntField(currVal);
            if (currValField.compare(op, value)){
                while ((bucket < (this.ranges.length - 1)) && (currVal >= this.ranges[bucket+1])){
                    bucket++;
                }
                selectivity += this.counts[bucket]/(this.bucketRange*this.numTuples);
            }
            currVal++;
        }
        return selectivity;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
