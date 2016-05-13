package TaoProxy;

public class SubtreeBucket extends Bucket {
    // Left and right child buckets
    private SubtreeBucket mLeft;
    private SubtreeBucket mRight;

    /**
     * @brief Default constructor
     */
    public SubtreeBucket() {
        super();
        mLeft = null;
        mRight = null;
    }

    /**
     * @brief Copy constructor
     * @param bucket
     */
    public SubtreeBucket(Bucket bucket) {
        super(bucket);
        mLeft = null;
        mRight = null;
    }

    /**
     * @brief Mutator method to set right child if it does not exist
     * @param bucket
     */
    public void initializeRight(Bucket bucket) {
        if (mRight == null) {
            System.out.println("Going right in subtree bucket");
            if (bucket != null) {
                System.out.println("Somehow the bucket isn't null");
                mRight = new SubtreeBucket(bucket);
            } else {
                System.out.println("the bucket is null");
                mRight = new SubtreeBucket();
            }
        }
    }

    /**
     * @brief Mutator method to set left child if it does not exist
     * @param bucket
     */
    public void initializeLeft(Bucket bucket) {
        if (mLeft == null) {
            System.out.println("Going left in subtree bucket");
            if (bucket != null) {
                System.out.println("Somehow the bucket isn't null");
                mLeft = new SubtreeBucket(bucket);
            } else {
                System.out.println("the bucket is null");
                mLeft = new SubtreeBucket();
            }
        }
    }

    public void setRight(Bucket b) {
        if (b == null) {
            mRight = null;
        } else {
            mRight = new SubtreeBucket(b);
        }
    }

    public void setLeft(Bucket b) {
        if (b == null) {
            mLeft = null;
        } else {
            mLeft = new SubtreeBucket(b);
        }
    }

    /**
     * @brief Accessor method to get the right child of this bucket
     * @return mRight
     */
    public SubtreeBucket getRight() {
        return mRight;
    }

    /**
     * @brief Accessor method to get the left child of this bucket
     * @return mLeft
     */
    public SubtreeBucket getLeft() {
        return mLeft;
    }
}