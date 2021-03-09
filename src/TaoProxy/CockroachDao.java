package TaoProxy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Random;
import org.postgresql.ds.PGSimpleDataSource;

import com.google.common.primitives.Longs;

import Configuration.TaoConfigs;
import Configuration.Utility;

/**
 * Data access object that provides an interface for CockroachTaoProxy to access CockroachDB.
 * Abstraction over some common CockroachDB operations, including:
 *
 * - Auto-handling transaction retries in the 'runSQL' method
 *
 * - Example of bulk inserts in the 'bulkInsertRandomAccountData'
 *   method
 */

class CockroachDao {

	// singleton instance
	private static CockroachDao instance;

	private static final int MAX_RETRY_COUNT = 3;
	private static final String RETRY_SQL_STATE = "40001";

	private Connection connection;

	private final Random rand = new Random();

	CockroachDao(int port) {
		PGSimpleDataSource ds = new PGSimpleDataSource();
		ds.setServerNames(new String[] { "localhost" });
		ds.setPortNumbers(new int[] { port });
		ds.setDatabaseName("taostore");
		ds.setUser("seif"); ds.setPassword("seif"); ds.setSsl(true);
		ds.setSslMode("require");
		ds.setReWriteBatchedInserts(true); // add `rewriteBatchedInserts=true` to pg connection string
		ds.setApplicationName("taostore");
		try {
			this.connection = ds.getConnection();
		} catch (SQLException e) {
			System.out.printf("CockroachDao Constructor ERROR: { state => %s, cause => %s, message => %s }\n",
					e.getSQLState(), e.getCause(), e.getMessage());
		}
		createBuckets();
	}

	// thread-safe double-locking singleton accessor
	public static CockroachDao getInstance(int port) {
		if (instance == null) {
			synchronized (CockroachDao.class) {
				if (instance == null) {
					instance = new CockroachDao(port);
				}
			}
		}
		return instance;
	}

	/**
	 * Run SQL code in a way that automatically handles the
	 * transaction retry logic so we don't have to duplicate it in
	 * various places.
	 *
	 * @return Integer Number of rows updated, or -1 if an error is thrown.
	 */
	public Integer runSQLUpdate(PreparedStatement pstmt) {
		int rv = 0;

		try {
			// We're managing the commit lifecycle ourselves so we can
			// automatically issue transaction retries.
			connection.setAutoCommit(false);

			int retryCount = 0;

			while (retryCount <= MAX_RETRY_COUNT) {
				if (retryCount == MAX_RETRY_COUNT) {
					String err = String.format("hit max of %s retries, aborting", MAX_RETRY_COUNT);
					throw new RuntimeException(err);
				}

				try {
					rv += pstmt.executeUpdate();
					connection.commit();
					break;
				} catch (SQLException e) {
					if (RETRY_SQL_STATE.equals(e.getSQLState())) {
						// Since this is a transaction retry error, we
						// roll back the transaction and sleep a
						// little before trying again. Each time
						// through the loop we sleep for a little
						// longer than the last time
						// (A.K.A. exponential backoff).
						System.out.printf(
								"retryable exception occurred:\n    sql state = [%s]\n    message = [%s]\n    retry counter = %s\n",
								e.getSQLState(), e.getMessage(), retryCount);
						connection.rollback();
						retryCount++;
						int sleepMillis = (int) (Math.pow(2, retryCount) * 100) + rand.nextInt(100);
						System.out.printf("Hit 40001 transaction retry error, sleeping %s milliseconds\n", sleepMillis);
						try {
							Thread.sleep(sleepMillis);
						} catch (InterruptedException ignored) {
							// Necessary to allow the Thread.sleep()
							// above so the retry loop can continue.
						}
						rv = -1;
					} else {
						rv = -1;
						throw e;
					}
				}
			}
		} catch (SQLException e) {
			System.out.printf("CockroachDao.runSQLUpdate ERROR: { state => %s, cause => %s, message => %s }\n",
					e.getSQLState(), e.getCause(), e.getMessage());
			rv = -1;
		}

		return rv;
	}

	/**
	 * @param sqlCode a String containing the SQL code you want to
	 * execute.  Can have placeholders, e.g., "INSERT INTO accounts
	 * (id, balance) VALUES (?, ?)".
	 *
	 * @param args String Varargs to fill in the SQL code's
	 * placeholders.
	 */
	public Integer runSQLUpdate(String sqlCode, String... args) {
		int rv = -1;
		try {
			try (PreparedStatement pstmt = connection.prepareStatement(sqlCode)) {
				// Loop over the args and insert them into the
				// prepared statement based on their types. In
				// this simple example we classify the argument
				// types as "integers" and "everything else"
				// (a.k.a. strings).
				for (int i = 0; i < args.length; i++) {
					int place = i + 1;
					String arg = args[i];
					try {
						int val = Integer.parseInt(arg);
						pstmt.setInt(place, val);
					} catch (NumberFormatException e) {
						pstmt.setString(place, arg);
					}
				}
				rv = runSQLUpdate(pstmt);
			}
		} catch (SQLException e) {
			System.out.printf("CockroachDao.runSQLUpdate ERROR: { state => %s, cause => %s, message => %s }\n",
					e.getSQLState(), e.getCause(), e.getMessage());
		}
		return rv;
	}

	/**
	 * @param pathID
	 * @return Array of bucket IDs for a path
	 * Unique path keys assigned as follows:
	 * 	      0
	 *    1       2
	 *  3   4   5   6
	 */
	private long[] bucketIDsFromPID(long pathID) {
		long[] bucketIDs = new long[TaoConfigs.TREE_HEIGHT + 1];
		boolean[] pathDirection = Utility.getPathFromPID(pathID, TaoConfigs.TREE_HEIGHT);

		bucketIDs[0] = 0;
		for (int i = 1; i < TaoConfigs.TREE_HEIGHT + 1; ++i) {
			final boolean right = pathDirection[i - 1];
			if (right) {
				bucketIDs[i] = (bucketIDs[i - 1] + 1) << 1;
			} else {
				bucketIDs[i] = (bucketIDs[i - 1] << 1) + 1;
			}
		}
		return bucketIDs;
	}

	/**
	 * Creates a fresh, empty accounts table in the database.
	 */
	public void createBuckets() {
		runSQLUpdate("CREATE TABLE IF NOT EXISTS buckets (id INT PRIMARY KEY, bucket BYTES)");
	};

	public byte[] readBucket(long bucketID) {
		try {
			ResultSet res = connection.createStatement()
					.executeQuery("SELECT bucket FROM buckets WHERE id = " + bucketID);
			if (!res.next()) {
				System.out.printf("No buckets in the table with id %d", bucketID);
			} else {
				return res.getBytes("bucket");
			}
		} catch (SQLException e) {
			System.out.printf("CockroachDao.readBucket ERROR: { state => %s, cause => %s, message => %s }\n",
					e.getSQLState(), e.getCause(), e.getMessage());
		}
		return null;
	}

	/**
	 * @param id
	 * @param bucket
	 * @return 1 if bucket is written, -1 if error
	 */
	private Integer writeBucket(long bucketID, byte[] bucket) {
		int rv = -1;
		String statement = "UPSERT INTO buckets (id, bucket) VALUES (?, ?)";
		try {
			try (PreparedStatement pstmt = connection.prepareStatement(statement)) {
				pstmt.setLong(1, bucketID);
				pstmt.setBytes(2, bucket);
				rv = runSQLUpdate(pstmt);
			}
		} catch (SQLException e) {
			System.out.printf("CockroachDao.writeBucket ERROR: { state => %s, cause => %s, message => %s }\n",
					e.getSQLState(), e.getCause(), e.getMessage());
		}
		return rv;
	}

	/**
	 * Reads a path from the database
	 * @param pathID
	 * @return path data as a byte array
	 */
	public byte[] readPath(long pathID) {
		System.out.println("Reading path: " + pathID);
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		try {
			outputStream.write(Longs.toByteArray(pathID));
			for (long bucketKey : bucketIDsFromPID(pathID)) {
				byte[] bucket = readBucket(bucketKey);
				outputStream.write(bucket);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return outputStream.toByteArray();
	}

	/**
	 * Writes a path from data into the database
	 * @param pathID
	 * @param data
	 * @return success
	 */
	public boolean writePath(long pathID, byte[] data) {
		System.out.println("Writing path: " + pathID);
		final int bucketSize = (int) TaoConfigs.ENCRYPTED_BUCKET_SIZE;
		final int numBuckets = data.length / bucketSize;

		assert numBuckets == TaoConfigs.TREE_HEIGHT;

		int successfulWrites = 0;

		// Index into the data byte array
		// Skip the first 8 bytes which hold the pathID
		int dataIndexStart = 8;
		for (long bucketKey : bucketIDsFromPID(pathID)) {
			// Get the data for the current bucket to be written
			byte[] dataToWrite = Arrays.copyOfRange(data, dataIndexStart, dataIndexStart + bucketSize);
			successfulWrites += writeBucket(bucketKey, dataToWrite);
			dataIndexStart += bucketSize;
		}
		return successfulWrites == TaoConfigs.TREE_HEIGHT;
	}

	/**
	 * Perform any necessary cleanup of the data store so it can be
	 * used again.
	 */
	public void tearDown() {
		runSQLUpdate("DROP TABLE buckets;");
		try {
			this.connection.close();
		} catch (SQLException e) {
			System.out.printf("CockroachDao.tearDown ERROR: { state => %s, cause => %s, message => %s }\n",
					e.getSQLState(), e.getCause(), e.getMessage());
		}
	}
}