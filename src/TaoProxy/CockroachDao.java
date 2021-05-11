package TaoProxy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.Random;

import com.google.common.primitives.Longs;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

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

	private final Random rand = new Random();

	private HikariDataSource writeDS;
	private HikariDataSource readDS;

	private CryptoUtil mCryptoUtil;

	CockroachDao(CryptoUtil cryptoUtil) {
		mCryptoUtil = cryptoUtil;

		// expects a database named taostore
		String ip = TaoConfigs.PARTITION_SERVERS.get(0).getAddress().toString();
		ip = ip.substring(ip.indexOf('/') + 1);
		String url = "jdbc:postgresql://" + ip + ":" + TaoConfigs.SERVER_PORT + "/taostore?sslmode=disable";

		// connection pool for writing
		HikariConfig writeConfig = new HikariConfig();
		writeConfig.setJdbcUrl(url);
		writeConfig.setUsername("seif");
		writeConfig.setPassword("seif");
		writeConfig.addDataSourceProperty("reWriteBatchedInserts", "true");
		writeConfig.setAutoCommit(false);
		writeConfig.setMaximumPoolSize(128);
		writeConfig.setKeepaliveTime(150000);
		writeDS = new HikariDataSource(writeConfig);

		// connection pool for reading
		HikariConfig readConfig = new HikariConfig();
		readConfig.setJdbcUrl(url);
		readConfig.setUsername("seif");
		readConfig.setPassword("seif");
		readConfig.addDataSourceProperty("reWriteBatchedInserts", "true");
		readConfig.setAutoCommit(false);
		readConfig.setMaximumPoolSize(32);
		readConfig.setKeepaliveTime(150000);
		readConfig.setReadOnly(true);
		readDS = new HikariDataSource(readConfig);

		createBuckets();
	}

	// thread-safe double-locking singleton accessor
	public static CockroachDao getInstance(CryptoUtil cryptoUtil) {
		if (instance == null) {
			synchronized (CockroachDao.class) {
				if (instance == null) {
					instance = new CockroachDao(cryptoUtil);
				}
			}
		}
		return instance;
	}

	/**
	 * Run SQL code in a way that automatically handles the
	 * transaction retry logic so we don't have to duplicate it in
	 * various places.
	 * @param connection 
	 *
	 * @return Integer Number of rows updated, or -1 if an error is thrown.
	 */
	public Integer runSQLUpdate(Connection connection, PreparedStatement pstmt) {
		int rv = 0;

		try {
			// We're managing the commit lifecycle ourselves so we can
			// automatically issue transaction retries.

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
			try (Connection connection = writeDS.getConnection();
					PreparedStatement pstmt = connection.prepareStatement(sqlCode)) {
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
				rv = runSQLUpdate(connection, pstmt);
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
		runSQLUpdate("CREATE TABLE IF NOT EXISTS buckets (id INT PRIMARY KEY, bucket BYTES, timestamp INT)");
	};

	public byte[] readBucket(long bucketID, Connection connection) {
		try {
			ResultSet res = connection.createStatement()
					.executeQuery("SELECT bucket FROM buckets WHERE id = " + bucketID);
			connection.commit();
			if (!res.next()) {
				TaoLogger.logForce("No buckets in the table with id " + bucketID);
				System.exit(1);
			} else {
				return res.getBytes("bucket");
			}
		} catch (SQLException e) {
			System.out.printf("CockroachDao.readBucket ERROR: { state => %s, cause => %s, message => %s }\n",
					e.getSQLState(), e.getCause(), e.getMessage());
		}
		return null;
	}

	public byte[] readPath(long pathID) {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		try (Connection connection = readDS.getConnection()) {
			outputStream.write(Longs.toByteArray(pathID));
			for (long bucketKey : bucketIDsFromPID(pathID)) {
				byte[] bucket = readBucket(bucketKey, connection);
				outputStream.write(bucket);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			System.out.printf("CockroachDao.readPath ERROR: { state => %s, cause => %s, message => %s }\n",
					e.getSQLState(), e.getCause(), e.getMessage());
		}
		return outputStream.toByteArray();
	}

	public byte[] batchReadPath(long pathID) {
		int numBucketsRead = 0;

		String query = String.format("SELECT bucket FROM buckets WHERE id in (%s)",
				Arrays.stream(bucketIDsFromPID(pathID)).mapToObj(String::valueOf).collect(Collectors.joining(", ")));

		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		try (Connection connection = readDS.getConnection()) {
			ResultSet res = connection.createStatement().executeQuery(query);
			connection.commit();

			outputStream.write(Longs.toByteArray(pathID));
			while (res.next()) {
				outputStream.write(res.getBytes("bucket"));
				numBucketsRead++;
			}
		} catch (SQLException e) {
			System.out.printf("CockroachDao.readBucket ERROR: { state => %s, cause => %s, message => %s }\n",
					e.getSQLState(), e.getCause(), e.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
		}

		assert numBucketsRead == TaoConfigs.TREE_HEIGHT;
		return outputStream.toByteArray();
	}

	private Map<Long, byte[]> getUniqueBuckets(List<Path> paths) {
		// put everything into a map so that we only write each bucket once
		final int bucketSize = (int) TaoConfigs.ENCRYPTED_BUCKET_SIZE;
		Map<Long, byte[]> buckets = new HashMap<Long, byte[]>();
		for (Path path : paths) {
			byte[] encryptedPath = mCryptoUtil.encryptPath(path);
			// Index into the data byte array
			// Skip the first 8 bytes which hold the pathID
			int dataIndexStart = 8;
			for (long bucketID : bucketIDsFromPID(path.getPathID())) {
				// Get the data for the current bucket to be written
				if (!buckets.containsKey(bucketID)) {
					byte[] bucket = Arrays.copyOfRange(encryptedPath, dataIndexStart, dataIndexStart + bucketSize);
					buckets.put(bucketID, bucket);
				}
				dataIndexStart += bucketSize;
			}
		}
		return buckets;
	}

	/**
	 * Writes a path from data into the database
	 * uses a single connection and only writes
	 * writing unique buckets but doesn't batch
	 * @param pathID
	 * @param data
	 * @param writeBackTime 
	 * @return success
	 */
	public boolean writePaths(List<Path> paths, long timestamp, boolean update) {
		int successfulWrites = 0;
		String statement;
		if (update) {
			statement = "UPDATE buckets SET (bucket, timestamp) = (?, ?)" + " WHERE id = ? AND timestamp <= ?";
		} else {
			statement = "INSERT INTO buckets (id, bucket, timestamp) VALUES (?, ?, ?)"
					+ " ON CONFLICT (id) DO UPDATE SET (bucket, timestamp)=(excluded.bucket, excluded.timestamp)"
					+ " WHERE excluded.timestamp >= buckets.timestamp";
		}
		Map<Long, byte[]> buckets = getUniqueBuckets(paths);
		// TODO could be parallelized
		try (Connection connection = writeDS.getConnection();
				PreparedStatement pstmt = connection.prepareStatement(statement)) {
			for (Entry<Long, byte[]> pair : buckets.entrySet()) {
				if (update) {
					pstmt.setBytes(1, pair.getValue());
					pstmt.setLong(2, timestamp);
					pstmt.setLong(3, pair.getKey());
					pstmt.setLong(4, timestamp);
				} else {
					pstmt.setLong(1, pair.getKey());
					pstmt.setBytes(2, pair.getValue());
					pstmt.setLong(3, timestamp);
				}
				successfulWrites += runSQLUpdate(connection, pstmt);
			}
		} catch (SQLException e) {
			System.out.printf("CockroachDao.writePaths ERROR: { state => %s, cause => %s, message => %s }\n",
					e.getSQLState(), e.getCause(), e.getMessage());
		}
		return successfulWrites == TaoConfigs.TREE_HEIGHT;
	}

	/**
	 * Bulk insert paths
	 * @param paths
	 * @param timestamp
	 * @return success
	 */
	public boolean batchWritePaths(List<Path> paths, long timestamp, int batch_size, boolean update) {
		TaoLogger.logInfo("Doing a batch write for " + paths.size() + " paths.");
		String statement;
		if (update) {
			statement = "UPDATE buckets SET (bucket, timestamp) = (?, ?)" + " WHERE id = ? AND timestamp <= ?";
		} else {
			statement = "INSERT INTO buckets (id, bucket, timestamp) VALUES (?, ?, ?)"
					+ " ON CONFLICT (id) DO UPDATE SET (bucket, timestamp)=(excluded.bucket, excluded.timestamp)"
					+ " WHERE excluded.timestamp >= buckets.timestamp";
		}

		Map<Long, byte[]> buckets = getUniqueBuckets(paths);

		int successfulWrites = 0;
		int retryCount = 0;
		try (Connection connection = writeDS.getConnection()) {
			while (retryCount <= MAX_RETRY_COUNT) {
				if (retryCount == MAX_RETRY_COUNT) {
					String err = String.format("hit max of %s retries, aborting", MAX_RETRY_COUNT);
					throw new RuntimeException(err);
				}
				try (PreparedStatement pstmt = connection.prepareStatement(statement)) {
					Iterator<Entry<Long, byte[]>> it = buckets.entrySet().iterator();
					for (int i = 0; i <= buckets.size() / batch_size; i++) {
						final int batch_start = i * batch_size;
						final int batch_end = Math.min((i + 1) * batch_size, buckets.size());
						for (int j = batch_start; j < batch_end; j++) {
							Entry<Long, byte[]> pair = it.next();

							if (update) {
								pstmt.setBytes(1, pair.getValue());
								pstmt.setLong(2, timestamp);
								pstmt.setLong(3, pair.getKey());
								pstmt.setLong(4, timestamp);
							} else {
								pstmt.setLong(1, pair.getKey());
								pstmt.setBytes(2, pair.getValue());
								pstmt.setLong(3, timestamp);
							}

							pstmt.addBatch();
						}
						int[] count = pstmt.executeBatch();
						successfulWrites += count.length;
					}
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
					} else {
						throw e;
					}
				}
			}
		} catch (SQLException e) {
			System.out.printf("CockroachDao.writePaths ERROR: { state => %s, cause => %s, message => %s }\n",
					e.getSQLState(), e.getCause(), e.getMessage());
		}
		return successfulWrites == paths.size() * TaoConfigs.TREE_HEIGHT;
	}

	/**
	 * Perform any necessary cleanup of the data store so it can be
	 * used again.
	 */
	public void tearDown() {
		runSQLUpdate("DROP TABLE buckets;");
		writeDS.close();
	}
}