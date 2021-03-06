package TaoProxy;

import java.nio.channels.AsynchronousChannelGroup;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

import Configuration.ArgumentParser;
import Configuration.TaoConfigs;
import Messages.ClientRequest;
import Messages.MessageCreator;

/**
 * @brief A TaoProxy class which interacts with CockroachDB instead of the TaoServer
 */
public class CockroachTaoProxy extends TaoProxy {

	private CockroachDao cockroachDao;

	/**
	 * @param messageCreator
	 * @param pathCreator
	 * @param subtree
	 */
	public CockroachTaoProxy(MessageCreator messageCreator, PathCreator pathCreator, Subtree subtree) {
		// Initialize needed constants
		TaoConfigs.initConfiguration();

		// For trace purposes
		TaoLogger.logLevel = TaoLogger.LOG_OFF;
		// TaoLogger.BLOCK_DEBUG = true;

		// For profiling purposes
		mProfiler = new TaoProfiler();

		// Create a CryptoUtil
		mCryptoUtil = new TaoCryptoUtil();

		cockroachDao = CockroachDao.getInstance(mCryptoUtil);

		// Assign subtree
		mSubtree = subtree;

		// Create a position map
		mPositionMap = new TaoPositionMap(TaoConfigs.PARTITION_SERVERS);

		// Assign the message and path creators
		mMessageCreator = messageCreator;
		mPathCreator = pathCreator;

		try {
			// Create a thread pool for asynchronous sockets
			mThreadGroup = AsynchronousChannelGroup.withFixedThreadPool(TaoConfigs.PROXY_THREAD_COUNT,
					Executors.defaultThreadFactory());
		} catch (Exception e) {
			e.printStackTrace();
		}

		// Initialize the sequencer and proxy
		mSequencer = new TaoSequencer(mMessageCreator, mPathCreator);
		mProcessor = new CockroachTaoProcessor(this, mSequencer, mThreadGroup, mMessageCreator, mPathCreator,
				mCryptoUtil, mSubtree, mPositionMap, mRelativeLeafMapper, mProfiler);

		requestQueue = new LinkedBlockingDeque<>();
		requestExecutor = Executors.newFixedThreadPool(TaoConfigs.CONNECTION_POOL_SIZE);
	}

	/**
	 * @brief Initializes an empty tree into the database
	 */
	@Override
	public void initializeServer() {
		// Initialize the top of the subtree
		mSubtree.initRoot();

		// Get the total number of paths
		int totalPaths = 1 << TaoConfigs.TREE_HEIGHT;

		TaoLogger.logInfo("Tree height is " + TaoConfigs.TREE_HEIGHT);
		TaoLogger.logInfo("Total paths " + totalPaths);

		TaoLogger.logForce("Creating " + totalPaths + " paths");
		// batch to prevent out of memory error
		final int BATCH_SIZE = 4096 * (4096 / TaoConfigs.BLOCK_SIZE);
		for (int i = 0; i <= totalPaths / BATCH_SIZE; i++) {
			List<Path> paths = new ArrayList<Path>();
			final int batch_start = i * BATCH_SIZE;
			final int batch_end = Math.min((i + 1) * BATCH_SIZE, totalPaths);
			for (int j = batch_start; j < batch_end; j++) {
				// Create empty paths and serialize
				Path defaultPath = mPathCreator.createPath();
				defaultPath.setPathID(j);

				paths.add(defaultPath);
			}
			TaoLogger.logForce("Writing " + paths.size() + " paths");
			this.cockroachDao.batchWritePaths(paths, 0, 128, false);
		}
	}

	@Override
	public void onReceiveRequest(ClientRequest req) {
		try {
			requestQueue.put(req);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void processRequests() {
		try {
			while (true) {
				ClientRequest req;
				req = requestQueue.take();
				requestExecutor.submit(() -> mProcessor.readPath(req));
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		try {
			// Parse any passed in args
			Map<String, String> options = ArgumentParser.parseCommandLineArguments(args);

			// Determine if the user has their own configuration file name, or just use the
			// default
			String configFileName = options.getOrDefault("config_file", TaoConfigs.USER_CONFIG_FILE);
			TaoConfigs.USER_CONFIG_FILE = configFileName;

			// Create proxy
			CockroachTaoProxy proxy = new CockroachTaoProxy(new TaoMessageCreator(), new TaoBlockCreator(),
					new TaoSubtree());

			// Initialize and run server
			proxy.initializeServer();
			TaoLogger.logForce("Finished init, running proxy");
			// launch the consumer thread
			new Thread(() -> proxy.processRequests()).start();
			// launch the producer thread
			proxy.run();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
