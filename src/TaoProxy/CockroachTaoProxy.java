package TaoProxy;

import java.nio.channels.AsynchronousChannelGroup;
import java.util.Map;
import java.util.concurrent.Executors;

import Configuration.ArgumentParser;
import Configuration.TaoConfigs;
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
	public CockroachTaoProxy(MessageCreator messageCreator, PathCreator pathCreator, Subtree subtree,
			int cockroachPort) {
		cockroachDao = CockroachDao.getInstance(cockroachPort);

		// For trace purposes
		TaoLogger.logLevel = TaoLogger.LOG_OFF;

		// For profiling purposes
		mProfiler = new TaoProfiler();

		// Initialize needed constants
		TaoConfigs.initConfiguration();

		// Create a CryptoUtil
		mCryptoUtil = new TaoCryptoUtil();

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
				mCryptoUtil, mSubtree, mPositionMap, mRelativeLeafMapper, mProfiler, cockroachPort);
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

		for (int i = 0; i < totalPaths; i++) {
			TaoLogger.logForce("Creating path " + i);

			// Create empty paths and serialize
			Path defaultPath = mPathCreator.createPath();
			defaultPath.setPathID(i);

			// Encrypt path
			byte[] dataToWrite = mCryptoUtil.encryptPath(defaultPath);
			
			this.cockroachDao.writePath(i, dataToWrite);
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

			String cockroachPortArg = options.get("cockroachPort");
			if (cockroachPortArg == null) {
				TaoLogger.logForce("Please specify cockroachPort");
				return;
			}
			int cockroachPort = Integer.parseInt(cockroachPortArg);

			// Create proxy
			CockroachTaoProxy proxy = new CockroachTaoProxy(new TaoMessageCreator(), new TaoBlockCreator(),
					new TaoSubtree(), cockroachPort);

			// Initialize and run server
			proxy.initializeServer();
			TaoLogger.logForce("Finished init, running proxy");
			proxy.run();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
