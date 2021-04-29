package TaoProxy;

import java.nio.channels.AsynchronousChannelGroup;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import Configuration.TaoConfigs;
import Messages.ClientRequest;
import Messages.MessageCreator;
import Messages.ServerResponse;

public class CockroachTaoProcessor extends TaoProcessor {

	private CockroachDao cockroachDao;

	public CockroachTaoProcessor(Proxy proxy, Sequencer sequencer, AsynchronousChannelGroup threadGroup,
			MessageCreator messageCreator, PathCreator pathCreator, CryptoUtil cryptoUtil, Subtree subtree,
			PositionMap positionMap, Map<Long, Long> relativeMapper, Profiler profiler) {
		super(proxy, sequencer, threadGroup, messageCreator, pathCreator, cryptoUtil, subtree, positionMap,
				relativeMapper, profiler);
		this.cockroachDao = CockroachDao.getInstance(mCryptoUtil);
	}

	@Override
	public void readPath(ClientRequest req) {
		mProfiler.readPathStart(req);

		TaoLogger
				.logInfo("Starting a readPath for blockID " + req.getBlockID() + " and request #" + req.getRequestID());

		// Create new entry into response map for this request
		mResponseMap.put(req, new ResponseMapEntry());

		// Variables needed for fake read check
		boolean fakeRead;
		long pathID;

		// We make sure the request list and read/write lock for maps are not null
		List<ClientRequest> requestList = new ArrayList<>();
		ReentrantReadWriteLock requestListLock = new ReentrantReadWriteLock();
		mRequestMap.putIfAbsent(req.getBlockID(), requestList);
		mRequestLockMap.putIfAbsent(req.getBlockID(), requestListLock);
		requestList = mRequestMap.get(req.getBlockID());
		requestListLock = mRequestLockMap.get(req.getBlockID());

		// Acquire a read lock to ensure we do not assign a fake read that will not be
		// answered to
		requestListLock.readLock().lock();

		// Check if there is any current request for this block ID
		if (requestList.isEmpty()) {
			// If no other requests for this block ID have been made, it is not a fake read
			fakeRead = false;

			// Find the path that this block maps to
			pathID = mPositionMap.getBlockPosition(req.getBlockID());

			// If pathID is -1, that means that this blockID is not yet mapped to a path
			if (pathID == -1) {
				// Fetch a random path from server
				pathID = mCryptoUtil.getRandomPathID();
			}
		} else {
			// There is currently a request for the block ID, so we need to trigger a fake
			// read
			fakeRead = true;

			// Fetch a random path from server
			pathID = mCryptoUtil.getRandomPathID();
		}
		// UnlockLOG_WARNING
		requestListLock.readLock().unlock();

		// Add request to request map list
		requestListLock.writeLock().lock();
		requestList.add(req);
		requestListLock.writeLock().unlock();

		TaoLogger.logDebug("Doing a read for pathID " + pathID);

		// Insert request into mPathReqMultiSet to make sure that this path is not
		// deleted before this response returns from server
		mPathReqMultiSet.add(pathID);

		byte[] encryptedPath = this.cockroachDao.readPath(pathID);

		// need to wrap path in a "ServerResponse" to make the proxy happy
		ServerResponse response = new TaoServerResponse();
		response.setIsWrite(false);
		response.setPathID(pathID);
		response.setPathBytes(encryptedPath);

		Runnable serializeProcedure = () -> mProxy.onReceiveResponse(req, response, fakeRead);
		new Thread(serializeProcedure).start();
		mProfiler.readPathComplete(req);
	}

	@Override
	public void writeBack(long timeStamp) {
		// Variable to keep track of the current mNextWriteBack
		long writeBackTime;

		// Check to see if a write back should be started
		if (mWriteBackCounter >= mNextWriteBack) {
			// Multiple threads might pass first condition, must acquire lock in order to be
			// the thread that triggers
			// the write back
			if (mWriteBackLock.tryLock()) {
				// Theoretically could be rare condition when a thread acquires lock but another
				// thread has already
				// acquired the lock and incremented mNextWriteBack, so make sure that condition
				// still holds
				if (mWriteBackCounter >= mNextWriteBack) {
					// Keep track of the time
					writeBackTime = mNextWriteBack;

					// Increment the next time we should write trigger write back
					mNextWriteBack += TaoConfigs.WRITE_BACK_THRESHOLD;

					// Unlock and continue with write back
					mWriteBackLock.unlock();
				} else {
					// Condition no longer holds, so unlock and return
					mWriteBackLock.unlock();
					return;
				}
			} else {
				// Another thread is going to execute write back for this current value of
				// mNextWriteBack, so return
				return;
			}
		} else {
			return;
		}

		mProfiler.writeBackStart(writeBackTime);

		// Needed in order to clean up subtree later
		List<Long> allWriteBackIDs = new ArrayList<>();
		// Take the subtree reader's lock
		mSubtreeRWL.readLock().lock();
		TaoLogger.logInfo("Going to do writeback");
		// take a snapshot of the first TaoConfigs.WRITE_BACK_THRESHOLD path IDs from
		// the mWriteQueue
		List<Path> paths = new ArrayList<Path>();
		for (int i = 0; i < TaoConfigs.WRITE_BACK_THRESHOLD; i++) {
			Long pathID;
			synchronized (mWriteQueue) {
				pathID = mWriteQueue.remove();
			}
			allWriteBackIDs.add(pathID);
			Path path = mSubtree.getPath(pathID);
			if (path != null) {
				TaoPath pathCopy = new TaoPath();
				pathCopy.initFromPath(path);
				paths.add(pathCopy);
			}
		}
		mSubtreeRWL.readLock().unlock();

		for (Path path : paths) {
			byte[] encryptedPath = mCryptoUtil.encryptPath(path);
			final boolean success = this.cockroachDao.writePath(path.getPathID(), encryptedPath, writeBackTime);
			assert success : "Failed to write path " + path.getPathID();
		}

		// Iterate through every path that was written, check if there
		// are any nodes we can delete
		mSubtreeRWL.writeLock().lock();
		for (Long pathID : allWriteBackIDs) {
			// Upon response, delete all nodes in subtree whose
			// timestamp is <= timeStamp, and are not in mPathReqMultiSet
			mSubtree.deleteNodes(pathID, writeBackTime, new HashSet<Long>(mPathReqMultiSet.elementSet()));
		}
		mSubtreeRWL.writeLock().unlock();
	}
}
