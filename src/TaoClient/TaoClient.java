package TaoClient;

import Configuration.ArgumentParser;
import Configuration.TaoConfigs;
import Messages.*;
import TaoProxy.*;
import com.google.common.primitives.Bytes;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.math3.distribution.ZipfDistribution;

/**
 * @brief Class to represent a client of TaoStore
 */
public class TaoClient implements Client {
	// The address of the proxy
	protected InetSocketAddress mProxyAddress;

	// The address of this client
	protected InetSocketAddress mClientAddress;

	// A MessageCreator to create different types of messages to be passed from
	// client, proxy, and server
	protected MessageCreator mMessageCreator;

	// Counter to keep track of current request number
	// Incremented after each request
	protected AtomicLong mRequestID;

	// Thread group for asynchronous sockets
	protected AsynchronousChannelGroup mThreadGroup;

	// Map of request IDs to ProxyResponses. Used to differentiate which request a
	// response is answering
	protected Map<Long, ProxyResponse> mResponseWaitMap;

	// Channel to proxy
	protected AsynchronousSocketChannel mChannel;

	// ExecutorService for async reads/writes
	protected ExecutorService mExecutor;

	public long mClientID;
	
	public static AtomicLong sNextClientID = new AtomicLong();
	/* Below static variables are used for load testing */

	// Used for measuring response time
	public static List<Long> sResponseTimes = new ArrayList<>();

	// Used for locking the async load test until all the operations are replied to
	public static Object sAsycLoadLock = new Object();

	// List of bytes used for writing blocks as well as comparing the results of
	// returned block data
	public static ArrayList<byte[]> sListOfBytes = new ArrayList<>();

	// Map used during async load tests to map a blockID to the bytes that it should
	// be compared to upon proxy reply
	public static Map<Long, Integer> sReturnMap = new HashMap<>();

	// Number of operations in load test
	public static int LOAD_SIZE = 1000;

	// Number of unique data items in load test
	public static int NUM_DATA_ITEMS = 1000;

	// Whether or not a load test is for an async load or not
	public static boolean ASYNC_LOAD = false;

	public static long loadTestStartTime;

	public static ArrayList<Double> sThroughputs = new ArrayList<>();
	
	public static final long MAX_CLIENT_ID = 128;
	

	/**
	 * @brief Default constructor
	 */
	public TaoClient() {
		try {
			// Initialize needed constants
			TaoConfigs.initConfiguration();
			
			mClientID = sNextClientID.getAndAdd(1);

			// Get the current client's IP
			String currentIP = InetAddress.getLocalHost().getHostAddress();
			mClientAddress = new InetSocketAddress(currentIP, TaoConfigs.CLIENT_PORT);

			// Initialize proxy address
			mProxyAddress = new InetSocketAddress(TaoConfigs.PROXY_HOSTNAME, TaoConfigs.PROXY_PORT);

			// Create message creator
			mMessageCreator = new TaoMessageCreator();

			// Initialize response wait map
			mResponseWaitMap = new ConcurrentHashMap<>();

			// Thread group used for asynchronous I/O
			mThreadGroup = AsynchronousChannelGroup.withFixedThreadPool(TaoConfigs.PROXY_THREAD_COUNT,
					Executors.defaultThreadFactory());

			boolean connected = false;
			while (!connected) {
				try {
					// Create and connect channel to proxy
					mChannel = AsynchronousSocketChannel.open(mThreadGroup);
					Future<?> connection = mChannel.connect(mProxyAddress);
					connection.get();
					connected = true;
				} catch (Exception e) {
					try {
						mChannel.close();
					} catch (Exception e2) {
					}
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e3) {
					}
				}
			}

			// Create executor
			mExecutor = Executors.newFixedThreadPool(TaoConfigs.PROXY_THREAD_COUNT, Executors.defaultThreadFactory());

			// Request ID counter
			mRequestID = new AtomicLong();

			// Create listener for proxy responses
			Runnable serializeProcedure = () -> serveProxy();
			new Thread(serializeProcedure).start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * @brief Constructor
	 * @param messageCreator
	 */
	public TaoClient(MessageCreator messageCreator) {
		try {
			// Initialize needed constants
			TaoConfigs.initConfiguration();

			// Get the current client's IP
			String currentIP = InetAddress.getLocalHost().getHostAddress();
			mClientAddress = new InetSocketAddress(currentIP, TaoConfigs.CLIENT_PORT);

			// Initialize proxy address
			mProxyAddress = new InetSocketAddress(TaoConfigs.PROXY_HOSTNAME, TaoConfigs.PROXY_PORT);

			// Create message creator
			mMessageCreator = messageCreator;

			// Initialize response wait map
			mResponseWaitMap = new ConcurrentHashMap<>();

			// Thread group used for asynchronous I/O
			mThreadGroup = AsynchronousChannelGroup.withFixedThreadPool(TaoConfigs.PROXY_THREAD_COUNT,
					Executors.defaultThreadFactory());

			boolean connected = false;
			while (!connected) {
				try {
					// Create and connect channel to proxy
					mChannel = AsynchronousSocketChannel.open(mThreadGroup);
					Future<?> connection = mChannel.connect(mProxyAddress);
					connection.get();
					connected = true;
				} catch (Exception e) {
					try {
						mChannel.close();
					} catch (Exception e2) {
					}
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e3) {
					}
				}
			}

			// Create executor
			mExecutor = Executors.newFixedThreadPool(TaoConfigs.PROXY_THREAD_COUNT, Executors.defaultThreadFactory());

			// Request ID counter
			mRequestID = new AtomicLong();

			// Create listener for proxy responses
			Runnable serializeProcedure = () -> serveProxy();
			new Thread(serializeProcedure).start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public byte[] read(long blockID) {
		try {
			// Make request
			ClientRequest request = makeRequest(MessageTypes.CLIENT_READ_REQUEST, blockID, null, null);

			// Send read request
			ProxyResponse response = sendRequestWait(request);

			// Return read data
			return response.getReturnData();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

	@Override
	public boolean write(long blockID, byte[] data) {
		try {
			// Make request
			ClientRequest request = makeRequest(MessageTypes.CLIENT_WRITE_REQUEST, blockID, data, null);

			// Send write request
			ProxyResponse response = sendRequestWait(request);

			// Return write status
			return response.getWriteStatus();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * @brief Method to make a client request
	 * @param type
	 * @param blockID
	 * @param data
	 * @param extras
	 * @return a client request
	 */
	protected ClientRequest makeRequest(int type, long blockID, byte[] data, List<Object> extras) {
		// Keep track of requestID and increment it
		// Because request IDs must be globally unique, we bit-shift the
		// request ID to the left and add the client ID, thereby
		// ensuring that no two clients use the same request ID
		long requestID = (Long.highestOneBit(MAX_CLIENT_ID) + 1) * mRequestID.getAndAdd(1) + mClientID;

		// Create client request
		ClientRequest request = mMessageCreator.createClientRequest();
		request.setBlockID(blockID);
		request.setRequestID(requestID);
		request.setClientAddress(mClientAddress);

		// Set additional data depending on message type
		if (type == MessageTypes.CLIENT_READ_REQUEST) {
			if (ASYNC_LOAD) {
				sReturnMap.put(requestID, (int) blockID - 1);
			}
			request.setType(MessageTypes.CLIENT_READ_REQUEST);
			request.setData(new byte[TaoConfigs.BLOCK_SIZE]);
		} else if (type == MessageTypes.CLIENT_WRITE_REQUEST) {
			request.setType(MessageTypes.CLIENT_WRITE_REQUEST);
			request.setData(data);
		}

		return request;
	}

	/**
	 * @brief Private helper method to send request to proxy and wait for response
	 * @param request
	 * @return ProxyResponse to request
	 */
	protected ProxyResponse sendRequestWait(ClientRequest request) {
		try {
			// Create an empty response and put it in the mResponseWaitMap
			ProxyResponse proxyResponse = mMessageCreator.createProxyResponse();
			mResponseWaitMap.put(request.getRequestID(), proxyResponse);

			// Send request and wait until response
			synchronized (proxyResponse) {
				sendRequestToProxy(request);
				proxyResponse.wait();
			}

			// Return response
			return proxyResponse;
		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * @brief Private helper method to send a read or write request to a TaoStore proxy
	 * @param request
	 * @return a ProxyResponse
	 */
	protected void sendRequestToProxy(ClientRequest request) {
		try {
			// Send request to proxy
			byte[] serializedRequest = request.serialize();
			byte[] requestHeader = MessageUtility.createMessageHeaderBytes(request.getType(), serializedRequest.length);
			ByteBuffer requestMessage = ByteBuffer.wrap(Bytes.concat(requestHeader, serializedRequest));

			// Send message to proxy
			synchronized (mChannel) {
				TaoLogger.logDebug("Sending request #" + request.getRequestID());
				while (requestMessage.remaining() > 0) {
					Future<Integer> writeResult = mChannel.write(requestMessage);
					writeResult.get();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * @brief Method to serve a proxy connection
	 * @param channel
	 */
	private void serveProxy() {
		try {
			// Create a ByteBuffer to read in message type
			ByteBuffer typeByteBuffer = MessageUtility.createTypeReceiveBuffer();

			// Asynchronously read message
			mChannel.read(typeByteBuffer, null, new CompletionHandler<Integer, Void>() {
				@Override
				public void completed(Integer result, Void attachment) {
					// Flip the byte buffer for reading
					typeByteBuffer.flip();

					// Figure out the type of the message
					int[] typeAndLength = MessageUtility.parseTypeAndLength(typeByteBuffer);
					int messageType = typeAndLength[0];
					int messageLength = typeAndLength[1];

					// Serve message based on type
					if (messageType == MessageTypes.PROXY_RESPONSE) {
						// Get the rest of the message
						ByteBuffer messageByteBuffer = ByteBuffer.allocate(messageLength);

						// Do one last asynchronous read to get the rest of the message
						mChannel.read(messageByteBuffer, null, new CompletionHandler<Integer, Void>() {
							@Override
							public void completed(Integer result, Void attachment) {
								// Make sure we read all the bytes
								while (messageByteBuffer.remaining() > 0) {
									mChannel.read(messageByteBuffer, null, this);
									return;
								}
								// Flip the byte buffer for reading
								messageByteBuffer.flip();

								// Get the rest of the bytes for the message
								byte[] requestBytes = new byte[messageLength];
								messageByteBuffer.get(requestBytes);

								// Initialize ProxyResponse object based on read bytes
								ProxyResponse proxyResponse = mMessageCreator.createProxyResponse();
								proxyResponse.initFromSerialized(requestBytes);

								// Get the ProxyResponse from map and initialize it
								ProxyResponse clientAnswer = mResponseWaitMap.get(proxyResponse.getClientRequestID());
								clientAnswer.initFromSerialized(requestBytes);

								// Notify thread waiting for this response id
								synchronized (clientAnswer) {
									clientAnswer.notifyAll();
									TaoLogger.logInfo("Got response to request #" + clientAnswer.getClientRequestID());
									mResponseWaitMap.remove(clientAnswer.getClientRequestID());

									if (ASYNC_LOAD) {
										// If this is an async load, we need to notify the test that we are done
										if (clientAnswer.getClientRequestID() == (NUM_DATA_ITEMS + LOAD_SIZE - 1)) {
											synchronized (sAsycLoadLock) {
												sAsycLoadLock.notifyAll();
											}
										}

										// Check for correctness
										if (sReturnMap.get(clientAnswer.getClientRequestID()) != null) {
											if (!Arrays.equals(
													sListOfBytes.get(sReturnMap.get(clientAnswer.getClientRequestID())),
													clientAnswer.getReturnData())) {
												TaoLogger.logError("Read failed for block "
														+ sReturnMap.get(clientAnswer.getClientRequestID()));
												System.exit(1);
											}
										}
									} else {
										TaoLogger.logForce("Not an async load");
									}
									serveProxy();
								}
							}

							@Override
							public void failed(Throwable exc, Void attachment) {
								// TODO: implement?
							}
						});
					}
				}

				@Override
				public void failed(Throwable exc, Void attachment) {
					// TODO: implement?
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public Future<byte[]> readAsync(long blockID) {
		Callable<byte[]> readTask = () -> {
			// Make request
			ClientRequest request = makeRequest(MessageTypes.CLIENT_READ_REQUEST, blockID, null, null);

			// Send read request
			ProxyResponse response = sendRequestWait(request);
			return response.getReturnData();
		};

		Future<byte[]> future = mExecutor.submit(readTask);

		return future;
	}

	@Override
	public Future<Boolean> writeAsync(long blockID, byte[] data) {
		Callable<Boolean> readTask = () -> {
			// Make request
			ClientRequest request = makeRequest(MessageTypes.CLIENT_WRITE_REQUEST, blockID, data, null);

			// Send write request
			ProxyResponse response = sendRequestWait(request);
			return response.getWriteStatus();
		};

		Future<Boolean> future = mExecutor.submit(readTask);

		return future;
	}

	@Override
	public void printSubtree() {
		try {
			// Get proxy name and port
			Socket clientSocket = new Socket(mProxyAddress.getHostName(), mProxyAddress.getPort());

			// Create output stream
			DataOutputStream output = new DataOutputStream(clientSocket.getOutputStream());

			// Create client request
			ClientRequest request = mMessageCreator.createClientRequest();
			request.setType(MessageTypes.PRINT_SUBTREE);

			byte[] serializedRequest = request.serialize();
			byte[] header = MessageUtility.createMessageHeaderBytes(request.getType(), serializedRequest.length);
			output.write(header);

			// Close streams and ports
			clientSocket.close();
			output.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void writeStatistics() {
		try {
			// Get proxy name and port
			Socket clientSocket = new Socket(mProxyAddress.getHostName(), mProxyAddress.getPort());

			// Create output stream
			DataOutputStream output = new DataOutputStream(clientSocket.getOutputStream());

			// Create client request
			ClientRequest request = mMessageCreator.createClientRequest();
			request.setType(MessageTypes.WRITE_STATS);
			request.setClientAddress(mClientAddress);

			byte[] serializedRequest = request.serialize();
			byte[] header = MessageUtility.createMessageHeaderBytes(request.getType(), serializedRequest.length);
			output.write(header);

			// Close streams and ports
			clientSocket.close();
			output.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * @brief Method to do a load test on proxy
	 * @param client
	 * @param zipf_k 
	 * @param rwRatio 
	 */
	public static void loadTestAsync(Client client, double rwRatio, double zipfExp) throws InterruptedException {
		// Random number generator
		SecureRandom r = new SecureRandom();

		ZipfDistribution zipf = new ZipfDistribution(NUM_DATA_ITEMS, zipfExp);

		// Do a write for numDataItems blocks
		long blockID;
		sListOfBytes = new ArrayList<>();

		boolean writeStatus;
		for (int i = 1; i <= NUM_DATA_ITEMS; i++) {
			TaoLogger.logInfo("Doing a write for block " + i);
			blockID = i;
			byte[] dataToWrite = new byte[TaoConfigs.BLOCK_SIZE];
			Arrays.fill(dataToWrite, (byte) blockID);
			sListOfBytes.add(dataToWrite);

			writeStatus = client.write(blockID, dataToWrite);

			if (!writeStatus) {
				TaoLogger.logError("Write failed for block " + i);
				System.exit(1);
			} else {
				TaoLogger.logInfo("Write was successful for " + i);
			}
		}

		double readOrWrite;
		int targetBlock;

		TaoLogger.logForce("Going to start load test");
		long startTime = System.currentTimeMillis();
		for (int i = 0; i < LOAD_SIZE; i++) {
			readOrWrite = r.nextDouble();
			targetBlock = zipf.sample();

			if (readOrWrite < rwRatio) {
				TaoLogger.logInfo(
						"Doing read request #" + ((TaoClient) client).mRequestID.get() + " for block " + targetBlock);

				// Send read and keep track of response time
				long start = System.currentTimeMillis();
				client.readAsync(targetBlock);
				sResponseTimes.add(System.currentTimeMillis() - start);
			} else {
				TaoLogger.logInfo(
						"Doing write request #" + ((TaoClient) client).mRequestID.get() + " for block " + targetBlock);

				// Send write and keep track of response time
				long start = System.currentTimeMillis();
				client.writeAsync(targetBlock, sListOfBytes.get(targetBlock - 1));
				sResponseTimes.add(System.currentTimeMillis() - start);
			}
		}
		TaoLogger.logForce("Going to wait");
		synchronized (sAsycLoadLock) {
			sAsycLoadLock.wait();
		}

		long endTime = System.currentTimeMillis();
		TaoLogger.logForce("Ending load test");

		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
		}

		client.writeStatistics();

		// TODO: Fix this average
		// TaoLogger.logForce("Average response time was " + average + " ms");
		TaoLogger.logForce("Test took " + (endTime - startTime) + " ms");
	}

	public static void loadTest(Client client, double rwRatio, double zipfExp) throws InterruptedException {
		// Random number generator
		SecureRandom r = new SecureRandom();

		ZipfDistribution zipf = new ZipfDistribution(NUM_DATA_ITEMS, zipfExp);

		// Do a write for numDataItems blocks
		long blockID;
		ArrayList<byte[]> listOfBytes = new ArrayList<>();

		boolean writeStatus;
		for (int i = 1; i <= NUM_DATA_ITEMS; i++) {
			TaoLogger.logInfo("Doing a write for block " + i);
			blockID = i;
			byte[] dataToWrite = new byte[TaoConfigs.BLOCK_SIZE];
			Arrays.fill(dataToWrite, (byte) blockID);
			listOfBytes.add(dataToWrite);

			writeStatus = client.write(blockID, dataToWrite);

			if (!writeStatus) {
				TaoLogger.logError("Write failed for block " + i);
				System.exit(1);
			} else {
				TaoLogger.logInfo("Write was successful for " + i);
			}
		}

		double readOrWrite;
		int targetBlock;
		byte[] z;

		TaoLogger.logForce("Going to start load test");
		long startTime = System.currentTimeMillis();
		for (int i = 0; i < LOAD_SIZE; i++) {
			readOrWrite = r.nextDouble();
			targetBlock = zipf.sample();

			if (readOrWrite < rwRatio) {
				TaoLogger.logInfo("Doing read request #" + ((TaoClient) client).mRequestID.get());

				// Send read and keep track of response time
				long start = System.currentTimeMillis();
				z = client.read(targetBlock);
				sResponseTimes.add(System.currentTimeMillis() - start);

				if (!Arrays.equals(listOfBytes.get(targetBlock - 1), z)) {
					TaoLogger.logError("Read failed for block " + targetBlock);
					System.exit(1);
				}
			} else {
				TaoLogger.logInfo("Doing write request #" + ((TaoClient) client).mRequestID.get());

				// Send write and keep track of response time
				long start = System.currentTimeMillis();
				writeStatus = client.write(targetBlock, listOfBytes.get(targetBlock - 1));
				sResponseTimes.add(System.currentTimeMillis() - start);

				if (!writeStatus) {
					TaoLogger.logError("Write failed for block " + targetBlock);
					System.exit(1);
				}
			}
		}

		long endTime = System.currentTimeMillis();
		TaoLogger.logForce("Ending load test");

		// Get average response time over 1000 operations
		long total = 0;
		for (Long l : sResponseTimes) {
			total += l;
		}
		float average = total / ((float) sResponseTimes.size());

		TaoLogger.logForce("Average response time was " + average + " ms");
		TaoLogger.logForce("Test took " + (endTime - startTime) + " ms");
	}

	public static void multiClientLoadTest(int concurrentClients, int loadTestLength, double rwRatio, double zipfExp)
			throws InterruptedException {
		final ArrayList<byte[]> listOfBytes = new ArrayList<>();
		Callable<Integer> loadTestClientThread = () -> {
			TaoClient client = new TaoClient();

			// Random number generator
			SecureRandom r = new SecureRandom();

			ZipfDistribution zipf = new ZipfDistribution((int) NUM_DATA_ITEMS, zipfExp);

			int operationCount = 0;

			double readOrWrite;
			int targetBlock;
			byte[] z;

			boolean writeStatus;
			while (System.currentTimeMillis() < loadTestStartTime + loadTestLength) {
				readOrWrite = r.nextDouble();
				targetBlock = zipf.sample();

				if (readOrWrite < rwRatio) {
					TaoLogger.logInfo("Doing read request #" + ((TaoClient) client).mRequestID.get());

					// Send read and keep track of response time
					long start = System.currentTimeMillis();
					z = client.read(targetBlock);
					sResponseTimes.add(System.currentTimeMillis() - start);

					if (!Arrays.equals(listOfBytes.get(targetBlock - 1), z)) {
						TaoLogger.logError("Read failed for block " + targetBlock);
						System.exit(1);
					}
				} else {
					TaoLogger.logInfo("Doing write request #" + ((TaoClient) client).mRequestID.get());

					// Send write and keep track of response time
					long start = System.currentTimeMillis();
					writeStatus = client.write(targetBlock, listOfBytes.get(targetBlock - 1));
					sResponseTimes.add(System.currentTimeMillis() - start);

					if (!writeStatus) {
						TaoLogger.logError("Write failed for block " + targetBlock);
						System.exit(1);
					}
				}
				operationCount++;
			}

			synchronized (sThroughputs) {
				sThroughputs.add((double) operationCount);
			}

			return 1;
		};

		// Do a write for numDataItems blocks
		TaoClient client = new TaoClient();
		long blockID;
		boolean writeStatus;

		for (int i = 1; i <= NUM_DATA_ITEMS; i++) {
			TaoLogger.logInfo("Doing a write for block " + i);
			blockID = i;
			byte[] dataToWrite = new byte[TaoConfigs.BLOCK_SIZE];
			Arrays.fill(dataToWrite, (byte) blockID);
			listOfBytes.add(dataToWrite);

			writeStatus = client.write(blockID, dataToWrite);

			if (!writeStatus) {
				TaoLogger.logError("Write failed for block " + i);
				System.exit(1);
			} else {
				TaoLogger.logInfo("Write was successful for " + i);
			}
		}

		// Begin actual load test
		ExecutorService clientThreadExecutor = Executors.newFixedThreadPool(concurrentClients,
				Executors.defaultThreadFactory());
		loadTestStartTime = System.currentTimeMillis();
		for (int i = 0; i < concurrentClients; i++) {
			clientThreadExecutor.submit(loadTestClientThread);
		}
		clientThreadExecutor.shutdown();
		clientThreadExecutor.awaitTermination(loadTestLength * 2, TimeUnit.MILLISECONDS);

		double throughputTotal = 0;
		for (Double l : sThroughputs) {
			throughputTotal += l;
		}
		double averageThroughput = throughputTotal / (loadTestLength / 1000);

		TaoLogger.logForce("Ending load test");

		if (sResponseTimes.size() == 0) {
			TaoLogger.logForce("Could not record any response times, try testing for a longer duration.");
			return;
		}

		// Get average response time
		long total = 0;
		for (Long l : sResponseTimes) {
			total += l;
		}
		float average = total / sResponseTimes.size();

		// TaoLogger.logForce("TPS: "+(requestsPerSecond));
		TaoLogger.logForce("Average response time was " + average + " ms");
		TaoLogger.logForce("Throughput: " + averageThroughput);
	}

	public static void main(String[] args) {
		try {
			// Parse any passed in args
			Map<String, String> options = ArgumentParser.parseCommandLineArguments(args);

			// Determine if the user has their own configuration file name, or just use the
			// default
			String configFileName = options.getOrDefault("config_file", TaoConfigs.USER_CONFIG_FILE);
			TaoConfigs.USER_CONFIG_FILE = configFileName;

			// Create client
			TaoClient client = new TaoClient();

			// Determine if we are load testing or just making an interactive client
			String runType = options.getOrDefault("runType", "interactive");

			if (runType.equals("interactive")) {
				Scanner reader = new Scanner(System.in);
				while (true) {
					TaoLogger.logForce("W for write, R for read, P for print, Q for quit");
					String option = reader.nextLine();

					if (option.equals("Q")) {
						break;
					} else if (option.equals("W")) {
						TaoLogger.logForce("Enter block ID to write to");
						long blockID = reader.nextLong();

						TaoLogger.logForce("Enter number to fill in block");
						long fill = reader.nextLong();
						byte[] dataToWrite = new byte[TaoConfigs.BLOCK_SIZE];
						Arrays.fill(dataToWrite, (byte) fill);

						TaoLogger.logForce("Going to send write request for " + blockID);
						boolean writeStatus = client.write(blockID, dataToWrite);

						if (!writeStatus) {
							TaoLogger.logForce("Write failed");
							System.exit(1);
						}
					} else if (option.equals("R")) {
						TaoLogger.logForce("Enter block ID to read from");

						long blockID = reader.nextLong();

						TaoLogger.logForce("Going to send read request for " + blockID);
						byte[] result = client.read(blockID);

						TaoLogger.logForce("The result of the read is a block filled with the number " + result[0]);
						TaoLogger.logForce("Last number in the block is  " + result[result.length - 1]);
					} else if (option.equals("P")) {
						client.printSubtree();
					}
				}
				reader.close();
			} else {
				// Determine if we are doing a load test with synchronous operations, or
				// asynchronous
				String load_test_type = options.getOrDefault("load_test_type", "synchronous");

				// Determine the amount of operations in the load test
				String load_size = options.getOrDefault("load_size", Integer.toString(LOAD_SIZE));
				LOAD_SIZE = Integer.parseInt(load_size);

				// Determine the amount of unique data items that can be operated on

				long totalNodes = (long) Math.pow(2, TaoConfigs.TREE_HEIGHT + 1) - 1;
				long totalBlocks = totalNodes * TaoConfigs.BLOCKS_IN_BUCKET;
				String data_set_size = options.getOrDefault("data_set_size", Long.toString(totalBlocks));
				NUM_DATA_ITEMS = Integer.parseInt(data_set_size);

				String rwRatioArg = options.getOrDefault("rwRatio", "0.5");
				double rwRatio = Double.parseDouble(rwRatioArg);

				String zipfExpArg = options.getOrDefault("zipfExp", "1");
				double zipfExp = Double.parseDouble(zipfExpArg);

				if (load_test_type.equals("synchronous")) {
					TaoLogger.logForce("synchronous test");
					loadTest(client, rwRatio, zipfExp);
				} else if (load_test_type.equals("asynchronous")) {
					TaoLogger.logForce("asynchronous test");
					ASYNC_LOAD = true;
					loadTestAsync(client, rwRatio, zipfExp);
				} else if (load_test_type.equals("multiclient")) {
					TaoLogger.logForce("multiclient test");
					int concurrentClients = Integer.parseInt(options.getOrDefault("clients", Integer.toString(1)));
					// loadTestLength in milliseconds
					int loadTestLength = Integer
							.parseInt(options.getOrDefault("load_test_length", Integer.toString(1000 * (2 * 60))));

					multiClientLoadTest(concurrentClients, loadTestLength, rwRatio, zipfExp);
				} else {
					TaoLogger.logForce("Unknown test type: " + load_test_type);
				}

				System.exit(1);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return;
	}

}
