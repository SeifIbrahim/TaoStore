package TaoProxy;

import Configuration.TaoConfigs;
import Messages.ClientRequest;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * @brief Implementation of a class that implements the ClientRequest message type
 */
public class TaoClientRequest implements ClientRequest {
    // The block ID that this request is asking for
    protected long mBlockID;

    // The type of request this is
    protected int mType;

    // Either MessageTypes.CLIENT_READ_REQUEST or MessageTypes.CLIENT_WRITE_REQUEST
    protected byte[] mData;

    // ID that will uniquely identify this request
    protected long mRequestID;

    // The address of the client making the request
    protected InetSocketAddress mClientAddress;
    
	// Channel that the request came from
    protected AsynchronousSocketChannel mChannel;

    /**
     * @brief Default constructor
     */
    public TaoClientRequest() {
        mBlockID = -1;
        mType = -1;
        mData = new byte[TaoConfigs.BLOCK_SIZE];
        mRequestID = -1;
        mClientAddress = null;
    }

    @Override
    public void initFromSerialized(byte[] serialized) {
        int startIndex = 0;
        mBlockID = Longs.fromByteArray(Arrays.copyOfRange(serialized, startIndex, startIndex + 8));
        startIndex += 8;
        mType = Ints.fromByteArray(Arrays.copyOfRange(serialized, startIndex, startIndex + 4));
        startIndex += 4;
        mRequestID = Longs.fromByteArray(Arrays.copyOfRange(serialized, startIndex, startIndex + 8));
        startIndex += 8;
        mData = Arrays.copyOfRange(serialized, startIndex, startIndex + TaoConfigs.BLOCK_SIZE);
        startIndex += TaoConfigs.BLOCK_SIZE;
        int hostnameSize = Ints.fromByteArray(Arrays.copyOfRange(serialized, startIndex, startIndex + 4));
        startIndex += 4;
        byte[] hostnameBytes = Arrays.copyOfRange(serialized, startIndex, startIndex + hostnameSize);
        startIndex += hostnameSize;
        String hostname = new String(hostnameBytes, StandardCharsets.UTF_8);
        int port = Ints.fromByteArray(Arrays.copyOfRange(serialized, startIndex, startIndex + 4));
        startIndex += 4;
        // Cache to avoid having to recreate InetSocketAddress object
        mClientAddress = ClientAddressCache.getFromCache(hostname, Integer.toString(port));
    }

    @Override
    public long getBlockID() {
        return mBlockID;
    }

    @Override
    public void setBlockID(long blockID) {
        mBlockID = blockID;
    }

    @Override
    public int getType() {
        return mType;
    }

    @Override
    public void setType(int type) {
        mType = type;
    }

    @Override
    public byte[] getData() {
        return mData;
    }

    @Override
    public void setData(byte[] data) {
        System.arraycopy(data, 0, mData, 0, mData.length);
    }

    @Override
    public long getRequestID() {
        return mRequestID;
    }

    @Override
    public void setRequestID(long requestID) {
        mRequestID = requestID;
    }

    @Override
    public InetSocketAddress getClientAddress() {
        return mClientAddress;
    }

    @Override
    public void setClientAddress(InetSocketAddress clientAddress) {
        mClientAddress = clientAddress;
    }

    @Override
    public byte[] serialize() {
        byte[] blockIDBytes = Longs.toByteArray(mBlockID);
        byte[] typeBytes = Ints.toByteArray(mType);
        byte[] idBytes = Longs.toByteArray(mRequestID);
        byte[] hostnameLengthBytes = Ints.toByteArray(mClientAddress.getHostName().length());
        byte[] hostnameBytes = mClientAddress.getHostName().getBytes(StandardCharsets.UTF_8);
        byte[] portBytes = Ints.toByteArray(mClientAddress.getPort());

        return Bytes.concat(blockIDBytes, typeBytes, idBytes, mData, hostnameLengthBytes, hostnameBytes, portBytes);
    }

    @Override
    public boolean equals(Object obj) {
        if ( ! (obj instanceof ClientRequest) ) {
            return false;
        }

        if (obj == this) {
            return true;
        }

        // Two requests are equal if they have the same request ID
        TaoClientRequest rhs = (TaoClientRequest) obj;

        if (mRequestID != rhs.getRequestID()) {
            return false;
        }

        // If two requests have the same request ID, check to make sure they are not from different hosts
        if (! mClientAddress.getHostName().equals(rhs.getClientAddress().getHostName())) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(mRequestID, mClientAddress.getHostName());
    }

    @Override
    public void setChannel(AsynchronousSocketChannel channel) {
        mChannel = channel;
    }
    
    @Override
    public AsynchronousSocketChannel getChannel() {
        return mChannel;
    }
}
