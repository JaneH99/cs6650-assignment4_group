package mq;

public class DualChannelPool {

    private final ChannelPool pool1;  // rooms 1-10
    private final ChannelPool pool2;  // rooms 11-20
    private final int splitPoint;     // <= splitPoint → pool1

    public DualChannelPool(ChannelPool pool1, ChannelPool pool2, int splitPoint) {
        this.pool1 = pool1;
        this.pool2 = pool2;
        this.splitPoint = splitPoint;
    }

    public ChannelPool forRoom(int roomId) {
        return roomId <= splitPoint ? pool1 : pool2;
    }

    public void closeAll() {
        pool1.closeAll();
        pool2.closeAll();
    }
}