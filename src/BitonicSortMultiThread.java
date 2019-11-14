import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @program: bitonicSort
 * @description: 双调排序多线程版
 * @author: Ya
 * @create: 2019-11-14 11:23
 **/
public class BitonicSortMultiThread {
    /**
     * 填充数字 ,需要比待排序数组的所有值都要小
     * 默认Integer.MIN_VALUE
     */
    private int paddingNum = Integer.MAX_VALUE;
    private long startTime;
    private ExecutorService exec = Executors.newCachedThreadPool();
    private static CyclicBarrier barrier;


    /**
     * 默认线程数量 当前是4个线程
     */
    private int threadNum = 1 << 2;
    private static int[] array;
    private List<BitonicSortThread> threads = new ArrayList<>();

    public BitonicSortMultiThread(int[] array) {
        barrier = new CyclicBarrier(threadNum, new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < threads.size(); i++) {
                    //如果每个线程都完成了 任务终止
                    boolean res = true;
                    for (BitonicSortThread bst : threads) {
                        // fixme 目前只是生成双调序列 ,
                        if (bst.getSegment() <= array.length) {
                            res = false;
                        }
                    }
                    if (res) {
                        exec.shutdownNow();
                        // 打印花费时间
                        System.out.println("System.out...... -> " + (System.nanoTime() - startTime));
                        return;
                    }
                }
            }
        });
        // 添加任务
        //记录一下开始时间
        startTime = System.nanoTime();
        for (int i = 0; i < threadNum; i++) {
            BitonicSortThread bst = new BitonicSortThread(barrier, array, threadNum);
            threads.add(bst);
            exec.execute(bst);
        }
    }
}

class BitonicSortThread implements Runnable {
    private static CyclicBarrier barrier;
    private static int counter = 0;
    private final int id = counter++;
    private int segment = 2;
    private int threadNum;
    private int[] array;

    public BitonicSortThread(CyclicBarrier b, int[] array, int threadNum) {
        barrier = b;
        this.array = array;
        this.threadNum = threadNum;
    }

    @Override
    public void run() {
        try {
            while (!Thread.interrupted()) {
                // 线程内执行
                if (segment <= array.length) {
                    sortSingle(array, segment, threadNum, id);
                    segment <<= 1;
                }
                barrier.await();
            }
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
    }

    /**
     * 进行分段排序
     * 奇数段递增，偶数段递减,这里已经由id分配好
     * <p>
     * 当分段数 减少至线程数，需要改变方法
     */
    // fixme 应该用不到
    private static void sort(int[] array, int segment) {
        //fixme 这一步判断可以省略，方法入口已经有了判断
        if (segment >= array.length) {
            return;
        }
        boolean order = false;
        for (int i = 0; i < array.length; i += segment) {
            order = !order;
            sortSegment(array, i, i + segment, order);
        }
    }

    /**
     * 进行分段排序
     * 奇数段递增，偶数段递减,这里已经由id分配好
     * <p>
     * 按线程数量分组，
     * 每个线程排自己的部分
     */
    private static void sortSingle(int[] array, int segment, int threadNum, int id) {
        boolean order = (id & 1) == 0;
        int start;
        int end;
        for (int i = 0; segment * threadNum * i + id * segment < array.length; i++) {
            start = segment * threadNum * i + id * segment;
            end = start + segment;
            sortSegment(array, start, end, order);
        }
    }

    /**
     * 段内递归 排序
     */
    private static void sortSegment(int[] array, int start, int end, boolean order) {
        int mid = (start + end) >> 1;
        if (start == mid) {
            return;
        }
        for (int i = 0; start + i < mid; i++) {
            swap(array, start + i, mid + i, order);
        }
        sortSegment(array, start, mid, order);
        sortSegment(array, mid, end, order);
    }

    /**
     * 采用异或的方式进行交换
     *
     * @param array 排序数组
     * @param i     下标i
     * @param j     下标j
     */
    private static void swap(int[] array, int i, int j) {
        array[i] = array[i] ^ array[j];
        array[j] = array[i] ^ array[j];
        array[i] = array[i] ^ array[j];
    }

    /**
     * 交换
     *
     * @param array 数组
     * @param i     下标
     * @param j     下标
     * @param order true升序 false降序
     */
    private static void swap(int[] array, int i, int j, boolean order) {
        if (order && array[i] > array[j]) {
            swap(array, i, j);
        } else if (!order && array[i] < array[j]) {
            swap(array, i, j);
        }
    }

    public int getSegment() {
        return this.segment;
    }
}
