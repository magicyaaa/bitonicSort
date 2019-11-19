import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @program: bitonicSort
 * @description: 双调排序 多线程
 * @author: Ya
 * @create: 2019-11-14 11:23
 * <p>
 * 要求：
 * 1、数组长度必须为2的整数次幂，未实现padding填充。
 * 2、线程数量也必须为2的整数次幂
 * <p>
 * 3、当分段的双调序列数量小于线程数时，未能完全利用线程，待优化。
 * 4、最后对双调序列进行排序未能利用到多线程，待优化。
 * <p>
 * 在百万至亿级长度的数组测试，比Arrays.sort()快排耗时多50%。
 **/
public class BitonicSortMultiThread {
    /**
     * 填充数字 ,需要比待排序数组的所有值都要小
     * 默认Integer.MIN_VALUE
     */
    static final int PADDING_NUM = Integer.MAX_VALUE;
    private long startTime;
    private ExecutorService exec;
    private static CyclicBarrier barrier;
    /**
     * 默认线程数量 当前是8个线程
     */
    public static final int THREAD_SIZE = 1 << 3;

    private List<BitonicSortThread> threads;


    public BitonicSortMultiThread() {

    }

    public void bitonicSort(int[] array) {
        exec = Executors.newCachedThreadPool();
        threads = new ArrayList<>(THREAD_SIZE);
        barrier = new CyclicBarrier(THREAD_SIZE, new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < threads.size(); i++) {
                    //如果每个线程都完成了 任务终止
                    boolean res = true;
                    for (BitonicSortThread bst : threads) {
                        if (!bst.isComplete()) {
                            res = false;
                        }
                    }
                    if (res) {
                        exec.shutdownNow();
                        // 打印花费时间
                        System.out.println("System.out......双调 -> " + (System.nanoTime() - startTime));
                        return;
                    }
                }
            }
        });
        // 添加任务
        //记录一下开始时间
        startTime = System.nanoTime();
        for (int i = 0; i < THREAD_SIZE; i++) {
            BitonicSortThread bst = new BitonicSortThread(barrier, array, THREAD_SIZE);
            threads.add(bst);
            exec.execute(bst);
        }
    }
}

class BitonicSortThread implements Runnable {
    private static CyclicBarrier barrier;
    private static int counter = 0;
    private final int ID = counter++;
    private int[] array;
    private int perThreadCount;
    private int segment = 2;
    private int t_segment = 2;
    private int part;
    private int t_part;
    private int inner_part = 1;

    private int group;
    private int inner_group;
    private int group_offset;
    private int inner_offset;
    private int totalCount;
    private boolean order = true;

    public BitonicSortThread(CyclicBarrier b, int[] array, int threadNum) {
        barrier = b;
        this.array = array;
        this.perThreadCount = (array.length >> 1) / BitonicSortMultiThread.THREAD_SIZE;
        part = array.length >> 1;
        t_part = part;
    }

    @Override
    public void run() {
        try {
            while (!Thread.interrupted()) {
                // 线程内执行
                if (segment <= array.length) {
                    sort();
                }
                barrier.await();
            }
        } catch (Exception e) {
            System.out.println("System.out...... ID-> " + ID);
            e.printStackTrace();
        }
    }

    private void sort() {
        inner_group = BitonicSortMultiThread.THREAD_SIZE / t_part;
        group_offset = (ID / part) % inner_part;
        inner_offset = (ID / part) / inner_part;

        int start, end, limit;

        if (inner_part == 1) {
            group = ID % part;
            order = (group & 1) == 0;
            limit = part / BitonicSortMultiThread.THREAD_SIZE;
            if (inner_group <= 1) {
                for (int i = 0; i < limit; i++) {
                    start = group * segment + i * segment * BitonicSortMultiThread.THREAD_SIZE;
                    sortSegment(array, start, start + segment, order);
                }
                segment <<= 1;
                t_segment = segment;
                part >>= 1;
                t_part = part;
                inner_part = 1;
            } else {
                for (int i = 0; i < perThreadCount; i++) {
                    start = group * segment
                            + group_offset * t_segment // 可以去掉这行 因为 group_offset = 0
                            + inner_offset
                            + i * inner_group;
                    swap(array, start, start + (t_segment >> 1), order);
                }
                inner_part <<= 1;
                t_segment >>= 1;
                t_part <<= 1;
            }
        } else {
            if (inner_group <= 1) {
                start = group * segment + group_offset * t_segment;
                sortSegment(array, start, start + t_segment, order);
                segment <<= 1;
                t_segment = segment;
                part >>= 1;
                t_part = part;
                inner_part = 1;
            } else {
                for (int i = 0; i < perThreadCount; i++) {
                    start = group * segment
                            + group_offset * t_segment
                            + inner_offset
                            + i * inner_group;
                    swap(array, start, start + (t_segment >> 1), order);
                }
                inner_part <<= 1;
                t_segment >>= 1;
                t_part <<= 1;
            }
        }

    }

    /**
     * 段内递归 排序
     * 段数 >= 线程数； 一个段只能由单个线程递归处理
     */
    private static void sortSegment(int[] array, int start, int end, boolean order) {
        if (start == end - 1) {
            return;
        }
        int mid = (start + end) >> 1;
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

    public boolean isComplete() {
        if (this.part == 0) {
            counter = 0;
            return true;
        } else {
            return false;
        }
    }

}
