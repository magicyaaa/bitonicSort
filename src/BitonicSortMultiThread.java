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
    private int paddingNum = Integer.MAX_VALUE;
    private long startTime;
    private ExecutorService exec = Executors.newCachedThreadPool();
    private static CyclicBarrier barrier;
    /**
     * 默认线程数量 当前是4个线程
     */
    private int threadNum = 1 << 3;
    private static int[] array;
    private List<BitonicSortThread> threads = new ArrayList<>();

    /**
     * 直接调用构造方法，参数为待排序数组
     *
     * @param array
     */
    public BitonicSortMultiThread(int[] array) {
        barrier = new CyclicBarrier(threadNum, new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < threads.size(); i++) {
                    //如果每个线程都完成了 任务终止
                    boolean res = true;
                    for (BitonicSortThread bst : threads) {
                        // fixme 当段数 （数组长度／segment ）小于 线程数量时，无法完全利用线程。 最终双调排序是单线程，需优化。
                        if (!bst.isComplete()) {
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
    private int t_segment = segment;
    private boolean t_order;
    private boolean isRecursion = false;
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
                //fixme 当段数小于线程数量时，需要优化
                if (segment <= array.length) {
                    sortSingle2(array, segment, threadNum, id);
                }
                barrier.await();
            }
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
    }


    /**
     * 固定8线程排序
     */
    private void sortSingle4() {

    }

    /**
     * 最终第一层
     */
    private void finallySort1() {
        int part = 1;
        int seg = array.length;
        int segmentThreadNum = threadNum / part;
        int threadSegmentNum = (seg >> 1) / segmentThreadNum;
        int start, end;
        for (int i = 0; i < threadSegmentNum; i++) {
            start = id * threadSegmentNum + i;
            end = start + (seg >> 1);
            swap(array, start, end, true);
        }
    }

    /**
     * 最终第二层排序
     */
    private void finallySort2() {
        int part = 2;
        int seg = array.length >> 1;
        int segmentThreadNum = threadNum / part;
        int threadSegmentNum = (seg >> 1) / segmentThreadNum;
        int start, end;
        for (int i = 0; i < threadSegmentNum; i++) {
            start = id * threadSegmentNum + i;
            end = start + (seg >> 1);
            swap(array, start, end, true);
        }
    }

    /**
     * 最终第三层排序
     */
    private void finallySort3() {
        int part = 4;
        int seg = array.length >> 2;
        int segmentThreadNum = threadNum / part;
        int threadSegmentNum = (seg >> 1) / segmentThreadNum;
        int start, end;
        for (int i = 0; i < threadSegmentNum; i++) {
            start = id * threadSegmentNum + i;
            end = start + (seg >> 1);
            swap(array, start, end, true);
        }
    }

    /**
     * 最终递归排序
     */
    private void finallySort(int part) {

    }

    /**
     * 再次新写一个
     */
    private void sortSingle3() {
        boolean order;
        int part;
        int t_part;
        int t_id;
        int offset;
        int inner_offset;
        int start;
        int end;
        int segmentThreadNum;
        int threadSegmentNum;

        //一开始 线程数量小于等于段数 ,每个线程处理一个或多个段
        part = array.length / segment;
        t_part = array.length / t_segment;
        if (isRecursion) {
            order = t_order;
            t_id = id % part;
            offset = (id / part) % t_part;

        }
        if (threadNum <= part) {
            order = (id & 1) == 0;
            offset = threadNum * segment;
            for (int i = 0; i * offset < array.length; i++) {
                start = i * offset;
                end = start + (segment >> 1);
                sortSegment(array, start, end, order);
            }
            segment <<= 1;
            t_segment = segment;
        } else if (threadNum > part) {
            //一开始，线程数量大于段数，则每个段由多个线程处理
            order = (id & 1) == 0;
            t_id = id % part;
            offset = (id / part) % part;
            segmentThreadNum = threadNum / part;
            threadSegmentNum = (segment >> 1) / segmentThreadNum;
            inner_offset = t_id * threadSegmentNum;
            for (int i = 0; i < threadSegmentNum; i++) {
                start = t_id * segment + offset * threadSegmentNum + i;
                end = start + (segment >> 1);
                swap(array, start, end, order);
            }
            isRecursion = true;
            t_segment = segment >> 1;
            t_order = order;
        }

    }

    private void sortSingle2(int[] array, int segment, int threadNum, int id) {
        //fixme 变量先声明 内部使用
        boolean order = (id & 1) == 0;
        int part = array.length / segment;
        //fixme 可以改为局部变量
        int t_part = array.length / t_segment;
        // 段id
        int t_id;
        int offset;
        int start;
        int end;

        if (part == 1 && !isRecursion) {
            //最终的双调排序
            order = true;
            //单个线程可以处理每段行数
            int threadSegmentNum = (segment >> 1) / threadNum;
            start = id * threadSegmentNum;
            end = start + (segment >> 1);
            for (int i = 0; i < threadSegmentNum; i++) {
                swap(array, start + i, end + i, order);
            }
            this.isRecursion = true;
            this.t_segment = segment >> 1;
            this.t_order = order;
            this.segment <<= 1;
        /*} else if (part == 1 && isRecursion && t_part < threadNum) {
            //最终排序 递归内
            int segmentThreadNum = threadNum / t_part;
            t_id = id % t_part;
            offset = (id / t_part);
            int threadSegmentNum = (t_segment >> 1) / segmentThreadNum;
            start = t_id * t_segment + offset * threadSegmentNum;
            end = start + (t_segment >> 1);
            for (int i = 0; i < threadSegmentNum; i++) {
                swap(array, start + i, end + i, order);
            }
            this.isRecursion = true;
            this.t_segment >>= 1;*/

        } else if (isRecursion && t_part >= threadNum) {
            //段内递归中 && 段数 >= 线程数  --> 实际上会刚好匹配 段数=线程数,直接一次递归完成
            t_id = id % part;
            offset = id / part;
            //段内递归，segment不变,因为线程在段上是跳跃的;偏移量为t_segment
            start = segment * t_id + offset * t_segment;
            end = start + t_segment;
            sortSegment(array, start, end, t_order);
            this.isRecursion = false;
            this.segment <<= 1;
        } else if (isRecursion) {
            //段内递归 && 段数 < 线程数
            int segmentThreadNum = threadNum / t_part;
            t_id = id % part;
            offset = (id / part) % t_part;
            //内部偏移  todo 有问题?
            int inner_offset = id / t_part;
            start = t_id * segment + offset * t_segment + inner_offset * ((t_segment >> 1) / segmentThreadNum);
            end = start + (t_segment >> 1);
            for (int i = 0; i < (t_segment >> 1) / segmentThreadNum; i++) {
                swap(array, start + i, end + i, order);
            }
            this.isRecursion = true;
            this.t_segment >>= 1;
        } else if (part < threadNum) {
            //分段数量 < 线程数量   --> 意味着一个段可以被多个线程处理
            int segmentThreadNum = threadNum / part;
            t_id = id % part;
            offset = (id / part) % part;
            start = t_id * segment + offset * ((segment >> 1) / segmentThreadNum);
            end = start + (segment >> 1);
            for (int i = 0; i < (segment >> 1) / segmentThreadNum; i++) {
                swap(array, start + i, end + i, order);
            }
            this.isRecursion = true;
            this.t_segment = segment >> 1;
            this.t_order = order;
        } else {
            // 分段数量 >= 线程数量   --> 意味着一个线程要处理多个段
            start = id * segment;
            for (int i = 0; id * segment + segment * threadNum * i < array.length; i++) {
                start = id * segment + segment * threadNum * i;
                end = start + segment;
                sortSegment(array, start, end, order);
            }
            this.segment <<= 1;
        }
    }

    /**
     * 进行分段排序
     * 奇数段递增，偶数段递减,这里已经由id分配好
     * <p>
     * 按线程数量分组，
     * 每个线程排自己的部分
     */
    private void sortSingle(int[] array, int segment, int threadNum, int id) {
        boolean order = (id & 1) == 0;
        int start;
        int end;
        for (int i = 0; segment * threadNum * i + id * segment < array.length; i++) {
            start = segment * threadNum * i + id * segment;
            end = start + segment;
            sortSegment(array, start, end, order);
        }
        this.segment <<= 1;
    }

    /**
     * 一个段由多个线程处理
     */
    private static void sortSegmentByMutilThread(int[] array, int start, int end, boolean order) {

    }

    /**
     * 段内递归 排序
     * 段数 >= 线程数； 一个段只能由单个线程递归处理
     */
    private void sortSegment(int[] array, int start, int end, boolean order) {
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

    /**
     * 判断当前线程是否完成
     * 如果当前段长度已经大于数组长度 ，说明已经完成
     */
    public boolean isComplete() {
        if (this.isRecursion) {
            //段内递归完成 需要取消递归状态
            if (this.t_segment == 1) {
                this.isRecursion = false;
                this.segment <<= 1;
                this.t_segment = segment;
                this.t_order = true;
            }
        } else {
            if (this.segment > this.array.length) {
                return true;
            }
        }
        return false;
    }
}
