import java.util.ArrayList;

/**
 * 双调排序，时间复杂度 O(N*logN*logN)，适合GPU并行计算
 * 本双调排序只适用于待排序数组长度为2的整数次幂，如果不满足，则默认填充Integer.MAX_VALUE直至长度满足2的整数次幂
 * 返回结果时，再去除填充值
 *
 * @program:
 * @description: 双调排序
 * @author: Ya
 * @create: 2019-11-13 17:22
 **/
public class BitonicSort {
    /**
     * 进行双调排序
     *
     * @param array
     */
    public static void bitonicSort(int[] array) {
        if (array == null || array.length <= 1) {
            return;
        }
        //双调排序前提是数组长度为 2的整数次幂,如果不是需要padding填充
        int[] initArray = padding(array, Integer.MAX_VALUE);
        //开始对数组进行双调排序
        startBitonicSort(initArray);
        System.arraycopy(initArray, 0, array, 0, array.length);
    }

    /**
     * 开始双调排序
     *
     * @param initArray
     */
    private static void startBitonicSort(int[] initArray) {
        //int segmentNum = initArray.length >> 1;
        for (int segment = 2; segment < initArray.length; segment <<= 1) {
            sort(initArray, segment);
        }
        //最终排序，双调-->单调
        sortSegment(initArray, 0, initArray.length, true);
    }

    /**
     * 进行分段排序
     * 奇数段递增，偶数段递减
     */
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
     * 段内递归 排序
     */
    private static void sortSegment(int[] array, int start, int end, boolean order) {
        int mid = (start + end) >> 1;
        if (start == mid) {
            return;
        }
        // 没用
        int a = 1;
        for (int i = 0; start + i < mid; i++) {
            swap(array, start + i, mid + i, order);
        }
        sortSegment(array, start, mid, order);
        sortSegment(array, mid, end, order);
    }


    /**
     * 双调排序要求数组长度为2的整数次幂
     * 如果不满足，则需要进行填充，直至长度满足2的整数次幂；
     * <p>
     * 尾部填充数组,填充值为paddingNum，直到长度为2的整数次幂
     * 返回填充后的数组
     *
     * @param array      待排序数组
     * @param paddingNum 填充值，默认Integer.MAX_VALUE
     * @return 返回填充后的数组
     */
    private static int[] padding(int[] array, int paddingNum) {
        int length = array.length;
        int adjustSize = 1 << 1;
        int paddingLength;
        while (adjustSize < length) {
            adjustSize <<= 1;
        }
        paddingLength = adjustSize - length;
        int[] paddingArray = new int[paddingLength];
        for (int i = 0; i < paddingLength; i++) {
            paddingArray[i] = paddingNum;
        }
        int[] mergeArray = new int[adjustSize];
        System.arraycopy(array, 0, mergeArray, 0, length);
        System.arraycopy(paddingArray, 0, mergeArray, length, paddingLength);
        return mergeArray;
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

    private static void swap(int[] array, int i, int j, boolean order) {
        if (order && array[i] > array[j]) {
            swap(array, i, j);
        } else if (!order && array[i] < array[j]) {
            swap(array, i, j);
        }
    }
}
