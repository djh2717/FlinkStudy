import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

class Solution {

    public static void main(String[] args) {
//        int[] ints = new int[]{2, 3, 4, 5, 6, 7, 84, 1, 22, 3, 77, 442, 1, 99, 112, 908, 56};
//        quickSort(ints, 0, ints.length - 1);
//
//        System.out.println(Arrays.toString(ints));

    }


    public List<Integer> minSubsequence(int[] nums) {
        int result = 0;
        for (int num : nums) {
            result += num;
        }
        result = result / 2;

        quickSort(nums, 0, nums.length - 1);
        ArrayList<Integer> integers = new ArrayList<>();
        int all = 0;
        for (int num : nums) {
            all += num;
            integers.add(num);
            if (all > result) {
                break;
            }
        }

        return integers;
    }


    private static void quickSort(int[] nums, int l, int r) {
        if (l > r) {
            return;
        }

        int left = l;
        int right = r;
        int base = nums[left];

        while (left != right) {

            while (left < right && nums[right] <= base) {
                right--;
            }

            while (left < right && nums[left] >= base) {
                left++;
            }


            if (left < right) {
                int temp = nums[left];
                nums[left] = nums[right];
                nums[right] = temp;
            }
        }

        int temp = nums[left];
        nums[left] = nums[l];
        nums[l] = temp;

        quickSort(nums, l, left - 1);
        quickSort(nums, left + 1, r);
    }
}