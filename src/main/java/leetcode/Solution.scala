package leetcode

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * @author djh on  2021/4/19 15:03
 * @E-Mail 1544579459@qq.com
 */

object Solution {
    def sortArrayByParityII(nums: Array[Int]): Array[Int] = {
        val oddNumber = new ArrayBuffer[Int]()
        val even = new ArrayBuffer[Int]()

        for (i <- nums.indices) {
            if ((i % 2 == 0 && nums(i) % 2 != 0) || (i % 2 != 0 && nums(i) % 2 == 0)) {
                if (nums(i) % 2 == 0) {
                    even.+=(nums(i))
                } else {
                    oddNumber.+=(nums(i))
                }
                nums(i) = -1
            }
        }
        for (i <- nums.indices) {
            if (nums(i) == -1) {
                if (i % 2 == 0) {
                    nums(i) = even(0)
                    even.remove(0)
                } else {
                    nums(i) = oddNumber(0)
                    oddNumber.remove(0)
                }
            }
        }
        nums
    }
}