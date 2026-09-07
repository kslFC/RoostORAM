package com.ringoram;

/**
 * Two_random配列をrandom値の昇順に並べる簡易QuickSort。
 *
 * MathUtility.get_random_permutation()でランダム順列を作るために使う。
 */
public class QuickSort {
	public void quickSorting(Two_random[] arr){
	    // 配列全体を対象にQuickSortを開始する。
	    qsort(arr, 0, arr.length-1);
	}
	private void qsort(Two_random[] arr, int low, int high){
	    if (low < high){
	        int pivot=partition(arr, low, high);    
	        qsort(arr, low, pivot-1);                   
	        qsort(arr, pivot+1, high);                
	    }
	}
	private int partition(Two_random[] arr, int low, int high){
	    // 先頭要素をpivotにし、pivot未満/以上で左右に分ける。
	    Two_random pivot = arr[low];    
	    while (low<high){
	        while (low<high && arr[high].random>=pivot.random) --high;
	        arr[low]=arr[high];           
	        while (low<high && arr[low].random<=pivot.random) ++low;
	        arr[high] = arr[low];          
	    }
	    arr[low] = pivot;
	    return low;
	}
}
