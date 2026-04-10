#ifndef ALLOC_STRATEGY_H
#define ALLOC_STRATEGY_H

#include <stddef.h>

// p = k - 1 是行数/对角线模数的统一常量
typedef struct {
    int row;   // 行索引 (0..p-1), p = k-1
    int col;   // 磁盘索引 (0..k-1), 数据列限 (0..k-1), col=k 用于RS预留
    int diag;  // 对角线索引 (0..p-1), 避开 diag==0 以优化校验更新
} BlockPos;

/* OAA 批处理请求结构：按物理地址排序以减少磁盘寻道（Seek） */
typedef struct {
    int logical_col;
    int logical_row;
    void *new_data;             /* 新数据指针，可为 NULL */
    long long physical_address_key;  /* 用于 qsort：如 (col*(w+1)+row)，模拟物理相邻 */
} UpdateRequest;

/* 用于 qsort 的请求比较函数（按物理地址键排序，实现 OAA） */
int compare_requests(const void *a, const void *b);

/* 按列主序初始化物理地址键：physical_address_key = col * rows + row */
void init_phys_addr(UpdateRequest *reqs, int num_reqs, int rows);

int map_blocks_row(int s, int k, int w, BlockPos *blocks);
int map_blocks_diag(int s, int k, int w, BlockPos *blocks);
int choose_best_mapping(int s, int k, int w, BlockPos *blocks);
int map_blocks_row_optimized(int s, int k, int w, BlockPos *blocks);
int map_blocks_diag_fixed(int s, int k, int w, int target_diag, BlockPos *blocks);
int map_blocks_diag_multi_nonzero(int s, int k, int w, BlockPos *blocks);
int choose_best_mapping_enhanced(int s, int k, int w, BlockPos *blocks);
int map_blocks_rs_friendly(int s, int k, int w, BlockPos *blocks);
int map_blocks_batch_aware(int s, int k, int w, int batch_size, BlockPos *blocks);
int map_blocks_rs_load_aware(int s, int k, int w, double current_rs_load, BlockPos *blocks);
int map_blocks_rs_ultra_compact(int s, int k, int w, int max_stripes_per_batch, BlockPos *blocks);
int map_blocks_stripe_hotspot(int s, int k, int w, int hotspot_stripe_id, BlockPos *blocks);

// 新增：统一候选入口函数
int map_blocks_diag_set(int s, int k, int p, const int *diag_set, int n_diags, BlockPos *blocks);
double calculate_enhanced_cost(BlockPos *blocks, int s, int k, int p);
int map_blocks_row_optimized_v2(int s, int k, int p, BlockPos *blocks);
int choose_best_mapping_unified(int s, int k, int p, BlockPos *blocks);

#endif /* ALLOC_STRATEGY_H */
