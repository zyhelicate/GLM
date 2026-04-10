// SPDX-License-Identifier: MIT
#ifndef PLR_MODULE_H
#define PLR_MODULE_H

#include <stddef.h>
#include <stdint.h>

typedef struct plr_context plr_context_t;

typedef struct {
    int enabled;
    int parity_count;
    size_t parity_bytes;          // 大小同一个主奇偶块
    size_t reserved_bytes;        // 每个奇偶块预留空间大小
    double ewma_alpha;            // EWMA 平滑因子 (0,1]
    long merge_interval_updates;  // 触发后台合并的更新次数阈值
    long shrink_interval_updates; // 触发收缩检查的更新次数
    double expand_util_threshold; // 利用率高于该值时扩容
    double shrink_util_threshold; // 利用率低于该值时收缩
    const char *base_dir;         // 存储目录
    const char *file_prefix;      // 文件名前缀
    int read_only;                // 只读模式（禁用写入）
    int verbose;                  // 输出详细日志
} plr_config_t;

typedef struct {
    long stripe_index;
    size_t payload_bytes;
    size_t logical_offset; // 针对条带内的位置（可选）
} plr_delta_descriptor_t;

// PLR delta header 结构体（用于日志条目）
typedef struct __attribute__((packed)) {
    uint64_t magic;
    uint64_t stripe_index;
    uint64_t logical_offset;
    uint32_t payload_bytes;
    uint32_t crc32;
} plr_delta_header_t;

int plr_context_create(const plr_config_t *cfg, plr_context_t **out_ctx);
void plr_context_destroy(plr_context_t *ctx);

int plr_flush_metadata(plr_context_t *ctx);

int plr_log_delta(plr_context_t *ctx,
                  int parity_idx,
                  const plr_delta_descriptor_t *desc,
                  const void *delta_buf,
                  size_t delta_len);

int plr_read_effective_parity(plr_context_t *ctx,
                              int parity_idx,
                              void *parity_buffer,
                              size_t parity_len);

int plr_run_merge(plr_context_t *ctx, int parity_idx, int force_merge);

int plr_background_maintenance(plr_context_t *ctx);

// 启动后台合并线程（借鉴 RS manager 的实现）
// k_value: k 值，用于自适应调整合并阈值
int plr_start_merge_thread(plr_context_t *ctx, int k_value);

// 设置日志分片策略（PLR日志分片）
// alloc_strategy: 分配策略类型
// total_disks: 总磁盘数
int plr_set_log_striping(plr_context_t *ctx, int alloc_strategy, int total_disks);

size_t plr_reserved_used_bytes(plr_context_t *ctx, int parity_idx);

double plr_reserved_util(plr_context_t *ctx, int parity_idx);

// 异步 I/O 支持（需要 io_uring）
#ifdef __linux__
#include <liburing.h>
// 准备 PLR 增量写操作（不执行，返回需要写入的数据和偏移量）
int plr_prepare_delta_write(plr_context_t *ctx,
                            int parity_idx,
                            const plr_delta_descriptor_t *desc,
                            const void *delta_buf,
                            size_t delta_len,
                            void **header_buf_out,
                            size_t *header_len_out,
                            void **data_buf_out,
                            size_t *data_len_out,
                            off_t *write_offset_out);
// 提交 PLR 增量写操作到 io_uring
// 返回 0 表示成功，write_offset_out 返回写入偏移量
int plr_submit_delta_write(plr_context_t *ctx,
                           int parity_idx,
                           struct io_uring *ring,
                           const plr_delta_descriptor_t *desc,
                           const void *delta_buf,
                           size_t delta_len,
                           off_t *write_offset_out);
// 完成 PLR 增量写操作（更新元数据）
int plr_complete_delta_write(plr_context_t *ctx,
                             int parity_idx,
                             const plr_delta_descriptor_t *desc,
                             size_t delta_len,
                             off_t write_offset);
// 释放 PLR header 缓冲区（用于 CQE 完成回调）
void plr_free_header_buffer(plr_delta_header_t *h);
// 释放 PLR header context 缓冲区（用于 CQE 完成回调）
void plr_free_header_ctx_buffer(void *ctx);
#endif

#endif // PLR_MODULE_H


