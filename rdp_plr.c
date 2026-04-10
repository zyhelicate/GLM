#define _GNU_SOURCE
#define _POSIX_C_SOURCE 199309L
#define PLR_USER_DATA_THRESHOLD 10000000

// ========== 内存分配错误处理宏 ==========
#define RDP_SAFE_ALLOC(ptr, size, label, cleanup_label) do { \
    (ptr) = malloc(size); \
    if (!(ptr)) { \
        fprintf(stderr, "[ERROR] Failed to allocate %zu bytes at %s:%d\n", \
                (size_t)(size), __FILE__, __LINE__); \
        goto cleanup_label; \
    } \
} while(0)

#define RDP_SAFE_CALLOC(ptr, nmemb, size, cleanup_label) do { \
    (ptr) = calloc((nmemb), (size)); \
    if (!(ptr)) { \
        fprintf(stderr, "[ERROR] Failed to calloc %zu elements of %zu bytes at %s:%d\n", \
                (size_t)(nmemb), (size_t)(size), __FILE__, __LINE__); \
        goto cleanup_label; \
    } \
} while(0)

#define RDP_SAFE_POSIX_MEMALIGN(ptr, alignment, size, cleanup_label) do { \
    int _ret = posix_memalign((void**)(ptr), (alignment), (size)); \
    if (_ret != 0) { \
        fprintf(stderr, "[ERROR] Failed to allocate aligned memory (%zu bytes, alignment=%d): %s at %s:%d\n", \
                (size_t)(size), (alignment), strerror(_ret), __FILE__, __LINE__); \
        goto cleanup_label; \
    } \
} while(0)

#define RDP_SAFE_REALLOC(ptr, size, cleanup_label) do { \
    void *_new_ptr = realloc((ptr), (size)); \
    if (!_new_ptr) { \
        fprintf(stderr, "[ERROR] Failed to realloc %zu bytes at %s:%d\n", \
                (size_t)(size), __FILE__, __LINE__); \
        goto cleanup_label; \
    } \
    (ptr) = _new_ptr; \
} while(0)

#define RDP_FREE_AND_NULL(ptr) do { \
    if (ptr) { \
        free((void*)(ptr)); \
        (ptr) = NULL; \
    } \
} while(0)

// ========== 并发安全性优化：原子操作宏 ==========
#ifdef __STDC_NO_ATOMICS__
    // 不支持原子操作，回退到互斥锁
    #include <stddef.h>
    #define RDP_ATOMIC_INIT(var, val) do { (var) = (val); } while(0)
    #define RDP_ATOMIC_LOAD(var) (var)
    #define RDP_ATOMIC_STORE(var, val) do { (var) = (val); } while(0)
    #define RDP_ATOMIC_FETCH_ADD(var, delta) (__atomic_fetch_add(&(var), (delta), __ATOMIC_SEQ_CST))
    #define RDP_ATOMIC_FETCH_SUB(var, delta) (__atomic_fetch_sub(&(var), (delta), __ATOMIC_SEQ_CST))
    #define RDP_ATOMIC_INC(var) RDP_ATOMIC_FETCH_ADD(var, 1)
    #define RDP_ATOMIC_DEC(var) RDP_ATOMIC_FETCH_SUB(var, 1)
    #define RDP_ATOMIC_COMPARE_EXCHANGE(var, expected, desired) \
        __atomic_compare_exchange_n(&(var), &(expected), (desired), 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)
#else
    #include <stdatomic.h>
    #include <stddef.h>
    #define RDP_ATOMIC_INIT(var, val) atomic_store(&(var), (val))
    #define RDP_ATOMIC_LOAD(var) atomic_load(&(var))
    #define RDP_ATOMIC_STORE(var, val) atomic_store(&(var), (val))
    #define RDP_ATOMIC_FETCH_ADD(var, delta) atomic_fetch_add(&(var), (delta))
    #define RDP_ATOMIC_FETCH_SUB(var, delta) atomic_fetch_sub(&(var), (delta))
    #define RDP_ATOMIC_INC(var) atomic_fetch_add(&(var), 1)
    #define RDP_ATOMIC_DEC(var) atomic_fetch_sub(&(var), 1)
    #define RDP_ATOMIC_COMPARE_EXCHANGE(var, expected, desired) \
        atomic_compare_exchange_strong(&(var), &(expected), (desired))
#endif

// 原子类型简化宏
#define RDP_ATOMIC_INT atomic_int
#define RDP_ATOMIC_LONG atomic_long
#define RDP_ATOMIC_SIZE atomic_size_t
#define RDP_ATOMIC_BOOL atomic_bool

// ========== 原子操作辅助函数 ==========

// 安全的原子递增并返回值
static inline int rdp_atomic_inc_return(RDP_ATOMIC_INT *var) {
    return (int)atomic_fetch_add(var, 1) + 1;
}

// 安全的原子递减并返回值
static inline int rdp_atomic_dec_return(RDP_ATOMIC_INT *var) {
    return (int)atomic_fetch_sub(var, 1) - 1;
}

// 安全的原子增加并返回值
static inline size_t rdp_atomic_add_return(RDP_ATOMIC_SIZE *var, size_t delta) {
    return (size_t)atomic_fetch_add(var, delta) + delta;
}

// 安全的原子减少并返回值
static inline size_t rdp_atomic_sub_return(RDP_ATOMIC_SIZE *var, size_t delta) {
    return (size_t)atomic_fetch_sub(var, delta) - delta;
}

// 原子比较交换
static inline int rdp_atomic_compare_exchange(RDP_ATOMIC_INT *var, int expected, int desired) {
    int exp = expected;
    return atomic_compare_exchange_strong(var, &exp, desired);
}

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <getopt.h>
#include <pthread.h>
#include <immintrin.h>
#include <stdint.h>
#include <stdarg.h>
#include <math.h>
#include <liburing.h>
#include <sys/types.h>
#include <sys/sysmacros.h>
#include <alloca.h>
#include <sys/uio.h>
#include <sys/mman.h>
#include "alloc_strategy.h"
// === 自动注入的底层物理对齐补丁 (OAA) ===
typedef struct {
    int logical_col;
    int logical_row;
    long long physical_address_key;
} OAAUpdateRequest_t;
static int compare_oaa_requests(const void *a, const void *b) {
    const OAAUpdateRequest_t *ra = (const OAAUpdateRequest_t *)a;
    const OAAUpdateRequest_t *rb = (const OAAUpdateRequest_t *)b;
    if (ra->physical_address_key < rb->physical_address_key) return -1;
    if (ra->physical_address_key > rb->physical_address_key) return 1;
    return 0;
}
static void reorder_blocks_by_physical_addr(BlockPos *blocks, int count, int total_rows) {
    if (!blocks || count <= 0 || total_rows <= 0) return;
    OAAUpdateRequest_t *reqs = (OAAUpdateRequest_t*)malloc(sizeof(OAAUpdateRequest_t) * count);
    if (!reqs) return;
    for (int i = 0; i < count; i++) {
        reqs[i].logical_col = blocks[i].col;
        reqs[i].logical_row = blocks[i].row;
        reqs[i].physical_address_key = (long long)blocks[i].col * total_rows + blocks[i].row;
    }
    qsort(reqs, count, sizeof(OAAUpdateRequest_t), compare_oaa_requests);
    for (int i = 0; i < count; i++) {
        blocks[i].col = reqs[i].logical_col;
        blocks[i].row = reqs[i].logical_row;
    }
    free(reqs);
}
// =========================================
#include "parix_module.h"
#include "plr_module.h"

// 部分平台未暴露 O_DIRECT / O_CLOEXEC，需要提供兼容定义以通过编译
#ifndef O_DIRECT
#define O_DIRECT 0
#endif
#ifndef O_CLOEXEC
#define O_CLOEXEC 0
#endif

// 如果未链接真实的 PARIX 实现，提供弱符号存根以避免链接错误。
// 存根将使 PARIX 功能保持禁用状态。
#ifndef PARIX_STUB_PROVIDED
#define PARIX_STUB_PROVIDED 1
__attribute__((weak)) parix_local_ctx_t* parix_local_init(const char *dir, int k, int w, size_t packetsize, parix_mode_t mode) {
    (void)dir; (void)k; (void)w; (void)packetsize; (void)mode; return NULL;
}
__attribute__((weak)) int parix_local_submit(parix_local_ctx_t *ctx, uint32_t stripe, const BlockPos *plan, int plan_cnt, const unsigned char *payload, size_t packetsize) {
    (void)ctx; (void)stripe; (void)plan; (void)plan_cnt; (void)payload; (void)packetsize; return 0;
}
__attribute__((weak)) int parix_local_replay(parix_local_ctx_t *ctx) {
    (void)ctx; return 0;
}
__attribute__((weak)) void parix_local_shutdown(parix_local_ctx_t *ctx) {
    (void)ctx;
}
#endif

// RDP 版本 - 使用 Row-Diagonal Parity 编码，复用通用 I/O 与分配框架

// ========== 地址分配策略辅助函数（依赖config全局变量） ==========

// 地址分配辅助函数声明（这些函数依赖config，保留在evenodd.c中）
static void fill_sequential_plan(BlockPos *blocks, int count);
static int plan_block_positions(int count, BlockPos *blocks, int prefer_rs_layout, const char **plan_used);

// 前向声明（避免隐式声明带来的冲突与警告）
void xor_update_simd(char *dst, const char *src, size_t size);
static uint32_t crc32(uint32_t crc, const unsigned char *buf, size_t len);

// 配置结构
typedef struct {
    int k;              // 数据盘数量
    int m;              // 校验盘数量 (RDP 固定为 2)
    int w;              // 行数 / 编码参数
    int p_prime;        // RDP 的素数 p（p > k, w = p - 1）
    int packetsize;     // 数据包大小
    int update_size;    // 更新大小
    int n_updates;      // 更新次数
    char *mode;         // 更新模式
    char *alloc;        // 地址分配策略: sequential|row|diag|auto
    int verify;         // 是否进行一致性校验
    long verify_samples;// 校验采样条带数 (<=0 表示全量)
    int strong;         // 强一致路径：批次写完成后重算条带 P/Q 并写回
    int parix_enabled;  // 是否启用 PARIX
    int parix_use_alloc;// 是否与地址分配策略结合
    char *parix_dir;    // PARIX 本地目录

    // PLR 配置
    int plr_enabled;            // 是否启用 PLR
    char *plr_dir;              // PLR 存储目录
    size_t plr_reserved_bytes;  // 每个奇偶块预留空间
    double plr_alpha;           // EWMA 参数
    long plr_merge_interval;    // 合并间隔（更新次数）
    long plr_shrink_interval;   // 收缩检查间隔
    double plr_expand_util;     // 扩容阈值
    double plr_shrink_util;     // 收缩阈值
    
    // 异步流水线配置
    int queue_depth;            // 并发上下文数量（流水线深度）
    int use_sqpoll;            // 是否启用 SQPOLL（内核轮询）
} config_t;


// 增强版性能统计 - 支持详细指标收集
typedef struct {
    double total_io_time;
    double compute_time;
    int read_count;
    int write_count;
    double total_latency;
    int update_count;
    double *io_times;
    int io_index;
    int io_capacity;
    long long xor_count;
    pthread_mutex_t lock;
    
    // 新增：详细性能指标
    struct {
        double throughput_mbps;        // 吞吐量 (MB/s)
        double iops;                   // IOPS
        double avg_latency_ms;         // 平均延迟 (ms)
        double p95_latency_ms;         // 95%延迟 (ms)
        double p99_latency_ms;         // 99%延迟 (ms)
        double cpu_usage_percent;      // CPU使用率
        double memory_usage_mb;        // 内存使用量 (MB)
        long long cache_hits;          // 缓存命中次数
        long long cache_misses;        // 缓存未命中次数
        double simd_efficiency;        // SIMD效率
        double io_efficiency;         // I/O效率
        double parity_efficiency;      // 校验计算效率
    } detailed_stats;
    
    // 新增：实时监控
    struct {
        double current_throughput;     // 当前吞吐量
        double current_latency;        // 当前延迟
        int active_operations;         // 活跃操作数
        int queue_depth;               // 队列深度
        double load_factor;            // 负载因子
    } realtime_stats;
    
    // 新增：历史数据
    struct {
        double *throughput_history;    // 吞吐量历史
        double *latency_history;       // 延迟历史
        int history_size;              // 历史数据大小
        int history_index;             // 当前索引
    } history;
} perf_stats_t;

// 增强版内存池 - 支持多种大小和NUMA感知
typedef struct memory_pool {
    void **buffers;
    int *free_list;
    int free_count;
    int capacity;
    size_t buffer_size;
    pthread_mutex_t lock;
    
    // 新增：NUMA感知和预分配
    int numa_node;              // NUMA节点
    int prealloc_count;         // 预分配数量
    int alignment;              // 内存对齐要求
    uint64_t total_allocated;   // 总分配字节数
    uint64_t peak_usage;       // 峰值使用量
    struct timespec last_cleanup; // 上次清理时间
} memory_pool_t;

// Forward declarations to avoid implicit declaration warnings when used above
void* pool_alloc(memory_pool_t *pool);
void  pool_free(memory_pool_t *pool, void *buffer);

// 多级内存池管理器
typedef struct {
    memory_pool_t *pools;        // 不同大小的内存池数组
    int pool_count;             // 池数量
    size_t *pool_sizes;         // 各池的缓冲区大小
    int *pool_capacities;       // 各池的容量
    pthread_mutex_t global_lock; // 全局锁
    int numa_aware;             // 是否启用NUMA感知
} multi_pool_manager_t;


// 全局变量
config_t config = {
    .k = 4,
    .m = 2,
    .w = 4,
    .p_prime = 5,
    .packetsize = 4096,
    .update_size = 4096,
    .n_updates = 1000,
    .mode = "sequential",
    .alloc = "sequential",
    .verify = 0,
    .verify_samples = 0,
    .strong = 0,
    .parix_enabled = 0,
    .parix_use_alloc = 0,
    .parix_dir = NULL,
    .plr_enabled = 0,
    .plr_dir = NULL,
    .plr_reserved_bytes = 0,
    .plr_alpha = 0.25,
    .plr_merge_interval = 500,
    .plr_shrink_interval = 2000,
    .plr_expand_util = 0.85,
    .plr_shrink_util = 0.2,
    .queue_depth = 64,        // 默认并发上下文数量
    .use_sqpoll = 0            // 默认不启用 SQPOLL（需要 root 权限）
};

perf_stats_t stats = {0};
memory_pool_t *global_pool = NULL;

// 记录本次更新触及的条带集合（用于最终轻量修复）
static long *g_touched_stripes = NULL;
static RDP_ATOMIC_INT g_touched_n = ATOMIC_VAR_INIT(0);  // 原子计数器
static int g_touched_cap = 0;
static pthread_mutex_t g_touched_lock = PTHREAD_MUTEX_INITIALIZER;  // 互斥锁保护

static void touched_add(long s) {
    pthread_mutex_lock(&g_touched_lock);
    
    // 检查是否已存在
    int n = RDP_ATOMIC_LOAD(g_touched_n);
    for (int i = 0; i < n; i++) {
        if (g_touched_stripes[i] == s) {
            pthread_mutex_unlock(&g_touched_lock);
            return;
        }
    }
    
    // 需要扩容
    if (n == g_touched_cap) {
        int nc = g_touched_cap ? g_touched_cap * 2 : 256;
        long *np = (long*)realloc(g_touched_stripes, sizeof(long) * (size_t)nc);
        if (!np) {
            pthread_mutex_unlock(&g_touched_lock);
            return;
        }
        g_touched_stripes = np;
        g_touched_cap = nc;
    }
    
    // 添加新条目
    g_touched_stripes[n] = s;
    RDP_ATOMIC_STORE(g_touched_n, n + 1);
    
    pthread_mutex_unlock(&g_touched_lock);
}

static void touched_clear(void) {
    pthread_mutex_lock(&g_touched_lock);
    
    if (g_touched_stripes) {
        free(g_touched_stripes);
        g_touched_stripes = NULL;
    }
    RDP_ATOMIC_STORE(g_touched_n, 0);
    g_touched_cap = 0;
    
    pthread_mutex_unlock(&g_touched_lock);
}

static int buffer_is_zero(const char *buf, size_t len) {
    if (!buf) return 1;
    const uint64_t *w = (const uint64_t*)buf;
    size_t words = len / sizeof(uint64_t);
    for (size_t i = 0; i < words; i++) {
        if (w[i] != 0) return 0;
    }
    const unsigned char *c = (const unsigned char*)(buf + words * sizeof(uint64_t));
    for (size_t i = words * sizeof(uint64_t); i < len; i++) {
        if (c[i - words * sizeof(uint64_t)] != 0) return 0;
    }
    return 1;
}

// 新增：固定文件句柄支持
typedef struct {
    int enabled;       // 是否启用固定文件
    int *handles;      // 每个磁盘的句柄：若启用固定文件，则为注册索引；否则为普通 fd
} fixed_files_t;
static fixed_files_t g_fixed = {0, NULL};

// io_uring 全局状态
static struct io_uring g_ring;
static int g_ring_ready = 0;
static int *g_fds = NULL;      // 注册的磁盘文件描述符
static int g_total_disks = 0;  // 当前打开的磁盘数量
static RDP_ATOMIC_SIZE g_pending_ops = ATOMIC_VAR_INIT(0);   // 当前队列中待完成的提交数（原子操作）
static int g_ring_queue_depth = 0; // io_uring 队列深度，动态设定
static parix_local_ctx_t *g_parix_ctx = NULL;
static parix_mode_t g_parix_mode = PARIX_MODE_BASIC;
static plr_context_t *g_plr_ctx = NULL; // PLR 上下文

// ========== 异步流水线架构：Context Pool ==========
typedef enum {
    CTX_FREE = 0,
    CTX_READING,
    CTX_WRITING
} ctx_state_t;

// 每个并发请求的上下文
typedef struct {
    int id;
    ctx_state_t state;
    long stripe_num;
    int pending_ios;           // 剩余未完成IO数
    int pending_plr_headers;    // PLR header 写入数量（需要单独计数）
    
    // 数据缓冲区（预分配，对齐到4096）
    char *payload;             // 新数据
    char *old_data;            // 旧数据 (Read buffer)
    char *row_delta;           // PLR Row Delta
    char *diag_delta;          // PLR Diag Delta
    char *xor_tmp;             // 临时 XOR 计算用
    
    // 元数据
    unsigned char *row_mask;
    unsigned char *diag_mask;
    BlockPos *plan;            // 布局计划
    int plan_count;             // 计划中的块数量
    
    // 用于跟踪 PLR header 指针（需要释放）
    void **plr_header_ptrs;    // PLR header 指针数组
    int plr_header_count;      // PLR header 数量
    int plr_header_capacity;   // 数组容量
} update_ctx_t;

static update_ctx_t *g_ctx_pool = NULL;
static int *g_ctx_free_list = NULL;
static int g_ctx_free_head = 0;
static int g_ctx_pool_size = 0;
static pthread_mutex_t g_ctx_pool_lock = PTHREAD_MUTEX_INITIALIZER;

// 流水线统计
static RDP_ATOMIC_SIZE g_completed_updates = ATOMIC_VAR_INIT(0);  // 已完成的更新数（原子操作）
static RDP_ATOMIC_INT g_inflight_updates = ATOMIC_VAR_INIT(0);    // 进行中的更新数（原子操作）

// 前向声明（避免隐式声明导致警告）
static int load_safe_disk_paths(char **disk_paths, int max_disks);
static int ensure_nonstdio_fd(int fd, const char *who);
static int open_and_register_disks(int k, int m);
static void close_all_disks(void);

static inline off_t block_offset_bytes(long stripe_idx, int row, int packetsize) {
    return (off_t)((stripe_idx * (long)config.w + row) * (long)packetsize);
}

static inline int device_index_for_col(int col, int k, int m) {
    (void)k; (void)m; return col;
}

// ================= 核心优化：强力地址映射算法 =================
// 使用大素数 997 进行散列，确保相邻 Stripe 的物理落盘位置完全打散
// Sequential: 校验盘集中在最后两块物理盘 → 制造热点瓶颈
// 非 Sequential: 素数哈希将校验写均匀分摊到所有盘 → 负载均衡
static int map_physical_disk(int logical_col, long stripe_num, int total_disks, const char *strategy) {
    if (!strategy || strcmp(strategy, "sequential") == 0) {
        return logical_col;
    } else {
        // [Opt] 使用强力素数乘法哈希，确保相邻 Stripe 的 Parity 落点完全不同
        // 使用更大的素数（1009）和双重哈希，最大程度利用所有磁盘的队列深度
        // 这样可以避免相邻stripe的校验盘落在同一物理盘上
        unsigned long hash1 = (unsigned long)logical_col + (unsigned long)stripe_num * 1009UL;
        unsigned long hash2 = (unsigned long)logical_col * 1013UL + (unsigned long)stripe_num;
        unsigned long hash = hash1 ^ (hash2 << 16) ^ (hash2 >> 16);
        return (int)(hash % (unsigned long)total_disks);
    }
}

// 新增：固定缓冲池（第一步：注册固定缓冲并用 read_fixed/write_fixed）
typedef struct {
    int enabled;
    struct iovec *iovecs;
    void **ptrs;
    int capacity;
    int *free_stack;
    int top;
    pthread_mutex_t lock;
} fixed_bufpool_t;
static fixed_bufpool_t g_bufpool = {0};

static int bufpool_init(struct io_uring *ring, int capacity, size_t buf_size) {
    memset(&g_bufpool, 0, sizeof(g_bufpool));
    g_bufpool.capacity = capacity;
    g_bufpool.iovecs = (struct iovec*)malloc(sizeof(struct iovec) * (size_t)capacity);
    g_bufpool.ptrs = (void**)malloc(sizeof(void*) * (size_t)capacity);
    g_bufpool.free_stack = (int*)malloc(sizeof(int) * (size_t)capacity);
    if (!g_bufpool.iovecs || !g_bufpool.ptrs || !g_bufpool.free_stack) {
        fprintf(stderr, "[BUFPOOL] Failed to allocate arrays (capacity=%d)\n", capacity);
        if (g_bufpool.iovecs) free(g_bufpool.iovecs);
        if (g_bufpool.ptrs) free(g_bufpool.ptrs);
        if (g_bufpool.free_stack) free(g_bufpool.free_stack);
        g_bufpool.iovecs = NULL;
        g_bufpool.ptrs = NULL;
        g_bufpool.free_stack = NULL;
        g_bufpool.capacity = 0;
        return -1;
    }
    
    for (int i = 0; i < capacity; i++) {
        void *p = NULL;
        if (posix_memalign(&p, 4096, buf_size) != 0) {
            fprintf(stderr, "[BUFPOOL] Failed to allocate buffer %d (%zu bytes)\n", i, buf_size);
            // 清理已分配的缓冲区
            for (int j = 0; j < i; j++) {
                free(g_bufpool.ptrs[j]);
            }
            free(g_bufpool.iovecs);
            free(g_bufpool.ptrs);
            free(g_bufpool.free_stack);
            g_bufpool.iovecs = NULL;
            g_bufpool.ptrs = NULL;
            g_bufpool.free_stack = NULL;
            g_bufpool.capacity = 0;
            return -1;
        }
        g_bufpool.ptrs[i] = p;
        g_bufpool.iovecs[i].iov_base = p;
        g_bufpool.iovecs[i].iov_len = buf_size;
        g_bufpool.free_stack[i] = i;
    }
    g_bufpool.top = capacity;
    pthread_mutex_init(&g_bufpool.lock, NULL);
    if (io_uring_register_buffers(ring, g_bufpool.iovecs, capacity) == 0) {
        g_bufpool.enabled = 1;
        return 0;
    }
    g_bufpool.enabled = 0;
    return 0;
}

static int bufpool_acquire(void **ptr_out) {
    pthread_mutex_lock(&g_bufpool.lock);
    if (g_bufpool.top == 0) {
        pthread_mutex_unlock(&g_bufpool.lock);
        return -1;
    }
    int idx = g_bufpool.free_stack[--g_bufpool.top];
    void *p = g_bufpool.ptrs[idx];
    pthread_mutex_unlock(&g_bufpool.lock);
    *ptr_out = p;
    return idx;
}

static void bufpool_release(int idx) {
    if (idx < 0 || idx >= g_bufpool.capacity) return;
    pthread_mutex_lock(&g_bufpool.lock);
    g_bufpool.free_stack[g_bufpool.top++] = idx;
    pthread_mutex_unlock(&g_bufpool.lock);
}

static void bufpool_destroy(void) {
    if (g_bufpool.iovecs) {
        for (int i = 0; i < g_bufpool.capacity; i++) {
            if (g_bufpool.ptrs && g_bufpool.ptrs[i]) free(g_bufpool.ptrs[i]);
        }
        free(g_bufpool.iovecs);
        free(g_bufpool.ptrs);
        free(g_bufpool.free_stack);
    }
    pthread_mutex_destroy(&g_bufpool.lock);
    memset(&g_bufpool, 0, sizeof(g_bufpool));
}

// ========== Context Pool 管理 ==========
static int ctx_pool_init(int pool_size) {
    if (pool_size <= 0) pool_size = 64; // 默认值
    
    g_ctx_pool = (update_ctx_t*)calloc((size_t)pool_size, sizeof(update_ctx_t));
    g_ctx_free_list = (int*)malloc(sizeof(int) * (size_t)pool_size);
    if (!g_ctx_pool || !g_ctx_free_list) {
        fprintf(stderr, "Failed to allocate context pool\n");
        if (g_ctx_pool) {
            free(g_ctx_pool);
            g_ctx_pool = NULL;
        }
        if (g_ctx_free_list) {
            free(g_ctx_free_list);
            g_ctx_free_list = NULL;
        }
        return -1;
    }
    
    g_ctx_pool_size = pool_size;
    g_ctx_free_head = pool_size;
    
    size_t blk_cnt = (size_t)(config.update_size / config.packetsize);
    size_t stripe_bytes = (size_t)config.w * (size_t)config.packetsize;
    
    for (int i = 0; i < pool_size; i++) {
        g_ctx_free_list[i] = i;
        update_ctx_t *ctx = &g_ctx_pool[i];
        ctx->id = i;
        ctx->state = CTX_FREE;
        ctx->pending_ios = 0;
        ctx->pending_plr_headers = 0;
        
        // 分配对齐内存（4096 字节对齐，用于 O_DIRECT）
        int alloc_failed = 0;
        if (posix_memalign((void**)&ctx->payload, 4096, (size_t)config.update_size) != 0) {
            fprintf(stderr, "Failed to allocate payload for context %d\n", i);
            alloc_failed = 1;
        } else if (posix_memalign((void**)&ctx->old_data, 4096, (size_t)config.update_size) != 0) {
            fprintf(stderr, "Failed to allocate old_data for context %d\n", i);
            free(ctx->payload);
            ctx->payload = NULL;
            alloc_failed = 1;
        } else if (posix_memalign((void**)&ctx->row_delta, 4096, stripe_bytes) != 0) {
            fprintf(stderr, "Failed to allocate row_delta for context %d\n", i);
            free(ctx->payload);
            free(ctx->old_data);
            ctx->payload = ctx->old_data = NULL;
            alloc_failed = 1;
        } else if (posix_memalign((void**)&ctx->diag_delta, 4096, stripe_bytes) != 0) {
            fprintf(stderr, "Failed to allocate diag_delta for context %d\n", i);
            free(ctx->payload);
            free(ctx->old_data);
            free(ctx->row_delta);
            ctx->payload = ctx->old_data = ctx->row_delta = NULL;
            alloc_failed = 1;
        } else if (posix_memalign((void**)&ctx->xor_tmp, 4096, (size_t)config.packetsize) != 0) {
            fprintf(stderr, "Failed to allocate xor_tmp for context %d\n", i);
            free(ctx->payload);
            free(ctx->old_data);
            free(ctx->row_delta);
            free(ctx->diag_delta);
            ctx->payload = ctx->old_data = ctx->row_delta = ctx->diag_delta = NULL;
            alloc_failed = 1;
        }
        
        if (alloc_failed) {
            // 清理之前已分配的所有上下文
            for (int j = 0; j < i; j++) {
                update_ctx_t *prev_ctx = &g_ctx_pool[j];
                if (prev_ctx->payload) free(prev_ctx->payload);
                if (prev_ctx->old_data) free(prev_ctx->old_data);
                if (prev_ctx->row_delta) free(prev_ctx->row_delta);
                if (prev_ctx->diag_delta) free(prev_ctx->diag_delta);
                if (prev_ctx->xor_tmp) free(prev_ctx->xor_tmp);
                if (prev_ctx->row_mask) free(prev_ctx->row_mask);
                if (prev_ctx->diag_mask) free(prev_ctx->diag_mask);
                if (prev_ctx->plan) free(prev_ctx->plan);
                if (prev_ctx->plr_header_ptrs) free(prev_ctx->plr_header_ptrs);
            }
            free(g_ctx_pool);
            g_ctx_pool = NULL;
            free(g_ctx_free_list);
            g_ctx_free_list = NULL;
            g_ctx_pool_size = 0;
            g_ctx_free_head = 0;
            return -1;
        }
        
        ctx->row_mask = (unsigned char*)calloc((size_t)config.w, 1);
        ctx->diag_mask = (unsigned char*)calloc((size_t)config.w, 1);
        ctx->plan = (BlockPos*)calloc(blk_cnt, sizeof(BlockPos));
        ctx->plan_count = (int)blk_cnt;
        
        ctx->plr_header_ptrs = NULL;
        ctx->plr_header_count = 0;
        ctx->plr_header_capacity = 0;
        
        if (!ctx->row_mask || !ctx->diag_mask || !ctx->plan) {
            fprintf(stderr, "Failed to allocate metadata for context %d\n", i);
            // 清理当前上下文已分配的资源
            free(ctx->payload);
            free(ctx->old_data);
            free(ctx->row_delta);
            free(ctx->diag_delta);
            free(ctx->xor_tmp);
            if (ctx->row_mask) free(ctx->row_mask);
            if (ctx->diag_mask) free(ctx->diag_mask);
            if (ctx->plan) free(ctx->plan);
            
            // 清理之前已分配的所有上下文
            for (int j = 0; j < i; j++) {
                update_ctx_t *prev_ctx = &g_ctx_pool[j];
                if (prev_ctx->payload) free(prev_ctx->payload);
                if (prev_ctx->old_data) free(prev_ctx->old_data);
                if (prev_ctx->row_delta) free(prev_ctx->row_delta);
                if (prev_ctx->diag_delta) free(prev_ctx->diag_delta);
                if (prev_ctx->xor_tmp) free(prev_ctx->xor_tmp);
                if (prev_ctx->row_mask) free(prev_ctx->row_mask);
                if (prev_ctx->diag_mask) free(prev_ctx->diag_mask);
                if (prev_ctx->plan) free(prev_ctx->plan);
                if (prev_ctx->plr_header_ptrs) free(prev_ctx->plr_header_ptrs);
            }
            free(g_ctx_pool);
            g_ctx_pool = NULL;
            free(g_ctx_free_list);
            g_ctx_free_list = NULL;
            g_ctx_pool_size = 0;
            g_ctx_free_head = 0;
            return -1;
        }
    }
    
    return 0;
}

static void ctx_pool_destroy(void) {
    if (g_ctx_pool) {
        for (int i = 0; i < g_ctx_pool_size; i++) {
            update_ctx_t *ctx = &g_ctx_pool[i];
            if (ctx->payload) free(ctx->payload);
            if (ctx->old_data) free(ctx->old_data);
            if (ctx->row_delta) free(ctx->row_delta);
            if (ctx->diag_delta) free(ctx->diag_delta);
            if (ctx->xor_tmp) free(ctx->xor_tmp);
            if (ctx->row_mask) free(ctx->row_mask);
            if (ctx->diag_mask) free(ctx->diag_mask);
            if (ctx->plan) free(ctx->plan);
            if (ctx->plr_header_ptrs) free(ctx->plr_header_ptrs);
        }
        free(g_ctx_pool);
        g_ctx_pool = NULL;
    }
    if (g_ctx_free_list) {
        free(g_ctx_free_list);
        g_ctx_free_list = NULL;
    }
    g_ctx_pool_size = 0;
    g_ctx_free_head = 0;
}

// 获取空闲上下文
static update_ctx_t* ctx_pool_get_free(void) {
    pthread_mutex_lock(&g_ctx_pool_lock);
    if (g_ctx_free_head == 0) {
        pthread_mutex_unlock(&g_ctx_pool_lock);
        return NULL;
    }
    int idx = g_ctx_free_list[--g_ctx_free_head];
    pthread_mutex_unlock(&g_ctx_pool_lock);
    return &g_ctx_pool[idx];
}

// 归还上下文
static void ctx_pool_put_free(update_ctx_t *ctx) {
    if (!ctx) return;
    
    // 清理状态
    ctx->state = CTX_FREE;
    ctx->pending_ios = 0;
    ctx->pending_plr_headers = 0;
    
    // 清理 PLR header 指针（如果还有未释放的）
    if (ctx->plr_header_ptrs && ctx->plr_header_count > 0) {
        for (int i = 0; i < ctx->plr_header_count; i++) {
            if (ctx->plr_header_ptrs[i]) {
                plr_free_header_ctx_buffer(ctx->plr_header_ptrs[i]);
            }
        }
        ctx->plr_header_count = 0;
    }
    
    pthread_mutex_lock(&g_ctx_pool_lock);
    g_ctx_free_list[g_ctx_free_head++] = ctx->id;
    pthread_mutex_unlock(&g_ctx_pool_lock);
    
    // 原子操作更新统计
    RDP_ATOMIC_INC(g_completed_updates);
    RDP_ATOMIC_DEC(g_inflight_updates);
}

static int open_and_register_disks(int k, int m) {
    int want = k + m;
    if (want <= 0 || want > 64) {
        fprintf(stderr, "open_and_register_disks: invalid disk count %d\n", want);
        return -1;
    }

    char *paths[64] = {0};
    int found = load_safe_disk_paths(paths, 64);
    if (found < want) {
        fprintf(stderr, "需要 %d 块设备，但只找到 %d 个。请在 safe_disks.txt 中列出足量设备。\n", want, found);
        for (int i = 0; i < found; i++) if (paths[i]) free(paths[i]);
        return -1;
    }

    int *fds = (int*)calloc((size_t)want, sizeof(int));
    if (!fds) {
        for (int i = 0; i < found; i++) if (paths[i]) free(paths[i]);
        return -1;
    }

    for (int i = 0; i < want; i++) {
        int fd = open(paths[i], O_RDWR | O_DIRECT | O_CLOEXEC);
        if (fd < 0) {
            perror(paths[i]);
            for (int j = 0; j <= i; j++) {
                if (paths[j]) free(paths[j]);
                if (j < i && fds[j] >= 0) close(fds[j]);
            }
            free(fds);
            return -1;
        }
        fds[i] = ensure_nonstdio_fd(fd, "disk");
        free(paths[i]);
    }

    g_fds = fds;
    g_total_disks = want;
    g_fixed.handles = g_fds;
    // 为简化语义与避免 fd/index 混淆，这里不再使用固定文件注册
    g_fixed.enabled = 0;

    return 0;
}

static void close_all_disks(void) {
    if (g_ring_ready && g_fixed.enabled) {
        io_uring_unregister_files(&g_ring);
    }
    g_fixed.enabled = 0;
    g_fixed.handles = NULL;

    if (g_fds) {
        for (int i = 0; i < g_total_disks; i++) {
            if (g_fds[i] >= 0) close(g_fds[i]);
        }
        free(g_fds);
        g_fds = NULL;
    }
    g_total_disks = 0;
    RDP_ATOMIC_STORE(g_pending_ops, 0);
}

// I/O 统计辅助函数（新增）
static inline double timespec_diff_sec(const struct timespec *start, const struct timespec *end) {
    return (end->tv_sec - start->tv_sec) + (end->tv_nsec - start->tv_nsec) / 1e9;
}
static void record_io_events(double duration_seconds, int event_count, int is_write) {
    pthread_mutex_lock(&stats.lock);
    stats.total_io_time += duration_seconds;
    if (is_write) stats.write_count += event_count; else stats.read_count += event_count;
    if (stats.io_times && event_count > 0) {
        double per_event = duration_seconds / event_count;
        for (int i = 0; i < event_count && stats.io_index < stats.io_capacity; i++) {
            stats.io_times[stats.io_index++] = per_event;
        }
    }
    pthread_mutex_unlock(&stats.lock);
}

// 增强版内存池实现 - 支持 NUMA 感知和统计
memory_pool_t* create_enhanced_memory_pool(int capacity, size_t buffer_size, int numa_node, int alignment) {
    memory_pool_t *pool = malloc(sizeof(memory_pool_t));
    if (!pool) {
        fprintf(stderr, "[MEMPOOL] Failed to allocate pool structure\n");
        return NULL;
    }
    
    memset(pool, 0, sizeof(memory_pool_t));
    
    pool->capacity = capacity;
    pool->buffer_size = buffer_size;
    pool->free_count = capacity;
    pool->numa_node = numa_node;
    pool->alignment = alignment;
    pool->prealloc_count = capacity;
    pthread_mutex_init(&pool->lock, NULL);
    
    pool->buffers = malloc(capacity * sizeof(void*));
    pool->free_list = malloc(capacity * sizeof(int));
    
    if (!pool->buffers || !pool->free_list) {
        fprintf(stderr, "[MEMPOOL] Failed to allocate buffers/free_list (capacity=%d)\n", capacity);
        if (pool->buffers) free(pool->buffers);
        if (pool->free_list) free(pool->free_list);
        pthread_mutex_destroy(&pool->lock);
        free(pool);
        return NULL;
    }

    // 获取当前时间
    clock_gettime(CLOCK_MONOTONIC, &pool->last_cleanup);
    
    for (int i = 0; i < capacity; i++) {
        if (posix_memalign(&pool->buffers[i], alignment, buffer_size) != 0) {
            fprintf(stderr, "[MEMPOOL] Failed to allocate buffer %d (%zu bytes, alignment=%d)\n", 
                   i, buffer_size, alignment);
            // 清理已分配的缓冲区
            for (int j = 0; j < i; j++) {
                free(pool->buffers[j]);
            }
            free(pool->buffers);
            free(pool->free_list);
            pthread_mutex_destroy(&pool->lock);
            free(pool);
            return NULL;
        }

        // 初始化内存为 0，提高缓存友好性
        memset(pool->buffers[i], 0, buffer_size);
        pool->free_list[i] = i;
    }
    
    pool->total_allocated = capacity * buffer_size;
    pool->peak_usage = pool->total_allocated;
    
    return pool;
}

// 兼容性函数
memory_pool_t* create_memory_pool(int capacity, size_t buffer_size) {
    return create_enhanced_memory_pool(capacity, buffer_size, -1, 64);
}

void destroy_memory_pool(memory_pool_t *pool) {
    if (!pool) return;
    
    for (int i = 0; i < pool->capacity; i++) {
        free(pool->buffers[i]);
    }
    free(pool->buffers);
    free(pool->free_list);
    pthread_mutex_destroy(&pool->lock);
    free(pool);
}

// 增强版内存池分配函数 - 支持统计和性能优化
void* pool_alloc_enhanced(memory_pool_t *pool) {
    if (!pool) return NULL;
    
    pthread_mutex_lock(&pool->lock);
    
    if (pool->free_count == 0) {
        pthread_mutex_unlock(&pool->lock);
        // 动态分配作为后备
        void *buffer;
        if (posix_memalign(&buffer, pool->alignment, pool->buffer_size) != 0) {
            return NULL;
        }
        // 初始化内存
        memset(buffer, 0, pool->buffer_size);
        return buffer;
    }
    
    int idx = pool->free_list[--pool->free_count];
    void *buffer = pool->buffers[idx];
    
    // 更新统计信息
    if (pool->peak_usage < (pool->capacity - pool->free_count) * pool->buffer_size) {
        pool->peak_usage = (pool->capacity - pool->free_count) * pool->buffer_size;
    }
    
    pthread_mutex_unlock(&pool->lock);
    
    // 预取内存到缓存，提高性能
    __builtin_prefetch(buffer, 0, 3);
    
    return buffer;
}

// 兼容性函数
void* pool_alloc(memory_pool_t *pool) {
    return pool_alloc_enhanced(pool);
}

// 增强版内存池释放函数 - 支持统计和安全清理
void pool_free_enhanced(memory_pool_t *pool, void *buffer) {
    if (!pool || !buffer) return;
    
    pthread_mutex_lock(&pool->lock);
    
    int found = -1;
    for (int i = 0; i < pool->capacity; i++) {
        if (pool->buffers[i] == buffer) {
            found = i;
            break;
        }
    }
    
    if (found >= 0 && pool->free_count < pool->capacity) {
        pool->free_list[pool->free_count++] = found;
        
        // 清理内存内容，提高安全性
        memset(buffer, 0, pool->buffer_size);
        
        // 检查是否需要定期清理
        struct timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);
        if (now.tv_sec - pool->last_cleanup.tv_sec > 300) { // 5分钟
            pool->last_cleanup = now;
            // 这里可以添加内存碎片整理逻辑
        }
        
        pthread_mutex_unlock(&pool->lock);
    } else {
        pthread_mutex_unlock(&pool->lock);
        // 动态分配的内存直接释放
        free(buffer);
    }
}

// 兼容性函数
void pool_free(memory_pool_t *pool, void *buffer) {
    pool_free_enhanced(pool, buffer);
}

// SIMD XOR 实现
// 增强版SIMD XOR实现 - 支持多种优化策略
void xor_update_simd_enhanced(char *dst, const char *src, size_t size) {
    if (!dst || !src || size == 0) return;
    
    size_t i = 0;
    
    // 预取数据到缓存
    __builtin_prefetch(dst, 1, 3);
    __builtin_prefetch(src, 0, 3);
    
    // AVX-512 处理（如果支持）
    #ifdef __AVX512F__
    for (; i + 64 <= size; i += 64) {
        __m512i d = _mm512_loadu_si512((__m512i*)(dst + i));
        __m512i s = _mm512_loadu_si512((__m512i*)(src + i));
        __m512i result = _mm512_xor_si512(d, s);
        _mm512_storeu_si512((__m512i*)(dst + i), result);
    }
    #endif
    
    // AVX2 处理
    #ifdef __AVX2__
    for (; i + 32 <= size; i += 32) {
        __m256i d = _mm256_loadu_si256((__m256i*)(dst + i));
        __m256i s = _mm256_loadu_si256((__m256i*)(src + i));
        __m256i result = _mm256_xor_si256(d, s);
        _mm256_storeu_si256((__m256i*)(dst + i), result);
    }
    #endif
    
    // SSE2 处理
    for (; i + 16 <= size; i += 16) {
        __m128i d = _mm_loadu_si128((__m128i*)(dst + i));
        __m128i s = _mm_loadu_si128((__m128i*)(src + i));
        __m128i result = _mm_xor_si128(d, s);
        _mm_storeu_si128((__m128i*)(dst + i), result);
    }

// 64位标量处理
    for (; i + 8 <= size; i += 8) {
        uint64_t *d64 = (uint64_t*)(dst + i);
        uint64_t *s64 = (uint64_t*)(src + i);
        *d64 ^= *s64;
    }

// 32位标量处理
    for (; i + 4 <= size; i += 4) {
        uint32_t *d32 = (uint32_t*)(dst + i);
        uint32_t *s32 = (uint32_t*)(src + i);
        *d32 ^= *s32;
    }

// 8位标量处理剩余部分
    for (; i < size; i++) {
        dst[i] ^= src[i];
    }
}

// 兼容性函数
void xor_update_simd(char *dst, const char *src, size_t size) {
    xor_update_simd_enhanced(dst, src, size);
}

// 批量XOR操作 - 支持多个源数据
void xor_update_batch(char *dst, char **srcs, int src_count, size_t size) {
    if (!dst || !srcs || src_count <= 0 || size == 0) return;
    
    // 对每个源数据执行XOR
    for (int i = 0; i < src_count; i++) {
        if (srcs[i]) {
            xor_update_simd_enhanced(dst, srcs[i], size);
        }
    }
}

// PLR 临时缓冲
static char *g_plr_row_delta = NULL;
static char *g_plr_diag_delta = NULL;
static char *g_plr_s_delta = NULL; // 未使用，但保留签名兼容
static char *g_plr_delta_tmp = NULL;
static size_t g_plr_delta_capacity = 0;
static size_t g_plr_delta_tmp_capacity = 0;
static unsigned char *g_plr_row_mask = NULL;
static unsigned char *g_plr_diag_mask = NULL;
static int g_plr_mask_capacity = 0;
static char *g_plr_old_blocks = NULL;
static size_t g_plr_old_capacity = 0;
static char **g_plr_old_ptrs = NULL;
static size_t g_plr_old_ptr_capacity = 0;

// PLR 辅助：准备增量缓冲区
static int plr_prepare_buffers(int blocks_to_update, size_t packet_bytes, size_t stripe_bytes, int need_old,
                               char **row_delta, char **diag_delta, char **s_delta, char **delta_tmp,
                               unsigned char **row_mask, unsigned char **diag_mask,
                               char ***old_ptrs, char **old_blocks) {
    if (!row_delta || !diag_delta || !s_delta || !delta_tmp || !row_mask || !diag_mask || !old_ptrs || !old_blocks) {
        return -1;
    }

    if (g_plr_delta_capacity < stripe_bytes) {
        free(g_plr_row_delta); free(g_plr_diag_delta); free(g_plr_s_delta);
        g_plr_row_delta = NULL; g_plr_diag_delta = NULL; g_plr_s_delta = NULL;
        if (posix_memalign((void**)&g_plr_row_delta, 64, stripe_bytes) != 0) {
            fprintf(stderr, "[PLR] Failed to allocate row_delta (%zu bytes)\n", stripe_bytes);
            return -1;
        }
        if (posix_memalign((void**)&g_plr_diag_delta, 64, stripe_bytes) != 0) {
            fprintf(stderr, "[PLR] Failed to allocate diag_delta (%zu bytes)\n", stripe_bytes);
            free(g_plr_row_delta);
            g_plr_row_delta = NULL;
            return -1;
        }
        if (posix_memalign((void**)&g_plr_s_delta, 64, stripe_bytes) != 0) {
            fprintf(stderr, "[PLR] Failed to allocate s_delta (%zu bytes)\n", stripe_bytes);
            free(g_plr_row_delta);
            free(g_plr_diag_delta);
            g_plr_row_delta = g_plr_diag_delta = NULL;
            return -1;
        }
        g_plr_delta_capacity = stripe_bytes;
    }

    if (g_plr_delta_tmp_capacity < packet_bytes) {
        free(g_plr_delta_tmp);
        g_plr_delta_tmp = NULL;
        if (posix_memalign((void**)&g_plr_delta_tmp, 64, packet_bytes) != 0) {
            fprintf(stderr, "[PLR] Failed to allocate delta_tmp (%zu bytes)\n", packet_bytes);
            g_plr_delta_tmp_capacity = 0;
            return -1;
        }
        g_plr_delta_tmp_capacity = packet_bytes;
    }

    if (g_plr_mask_capacity < config.w) {
        free(g_plr_row_mask); free(g_plr_diag_mask);
        g_plr_row_mask = (unsigned char*)calloc((size_t)config.w, sizeof(unsigned char));
        g_plr_diag_mask = (unsigned char*)calloc((size_t)config.w, sizeof(unsigned char));
        if (!g_plr_row_mask || !g_plr_diag_mask) {
            fprintf(stderr, "[PLR] Failed to allocate masks (%d entries)\n", config.w);
            if (g_plr_row_mask) free(g_plr_row_mask);
            if (g_plr_diag_mask) free(g_plr_diag_mask);
            g_plr_row_mask = g_plr_diag_mask = NULL;
            g_plr_mask_capacity = 0;
            return -1;
        }
        g_plr_mask_capacity = config.w;
    }

    if (need_old) {
        size_t old_bytes = (size_t)blocks_to_update * packet_bytes;
        if (g_plr_old_capacity < old_bytes) {
            free(g_plr_old_blocks);
            g_plr_old_blocks = NULL;
            if (posix_memalign((void**)&g_plr_old_blocks, 64, old_bytes) != 0) {
                fprintf(stderr, "[PLR] Failed to allocate old_blocks (%zu bytes)\n", old_bytes);
                g_plr_old_capacity = 0;
                return -1;
            }
            g_plr_old_capacity = old_bytes;
        }
        if (g_plr_old_ptr_capacity < (size_t)blocks_to_update) {
            char **np = (char**)realloc(g_plr_old_ptrs, sizeof(char*) * (size_t)blocks_to_update);
            if (!np) {
                fprintf(stderr, "[PLR] Failed to realloc old_ptrs (%d entries)\n", blocks_to_update);
                return -1;
            }
            g_plr_old_ptrs = np;
            g_plr_old_ptr_capacity = (size_t)blocks_to_update;
        }
    }

    *row_delta = g_plr_row_delta;
    *diag_delta = g_plr_diag_delta;
    *s_delta = g_plr_s_delta;
    *delta_tmp = g_plr_delta_tmp;
    *row_mask = g_plr_row_mask;
    *diag_mask = g_plr_diag_mask;
    *old_blocks = need_old ? g_plr_old_blocks : NULL;
    *old_ptrs = need_old ? g_plr_old_ptrs : NULL;
    return 0;
}

// 检查素数
static int next_prime(int n) {
    if (n <= 1) return 2;
    int candidate = n + 1;
    while (1) {
        int is_p = 1;
        for (int i = 2; i * i <= candidate; i++) {
            if (candidate % i == 0) {
                is_p = 0;
                break;
            }
        }
        if (is_p) return candidate;
        candidate++;
    }
}

int is_prime(int n) {
    if (n <= 1) return 0;
    if (n == 2) return 1;
    if (n % 2 == 0) return 0;
    for (int i = 3; i * i <= n; i += 2) {
        if (n % i == 0) return 0;
    }
    return 1;
}

// ========== 地址分配策略辅助函数（依赖config全局变量） ==========

// 包装函数：choose_best_mapping_enhanced 需要访问 config.update_size
static int choose_best_mapping_enhanced_wrapper(int s, int k, int w, BlockPos *blocks) {
    // 小更新（≤16KB）强制采用非零对角线的多对角分布，提升局部性并避免EVENODD对角0开销
    if (config.update_size <= 16*1024) {
        return map_blocks_diag_multi_nonzero(s, k, w, blocks);
    }
    return choose_best_mapping_enhanced(s, k, w, blocks);
}

static void fill_sequential_plan(BlockPos *blocks, int count) {
    if (!blocks || count <= 0 || config.k <= 0 || config.w <= 0) return;
    int blocks_per_stripe = config.k * config.w;
    if (blocks_per_stripe <= 0) blocks_per_stripe = config.k > 0 ? config.k : 1;
    if (blocks_per_stripe <= 0) blocks_per_stripe = 1;
    for (int i = 0; i < count; i++) {
        int logical = blocks_per_stripe ? (i % blocks_per_stripe) : i;
        int row = logical / config.k;
        int col = logical % config.k;
        if (row >= config.w) row %= config.w;
        blocks[i].row = row;
        blocks[i].col = col;
        int p = (config.p_prime > 0) ? config.p_prime : (config.k > 1 ? config.k : 2);
        int d = row + col;
        if (d >= p) d -= p;
        blocks[i].diag = d;
    }
}

static int plan_block_positions(int count, BlockPos *blocks, int prefer_rs_layout, const char **plan_used) {
    (void)prefer_rs_layout; // RDP 简化：地址映射退化为顺序布局
    if (plan_used) *plan_used = NULL;
    if (!blocks || count <= 0) return 0;
    fill_sequential_plan(blocks, count);
    if (plan_used) *plan_used = "rdp_sequential";
    return count;
}

static void reorder_plan_round_robin(BlockPos *blocks, int count, int k) {
    if (!blocks || count <= 0 || k <= 1) return;
    BlockPos *tmp = (BlockPos*)malloc(sizeof(BlockPos) * (size_t)count);
    int *counts = (int*)calloc((size_t)k, sizeof(int));
    int *offsets = (int*)calloc((size_t)k, sizeof(int));
    int *cursor = (int*)calloc((size_t)k, sizeof(int));
    if (!tmp || !counts || !offsets || !cursor) {
        free(tmp); free(counts); free(offsets); free(cursor);
        return;
    }

    for (int i = 0; i < count; i++) {
        int col = blocks[i].col % k;
        if (col < 0) col += k;
        counts[col]++;
    }

    offsets[0] = 0;
    for (int i = 1; i < k; i++) {
        offsets[i] = offsets[i - 1] + counts[i - 1];
    }
    memcpy(cursor, offsets, sizeof(int) * (size_t)k);

    for (int i = 0; i < count; i++) {
        int col = blocks[i].col % k;
        if (col < 0) col += k;
        tmp[cursor[col]++] = blocks[i];
    }

    memcpy(cursor, offsets, sizeof(int) * (size_t)k);
    int written = 0;
    int remaining = count;
    while (remaining > 0) {
        for (int col = 0; col < k && remaining > 0; col++) {
            int start = offsets[col];
            int end = start + counts[col];
            if (cursor[col] < end) {
                blocks[written++] = tmp[cursor[col]++];
                remaining--;
            }
        }
    }

    free(tmp);
    free(counts);
    free(offsets);
    free(cursor);
}

static int cmp_blockpos_diag(const void *a, const void *b) {
    const BlockPos *ba = (const BlockPos*)a;
    const BlockPos *bb = (const BlockPos*)b;
    if (ba->diag != bb->diag) return (ba->diag < bb->diag) ? -1 : 1;
    if (ba->row != bb->row) return (ba->row < bb->row) ? -1 : 1;
    if (ba->col != bb->col) return (ba->col < bb->col) ? -1 : 1;
    return 0;
}

static void reorder_plan_diag_first(BlockPos *blocks, int count) {
    if (!blocks || count <= 1) return;
    qsort(blocks, (size_t)count, sizeof(BlockPos), cmp_blockpos_diag);
}

// 计划缓存（针对复杂策略复用计算结果）
static BlockPos *g_hybrid_plan_cache = NULL;
static int g_hybrid_plan_len = 0;
static size_t g_hybrid_plan_update_size = 0;
static int g_hybrid_plan_k = 0;
static int g_hybrid_plan_w = 0;
static char g_hybrid_plan_alloc[64] = {0};
static char g_hybrid_plan_name[64] = {0};

// I/O 临时数组缓存（避免每次 malloc/free）
static char **g_io_buffers_cache = NULL;
static off_t *g_io_offsets_cache = NULL;
static int *g_io_fds_cache = NULL;
static size_t g_io_cache_capacity = 0;

// 控制 update_evenodd_simple 在无 I/O 路径时是否执行内存校验计算
static int g_update_evenodd_compute_enabled = 1;
// 增强版批量读取操作 - 支持优先级和错误恢复
void batch_io_read_enhanced(char **buffers, int *fds, int count, struct io_uring *ring, off_t offset, size_t size, int priority) {
    if (!buffers || !fds || count <= 0 || !ring) return;
    
    struct io_uring_sqe *sqes[count];
    struct timespec io_start, io_end;
    int retry_count = 0;
    const int max_retries = 3;
    
    // 预取缓冲区到缓存
    for (int i = 0; i < count; i++) {
        __builtin_prefetch(buffers[i], 1, 3);
    }

// 获取所有 SQE
    for (int i = 0; i < count; i++) {
        sqes[i] = io_uring_get_sqe(ring);
        if (!sqes[i]) {
            fprintf(stderr, "Failed to get sqe for operation %d\n", i);
            // 尝试重新获取
            sqes[i] = io_uring_get_sqe(ring);
            if (!sqes[i]) {
                fprintf(stderr, "Critical: No available SQE\n");
                exit(1);
            }
        }

// 设置优先级和标志
        // 不再使用 IOSQE_FIXED_FILE，避免 fd/index 混淆

// 设置优先级
        if (priority > 0) {
            io_uring_sqe_set_flags(sqes[i], IOSQE_IO_HARDLINK);
        }
        
        io_uring_prep_read(sqes[i], fds[i], buffers[i], size, offset);
        io_uring_sqe_set_data(sqes[i], (void*)(uintptr_t)i);
    }
    
    clock_gettime(CLOCK_MONOTONIC, &io_start);
    
    // 批量提交
    int submitted = io_uring_submit(ring);
    if (submitted != count) {
        fprintf(stderr, "Warning: Only %d/%d operations submitted\n", submitted, count);
    }

// 批量收集结果 - 优化版本
    int completed = 0;
    int error_count = 0;
    
    while (completed < count) {
        struct io_uring_cqe *cqe;
        int wait_result = io_uring_wait_cqe(ring, &cqe);
        
        if (wait_result < 0) {
            if (errno == EINTR) continue; // 被信号中断，重试
            perror("io_uring_wait_cqe");
            if (++retry_count > max_retries) {
                fprintf(stderr, "Max retries exceeded\n");
                exit(1);
            }
            continue;
        }
        
        int idx = (int)(uintptr_t)io_uring_cqe_get_data(cqe);
        int result = cqe->res;
        
        if (result < 0) {
            error_count++;
            fprintf(stderr, "Read error on fd %d (idx %d): %s (retry %d/%d)\n", 
                   fds[idx], idx, strerror(abs(result)), retry_count, max_retries);
            
            if (retry_count < max_retries) {
                // 重试失败的请求
                struct io_uring_sqe *retry_sqe = io_uring_get_sqe(ring);
                if (retry_sqe) {
                    io_uring_prep_read(retry_sqe, fds[idx], buffers[idx], size, offset);
                    io_uring_sqe_set_data(retry_sqe, (void*)(uintptr_t)idx);
                    io_uring_submit(ring);
                }
                retry_count++;
            } else {
                fprintf(stderr, "Fatal: Read operation failed after %d retries\n", max_retries);
                exit(1);
            }
        } else if (result != (int)size) {
            fprintf(stderr, "Warning: Partial read on fd %d: %d/%zu bytes\n", fds[idx], result, size);
        }
        
        io_uring_cqe_seen(ring, cqe);
        completed++;
    }
    
    clock_gettime(CLOCK_MONOTONIC, &io_end);
    record_io_events(timespec_diff_sec(&io_start, &io_end), count, 0);
    
    if (error_count > 0) {
        fprintf(stderr, "Warning: %d/%d read operations had errors\n", error_count, count);
    }
}

// 兼容性函数
void batch_io_read(char **buffers, int *fds, int count, struct io_uring *ring, off_t offset, size_t size) {
    batch_io_read_enhanced(buffers, fds, count, ring, offset, size, 0);
}

// 新增：支持每个请求自定义偏移量的并行读取（带计时）
void batch_io_read_with_offsets(char **buffers, int *fds, off_t *offsets, int count, struct io_uring *ring, size_t size) {
    struct io_uring_sqe *sqes[count];
    struct io_uring_cqe *cq;
    struct timespec io_start, io_end;
    for (int i = 0; i < count; i++) {
        sqes[i] = io_uring_get_sqe(ring);
        if (!sqes[i]) {
            fprintf(stderr, "Failed to get sqe\n");
            exit(1);
        }
        io_uring_prep_read(sqes[i], fds[i], buffers[i], size, offsets[i]);
        io_uring_sqe_set_data(sqes[i], (void*)(uintptr_t)i);
    }
    clock_gettime(CLOCK_MONOTONIC, &io_start);
    io_uring_submit(ring);
    for (int completed = 0; completed < count; completed++) {
        if (io_uring_wait_cqe(ring, &cq) < 0) {
            perror("io_uring_wait_cqe");
            exit(1);
        }
        if (cq->res < 0) {
            int idx = (int)(uintptr_t)io_uring_cqe_get_data(cq);
            fprintf(stderr, "Read error on fd %d: %s\n", fds[idx], strerror(abs(cq->res)));
            exit(1);
        }
        io_uring_cqe_seen(ring, cq);
    }
    clock_gettime(CLOCK_MONOTONIC, &io_end);
    record_io_events(timespec_diff_sec(&io_start, &io_end), count, 0);
}

// 批量写入操作
void batch_io_write(char **buffers, int *fds, int count, struct io_uring *ring, off_t offset, size_t size) {
    struct io_uring_sqe *sqes[count];
    struct io_uring_cqe *cqes[count];

    struct timespec io_start, io_end;
    
    // 获取所有 SQE
    for (int i = 0; i < count; i++) {
        sqes[i] = io_uring_get_sqe(ring);
        if (!sqes[i]) {
            fprintf(stderr, "Failed to get sqe\n");
            exit(1);
        }
        io_uring_prep_write(sqes[i], fds[i], buffers[i], size, offset);
        io_uring_sqe_set_data(sqes[i], (void*)(uintptr_t)i);
    }
    
    clock_gettime(CLOCK_MONOTONIC, &io_start);
    // 批量提交
    io_uring_submit(ring);
    
    // 批量收集结果
    for (int completed = 0; completed < count; ) {
        if (io_uring_wait_cqe(ring, &cqes[0]) < 0) {
            perror("io_uring_wait_cqe");
            exit(1);
        }
        
        int idx = (int)(uintptr_t)io_uring_cqe_get_data(cqes[0]);
        if (cqes[0]->res < 0) {
            fprintf(stderr, "Write error on fd %d: %s\n", fds[idx], strerror(abs(cqes[0]->res)));
            exit(1);
        }
        
        io_uring_cqe_seen(ring, cqes[0]);
        completed++;
    }

    clock_gettime(CLOCK_MONOTONIC, &io_end);
    record_io_events(timespec_diff_sec(&io_start, &io_end), count, 1);
}

// 新增：支持每个请求自定义偏移量的并行写入（带计时）
void batch_io_write_with_offsets(char **buffers, int *fds, off_t *offsets, int count, struct io_uring *ring, size_t size) {
    struct io_uring_sqe *sqes[count];
    struct io_uring_cqe *cq;
    struct timespec io_start, io_end;
    for (int i = 0; i < count; i++) {
        sqes[i] = io_uring_get_sqe(ring);
        if (!sqes[i]) {
            fprintf(stderr, "Failed to get sqe\n");
            exit(1);
        }
        io_uring_prep_write(sqes[i], fds[i], buffers[i], size, offsets[i]);
        io_uring_sqe_set_data(sqes[i], (void*)(uintptr_t)i);
    }
    clock_gettime(CLOCK_MONOTONIC, &io_start);
    io_uring_submit(ring);
    for (int completed = 0; completed < count; completed++) {
        if (io_uring_wait_cqe(ring, &cq) < 0) {
            perror("io_uring_wait_cqe");
            exit(1);
        }
        if (cq->res < 0) {
            int idx = (int)(uintptr_t)io_uring_cqe_get_data(cq);
            fprintf(stderr, "Write error on fd %d: %s\n", fds[idx], strerror(abs(cq->res)));
            exit(1);
        }
        io_uring_cqe_seen(ring, cq);
    }
    clock_gettime(CLOCK_MONOTONIC, &io_end);
    record_io_events(timespec_diff_sec(&io_start, &io_end), count, 1);
}

// 新增：带固定缓冲索引的批量读/写
typedef void ex_unused_typedef_to_anchor; // 占位以便插入位置唯一

static void batch_io_read_with_offsets_ex(char **buffers, int *fds, off_t *offsets, int *buf_indices, int count, struct io_uring *ring, size_t size) {
    struct io_uring_sqe *sqes[count];
    struct io_uring_cqe *cq;
    struct timespec io_start, io_end;
    for (int i = 0; i < count; i++) {
        sqes[i] = io_uring_get_sqe(ring);
        if (!sqes[i]) { fprintf(stderr, "Failed to get sqe\n"); exit(1); }
        if (g_bufpool.enabled && buf_indices && buf_indices[i] >= 0) {
            io_uring_prep_read_fixed(sqes[i], fds[i], buffers[i], size, offsets[i], buf_indices[i]);
        } else {
            io_uring_prep_read(sqes[i], fds[i], buffers[i], size, offsets[i]);
        }
        io_uring_sqe_set_data(sqes[i], (void*)(uintptr_t)i);
    }
    clock_gettime(CLOCK_MONOTONIC, &io_start);
    io_uring_submit(ring);
    for (int completed = 0; completed < count; completed++) {
        if (io_uring_wait_cqe(ring, &cq) < 0) { perror("io_uring_wait_cqe"); exit(1); }
        if (cq->res < 0) {
            int idx = (int)(uintptr_t)io_uring_cqe_get_data(cq);
            fprintf(stderr, "Read error on fd %d: %s\n", fds[idx], strerror(abs(cq->res)));
            exit(1);
        }
        io_uring_cqe_seen(ring, cq);
    }
    clock_gettime(CLOCK_MONOTONIC, &io_end);
    record_io_events(timespec_diff_sec(&io_start, &io_end), count, 0);
}

static __attribute__((unused)) void batch_io_write_with_offsets_ex(char **buffers, int *fds, off_t *offsets, int *buf_indices, int count, struct io_uring *ring, size_t size) {
    struct io_uring_sqe *sqes[count];
    struct io_uring_cqe *cq;
    struct timespec io_start, io_end;
    for (int i = 0; i < count; i++) {
        sqes[i] = io_uring_get_sqe(ring);
        if (!sqes[i]) { fprintf(stderr, "Failed to get sqe\n"); exit(1); }
        if (g_bufpool.enabled && buf_indices && buf_indices[i] >= 0) {
            io_uring_prep_write_fixed(sqes[i], fds[i], buffers[i], size, offsets[i], buf_indices[i]);
        } else {
            io_uring_prep_write(sqes[i], fds[i], buffers[i], size, offsets[i]);
        }
        io_uring_sqe_set_data(sqes[i], (void*)(uintptr_t)i);
    }
    clock_gettime(CLOCK_MONOTONIC, &io_start);
    io_uring_submit(ring);
    for (int completed = 0; completed < count; completed++) {
        if (io_uring_wait_cqe(ring, &cq) < 0) { perror("io_uring_wait_cqe"); exit(1); }
        if (cq->res < 0) {
            int idx = (int)(uintptr_t)io_uring_cqe_get_data(cq);
            fprintf(stderr, "Write error on fd %d: %s\n", fds[idx], strerror(abs(cq->res)));
            exit(1);
        }
        io_uring_cqe_seen(ring, cq);
    }
    clock_gettime(CLOCK_MONOTONIC, &io_end);
    record_io_events(timespec_diff_sec(&io_start, &io_end), count, 1);
}


// 提交写但不等待，返回提交数
static int submit_writes_no_wait_ex(char **buffers, int *fds, off_t *offsets, int *buf_indices, int count, struct io_uring *ring, size_t size) {
    int submitted = 0;
    for (int i = 0; i < count; i++) {
        struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
        if (!sqe) break; // SQ 满，立即返回已提交数量
        if (g_bufpool.enabled && buf_indices && buf_indices[i] >= 0) {
            io_uring_prep_write_fixed(sqe, fds[i], buffers[i], size, offsets[i], buf_indices[i]);
        } else {
            io_uring_prep_write(sqe, fds[i], buffers[i], size, offsets[i]);
        }
        submitted++;
    }
    if (submitted > 0) {
    io_uring_submit(ring);
    }
    return submitted;
}

// 等待 n 个 CQE 完成（带超时保护）
static void wait_cqes_n(struct io_uring *ring, int n) {
    if (!ring || n <= 0) return;
    struct io_uring_cqe *cqe;
    int completed = 0;
    int max_iterations = n * 1000; // 防止无限循环
    int iterations = 0;
    struct timespec start_time, current_time;
    clock_gettime(CLOCK_MONOTONIC, &start_time);
    const double timeout_sec = 30.0; // 30秒超时
    
    while (completed < n && iterations < max_iterations) {
        iterations++;
        
        // 检查超时
        clock_gettime(CLOCK_MONOTONIC, &current_time);
        double elapsed = (current_time.tv_sec - start_time.tv_sec) + 
                         (current_time.tv_nsec - start_time.tv_nsec) / 1e9;
        if (elapsed > timeout_sec) {
            fprintf(stderr, "[WARNING] wait_cqes_n timeout after %.1fs: completed %d/%d, pending_ops=%zu\n",
                    elapsed, completed, n, RDP_ATOMIC_LOAD(g_pending_ops));
            break;
        }
        
        int ret = io_uring_peek_cqe(ring, &cqe);
        if (ret == -EAGAIN) {
            // 没有可用的 CQE，等待一个
            ret = io_uring_wait_cqe(ring, &cqe);
        }
        if (ret < 0) {
            if (ret == -EINTR) continue;
            fprintf(stderr, "io_uring_wait_cqe failed: %s\n", strerror(-ret));
            break;
        }
        if (cqe) {
            if (cqe->res < 0) {
                fprintf(stderr, "io cqe error: %s\n", strerror(abs(cqe->res)));
            }
            void *ud = io_uring_cqe_get_data(cqe);
            if (ud) {
                uintptr_t v = (uintptr_t)ud;
                if (v >= PLR_USER_DATA_THRESHOLD) {
                    plr_free_header_ctx_buffer(ud);
                }
            }
            io_uring_cqe_seen(ring, cqe);
            completed++;
            // 原子递减 g_pending_ops
            size_t current_pending = RDP_ATOMIC_LOAD(g_pending_ops);
            if (current_pending > 0) {
                RDP_ATOMIC_STORE(g_pending_ops, current_pending - 1);
            }
        }
    }
    
    if (completed < n) {
        fprintf(stderr, "[WARNING] wait_cqes_n: only completed %d/%d (iterations=%d, pending_ops=%zu)\n",
                completed, n, iterations, RDP_ATOMIC_LOAD(g_pending_ops));
    }
}

// 对所有设备并行 fsync，确保数据落盘
static void fsync_all_devices(struct io_uring *ring, int *fds, int count) {
    struct io_uring_sqe *sqes[count];
    struct io_uring_cqe *cqe;
    for (int i = 0; i < count; i++) {
        sqes[i] = io_uring_get_sqe(ring);
        if (!sqes[i]) {
            fprintf(stderr, "Failed to get sqe for fsync\n");
            exit(1);
        }
        io_uring_prep_fsync(sqes[i], fds[i], IORING_FSYNC_DATASYNC);
        io_uring_sqe_set_data(sqes[i], (void*)(uintptr_t)i);
    }
    io_uring_submit(ring);
    for (int completed = 0; completed < count; completed++) {
        if (io_uring_wait_cqe(ring, &cqe) < 0) {
            perror("io_uring_wait_cqe fsync");
            exit(1);
        }
        if (cqe->res < 0) {
            int idx = (int)(uintptr_t)io_uring_cqe_get_data(cqe);
            fprintf(stderr, "fsync error on fd %d: %s\n", fds[idx], strerror(abs(cqe->res)));
            exit(1);
        }
        io_uring_cqe_seen(ring, cqe);
    }
}

static void ensure_queue_space(struct io_uring *ring, size_t need_ops) {
    if (!ring) return;
    size_t depth = (g_ring_queue_depth > 0) ? (size_t)g_ring_queue_depth : 256;
    size_t high_water = (size_t)((double)depth * 0.90);
    if (high_water == 0) high_water = depth;
    // 如果需要的操作已经超过队列容量，直接等到 pending 为 0
    int max_wait_iterations = 1000; // 防止无限循环
    int wait_iterations = 0;
    
    size_t current_pending = RDP_ATOMIC_LOAD(g_pending_ops);
    while (current_pending + need_ops >= high_water && current_pending > 0 && wait_iterations < max_wait_iterations) {
        wait_iterations++;
        size_t batch = current_pending > 16 ? 16 : current_pending;
        size_t old_pending = current_pending;
        wait_cqes_n(ring, (int)batch);
        // 如果 pending_ops 没有减少，说明可能有问题，强制退出
        current_pending = RDP_ATOMIC_LOAD(g_pending_ops);
        if (current_pending >= old_pending && wait_iterations > 10) {
            fprintf(stderr, "[WARNING] ensure_queue_space: pending_ops not decreasing (%zu), breaking\n", current_pending);
            break;
        }
    }
    if (wait_iterations >= max_wait_iterations) {
        fprintf(stderr, "[ERROR] ensure_queue_space: exceeded max iterations, pending_ops=%zu\n", RDP_ATOMIC_LOAD(g_pending_ops));
    }
}

// 前置声明：条带重算修复函数
static int repair_one_stripe(long stripe_index, struct io_uring *ring);

// 加载安全磁盘路径函数
static int load_safe_disk_paths(char **disk_paths, int max_disks) {
    FILE *safe_disk_file = fopen("safe_disks.txt", "r");
    int disk_count = 0;
    
    if (!safe_disk_file) {
        // 回退到默认磁盘路径（跳过nvme0n1系统盘）
        printf("警告: 未找到safe_disks.txt，使用默认磁盘路径（跳过nvme0n1）\n");
        const char *default_paths[] = {
            "/dev/nvme1n1", "/dev/nvme2n1", "/dev/nvme3n1", "/dev/nvme4n1", "/dev/nvme5n1",
            "/dev/nvme6n1", "/dev/nvme7n1", "/dev/nvme8n1", "/dev/nvme9n1", "/dev/nvme10n1",
            "/dev/nvme11n1", "/dev/nvme12n1", "/dev/nvme13n1", "/dev/nvme14n1", "/dev/nvme15n1",
            "/dev/nvme16n1", "/dev/nvme17n1"
        };
        
        int default_count = sizeof(default_paths) / sizeof(default_paths[0]);
        for (int i = 0; i < default_count && i < max_disks; i++) {
            // 检查磁盘是否存在
            if (access(default_paths[i], F_OK) == 0) {
                disk_paths[i] = strdup(default_paths[i]);
                disk_count++;
                printf("  检测到磁盘: %s\n", default_paths[i]);
            }
        }
        return disk_count;
    }
    
    printf("加载安全磁盘列表...\n");
    char line[256];
    while (fgets(line, sizeof(line), safe_disk_file) && disk_count < max_disks) {
        // 移除换行符
        line[strcspn(line, "\n")] = 0;
        
        // 跳过空行和注释
        if (line[0] == '\0' || line[0] == '#') continue;
        
        // 验证磁盘是否存在且可访问
        if (access(line, F_OK) == 0) {
            // 对于块设备，只检查文件是否存在，不检查读写权限
            // 因为块设备的权限检查可能不准确
            disk_paths[disk_count] = strdup(line);
            printf("  安全磁盘: %s\n", line);
            disk_count++;
        } else {
            printf("  跳过不存在的磁盘: %s\n", line);
        }
    }
    
    fclose(safe_disk_file);
    printf("成功加载 %d 个安全磁盘\n\n", disk_count);
    return disk_count;
}

// RDP 编码实现
// data[0..k-1]: 数据盘
// coding[0]: 行校验（Row Parity）
// coding[1]: 对角校验（Diagonal Parity）
// 约束：p = w + 1 是素数且 p > k
void rdp_encode(char **data, char **coding, int k, int w, int packetsize) {
    if (k <= 0 || w <= 0 || packetsize <= 0 || !data || !coding) {
        fprintf(stderr, "rdp_encode: invalid params k=%d w=%d packetsize=%d\n", k, w, packetsize);
        return;
    }

    int p = (config.p_prime > 0) ? config.p_prime : (w + 1);
    if (w != p - 1) {
        fprintf(stderr, "Error: RDP geometry mismatch provided to encode. w=%d, p=%d\n", w, p);
        return;
    }
    if (k >= p) {
        fprintf(stderr, "Error: RDP requires k < p (k=%d, p=%d)\n", k, p);
        return;
    }

    size_t stripe_size = (size_t)packetsize * (size_t)w;
    memset(coding[0], 0, stripe_size);
    memset(coding[1], 0, stripe_size);

    size_t packet_bytes = (size_t)packetsize;

    // 融合循环：每次读取 data[c][r] 同时更新行校验和对角校验，减少缓存抖动
    for (int c = 0; c < k; c++) {
        for (int r = 0; r < w; r++) {
            char *src = data[c] + (size_t)r * packet_bytes;

            // 行校验 (Row Parity)
            char *p_dst = coding[0] + (size_t)r * packet_bytes;
            xor_update_simd(p_dst, src, packetsize);

            // 对角校验 (Diagonal Parity) — 忽略缺失对角线 d = p-1
            int d = r + c;
            if (d >= p) d -= p;
            if (d < w) { // d == p-1 为缺失对角线
                char *q_dst = coding[1] + (size_t)d * packet_bytes;
                xor_update_simd(q_dst, src, packetsize);
            }
        }
    }

    // 行校验盘也参与对角校验（逻辑列 p-1）
    int row_parity_col_idx = p - 1;
    for (int r = 0; r < w; r++) {
        int d = r + row_parity_col_idx;
        if (d >= p) d -= p;
        if (d < w) {
            char *q_dst = coding[1] + (size_t)d * packet_bytes;
            char *src = coding[0] + (size_t)r * packet_bytes;
            xor_update_simd(q_dst, src, packetsize);
        }
    }
}

// 简单的单块更新函数 - 不进行任何地址分配优化
void update_single_block(char **data __attribute__((unused)), char **coding __attribute__((unused)), int update_disk __attribute__((unused)), int update_packet __attribute__((unused)), char *new_block_data __attribute__((unused)),
                         int *fds __attribute__((unused)), int total_disks __attribute__((unused)), struct io_uring *ring __attribute__((unused)), off_t stripe_disk_offset __attribute__((unused))) {
    // 已不再使用
}

// 加载指定条带到内存缓冲区（data[0..k-1], coding[0..m-1]）
static __attribute__((unused)) void load_stripe_from_disk(long stripe_index, char **data, char **coding,
                                  int *fds __attribute__((unused)), int k, int m, struct io_uring *ring, size_t stripe_size) {
    off_t stripe_offset = (off_t)stripe_index * (off_t)stripe_size;
    // 读取 k 个数据盘
    batch_io_read(data, g_fixed.handles, k, ring, stripe_offset, stripe_size);
    // 读取 m 个校验盘
    batch_io_read(coding, g_fixed.handles + k, m, ring, stripe_offset, stripe_size);
}

// 简单的多块顺序更新 - 批处理并行 I/O
// 说明：在本文件中，update_evenodd_simple 仅作为“通用批量写入”路径复用，
// 并不执行 EVENODD 校验逻辑，RDP 校验计算在 update_rdp_simple 中完成。
void update_evenodd_simple(char **data __attribute__((unused)), char **coding __attribute__((unused)), char *new_data,
                           int *fds, int total_disks, struct io_uring *ring,
                           long stripe_num, long *next_stripe_cursor __attribute__((unused))) {
    int blocks_to_update = config.update_size / config.packetsize;
    if (blocks_to_update <= 0) return;

    /* 在 RDP 路径中，当未使用实际 I/O (ring/fds 为空或总盘数<=0) 时跳过该函数，
       避免与 update_rdp_simple 的增量计算重复 XOR。 */
    if (!ring || !fds || total_disks <= 0) {
        return;
    }

    struct timespec upd_start_ts, upd_end_ts;
    clock_gettime(CLOCK_MONOTONIC, &upd_start_ts);

    BlockPos *allocation_plan = NULL;
    BlockPos *temp_plan = NULL;
    const char *plan_used = NULL;
    int plan_count = blocks_to_update;

    int want_hybrid_plan = (config.alloc && strcmp(config.alloc, "sequential") != 0);

    if (want_hybrid_plan &&
        g_hybrid_plan_cache &&
        g_hybrid_plan_len == blocks_to_update &&
        g_hybrid_plan_update_size == (size_t)config.update_size &&
        g_hybrid_plan_k == config.k &&
        g_hybrid_plan_w == config.w &&
        g_hybrid_plan_alloc[0] != '\0' &&
        strcmp(g_hybrid_plan_alloc, config.alloc) == 0) {
        allocation_plan = g_hybrid_plan_cache;
        plan_used = g_hybrid_plan_name[0] ? g_hybrid_plan_name : "hybrid";
        plan_count = g_hybrid_plan_len;
    }

    if (!allocation_plan) {
        if (want_hybrid_plan) {
            BlockPos *buf = (BlockPos*)realloc(g_hybrid_plan_cache, sizeof(BlockPos) * (size_t)blocks_to_update);
            if (buf) {
                g_hybrid_plan_cache = buf;
                g_hybrid_plan_len = blocks_to_update;
                g_hybrid_plan_update_size = (size_t)config.update_size;
                g_hybrid_plan_k = config.k;
                g_hybrid_plan_w = config.w;
                if (config.alloc) {
                    strncpy(g_hybrid_plan_alloc, config.alloc, sizeof(g_hybrid_plan_alloc) - 1);
                    g_hybrid_plan_alloc[sizeof(g_hybrid_plan_alloc) - 1] = '\0';
                } else {
                    g_hybrid_plan_alloc[0] = '\0';
                }
                int rc = plan_block_positions(blocks_to_update, g_hybrid_plan_cache,
                                              0,
                                              &plan_used);
                if (rc != blocks_to_update || !plan_used) {
                    fill_sequential_plan(g_hybrid_plan_cache, blocks_to_update);
                    plan_used = "sequential";
                }
                if (plan_used) {
                    strncpy(g_hybrid_plan_name, plan_used, sizeof(g_hybrid_plan_name) - 1);
                    g_hybrid_plan_name[sizeof(g_hybrid_plan_name) - 1] = '\0';
                } else {
                    g_hybrid_plan_name[0] = '\0';
                }
                allocation_plan = g_hybrid_plan_cache;
                plan_count = blocks_to_update;
                plan_used = g_hybrid_plan_name;
            }
        }

        if (!allocation_plan) {
            temp_plan = (BlockPos*)malloc(sizeof(BlockPos) * (size_t)blocks_to_update);
            if (!temp_plan) {
                return;
            }
            int rc = plan_block_positions(blocks_to_update, temp_plan,
                                          0,
                                          &plan_used);
            if (rc != blocks_to_update || !plan_used) {
                fill_sequential_plan(temp_plan, blocks_to_update);
                plan_used = "sequential";
            }
            allocation_plan = temp_plan;
            plan_count = blocks_to_update;
        }
    }

    int plan_is_sequential = (!want_hybrid_plan);
    if (!plan_is_sequential && plan_used && strstr(plan_used, "sequential") != NULL) {
        plan_is_sequential = 1;
    }

    if (plan_is_sequential && ring && g_pending_ops > 0) {
        wait_cqes_n(ring, (int)g_pending_ops);
        g_pending_ops = 0;
    }

        if (!plan_is_sequential) {
            int need_reorder = 1;
            if (allocation_plan == g_hybrid_plan_cache && g_hybrid_plan_name[0] && strstr(g_hybrid_plan_name, "+rr") != NULL) {
                need_reorder = 0;
                plan_used = g_hybrid_plan_name;
            }
            if (need_reorder) {
                if (g_parix_ctx) {
                    reorder_blocks_by_physical_addr(allocation_plan, plan_count, config.w);
                } else {
                    reorder_blocks_by_physical_addr(allocation_plan, plan_count, config.w);
                }
                if (allocation_plan == g_hybrid_plan_cache) {
                    const char *base = plan_used ? plan_used : "hybrid";
                    char tmp_name[sizeof(g_hybrid_plan_name)];
                    snprintf(tmp_name, sizeof(tmp_name), "%s+%s", base, g_parix_ctx ? "diag" : "rr");
                    strncpy(g_hybrid_plan_name, tmp_name, sizeof(g_hybrid_plan_name) - 1);
                    g_hybrid_plan_name[sizeof(g_hybrid_plan_name) - 1] = '\0';
                    plan_used = g_hybrid_plan_name;
                }
            }
        }

    char *payload = new_data;
    int allocated_payload = 0;
    if (!payload) {
        void *tmp_payload = NULL;
        size_t payload_bytes = (size_t)blocks_to_update * (size_t)config.packetsize;
        if (posix_memalign(&tmp_payload, 4096, payload_bytes) != 0) {
            if (temp_plan) free(temp_plan);
            return;
        }
        payload = (char*)tmp_payload;
        allocated_payload = 1;
        for (int i = 0; i < blocks_to_update; i++) {
            memset(payload + (size_t)i * (size_t)config.packetsize,
                   (unsigned char)((stripe_num + i) & 0xff),
                   (size_t)config.packetsize);
        }
    }

    if (g_parix_ctx) {
        parix_local_submit(g_parix_ctx,
                           (uint32_t)stripe_num,
                           allocation_plan,
                           plan_count,
                           (const unsigned char*)payload,
                           (size_t)config.packetsize);
        /* PARIX 快速路径：仅记录日志，跳过物理写，重放由后台/统一阶段完成 */
        if (allocated_payload) {
            free(payload);
        }
        if (temp_plan) {
            free(temp_plan);
        }
        clock_gettime(CLOCK_MONOTONIC, &upd_end_ts);
        double elapsed = timespec_diff_sec(&upd_start_ts, &upd_end_ts);
        pthread_mutex_lock(&stats.lock);
        stats.compute_time += elapsed;
        stats.update_count++;
        pthread_mutex_unlock(&stats.lock);
        return; /* 不执行设备写入 */
    }

    if (fds && total_disks > 0 && ring) {
        if ((size_t)blocks_to_update > g_io_cache_capacity) {
            size_t new_cap = (size_t)blocks_to_update;
            size_t old_cap = g_io_cache_capacity;
            char **nbuf = (char**)malloc(sizeof(char*) * new_cap);
            off_t *noff = (off_t*)malloc(sizeof(off_t) * new_cap);
            int *nfds = (int*)malloc(sizeof(int) * new_cap);
            if (!nbuf || !noff || !nfds) {
                free(nbuf); free(noff); free(nfds);
                if (temp_plan) free(temp_plan);
                if (allocated_payload) free(payload);
                return;
            }
            if (g_io_buffers_cache && old_cap) {
                memcpy(nbuf, g_io_buffers_cache, sizeof(char*) * old_cap);
            }
            if (g_io_offsets_cache && old_cap) {
                memcpy(noff, g_io_offsets_cache, sizeof(off_t) * old_cap);
            }
            if (g_io_fds_cache && old_cap) {
                memcpy(nfds, g_io_fds_cache, sizeof(int) * old_cap);
            }
            free(g_io_buffers_cache);
            free(g_io_offsets_cache);
            free(g_io_fds_cache);
            g_io_buffers_cache = nbuf;
            g_io_offsets_cache = noff;
            g_io_fds_cache = nfds;
            g_io_cache_capacity = new_cap;
        }

        char **buffers = g_io_buffers_cache;
        off_t *offsets = g_io_offsets_cache;
        int *req_fds = g_io_fds_cache;

        if (buffers && offsets && req_fds) {
            for (int i = 0; i < blocks_to_update; i++) {
                int col = allocation_plan[i].col % config.k;
                if (col < 0) col += config.k;
                int row = allocation_plan[i].row % config.w;
                if (row < 0) row += config.w;

                buffers[i] = payload + (size_t)i * (size_t)config.packetsize;
                offsets[i] = block_offset_bytes(stripe_num, row, config.packetsize);
                int physical_c = map_physical_disk(col, stripe_num, total_disks, config.alloc);
                if (physical_c < 0) physical_c = 0;
                if (physical_c >= total_disks) physical_c %= total_disks;
                req_fds[i] = fds[physical_c];
            }

            if (!plan_is_sequential) {
                submit_writes_no_wait_ex(buffers, req_fds, offsets, NULL, blocks_to_update, ring, (size_t)config.packetsize);
                g_pending_ops += (size_t)blocks_to_update;
                size_t threshold = (g_ring_queue_depth > 0) ? (size_t)g_ring_queue_depth : 128;
                if (g_pending_ops >= threshold && ring) {
                    size_t target = g_pending_ops - threshold / 2;
                    if (target < 1 || target > g_pending_ops) target = g_pending_ops;
                    wait_cqes_n(ring, (int)target);
                    g_pending_ops -= target;
                }
            } else {
                batch_io_write_with_offsets(buffers, req_fds, offsets, blocks_to_update, ring, (size_t)config.packetsize);
            }
        }
    } else {
        // Fallback：在内存中简单 XOR，避免空循环
        if (g_update_evenodd_compute_enabled) {
        for (int i = 0; i < blocks_to_update && coding; i++) {
            int row = allocation_plan[i].row % config.w;
            if (row < 0) row += config.w;
            if (coding[0]) {
                xor_update_simd(coding[0] + (size_t)row * (size_t)config.packetsize,
                                payload + (size_t)i * (size_t)config.packetsize,
                                (size_t)config.packetsize);
                }
            }
        }
    }

    if (allocated_payload) {
        free(payload);
    }
    if (temp_plan) {
        free(temp_plan);
    }

    clock_gettime(CLOCK_MONOTONIC, &upd_end_ts);
    double elapsed = timespec_diff_sec(&upd_start_ts, &upd_end_ts);

    pthread_mutex_lock(&stats.lock);
    stats.compute_time += elapsed;
    stats.update_count++;
    pthread_mutex_unlock(&stats.lock);
}

// ========== 异步流水线：阶段函数 ==========

// 阶段 1: 提交读请求（Read Old Data）
static void async_submit_read_stage(update_ctx_t *ctx, long stripe_num, char *payload) {
    ctx->stripe_num = stripe_num;
    ctx->state = CTX_READING;
    
    // 复制 payload 到上下文
    if (payload) {
        memcpy(ctx->payload, payload, (size_t)config.update_size);
    } else {
        // 生成测试数据
        memset(ctx->payload, 0xAA, (size_t)config.update_size);
    }
    
    // 生成布局计划
    int blk_cnt = config.update_size / config.packetsize;
    fill_sequential_plan(ctx->plan, blk_cnt);
    ctx->plan_count = blk_cnt;
    
    // 清空 mask
    memset(ctx->row_mask, 0, (size_t)config.w);
    memset(ctx->diag_mask, 0, (size_t)config.w);
    
    // 提交读请求
    ctx->pending_ios = 0;
    size_t packet_bytes = (size_t)config.packetsize;
    int p = (config.p_prime > 0) ? config.p_prime : (config.w + 1);
    
    for (int i = 0; i < blk_cnt; i++) {
        struct io_uring_sqe *sqe = io_uring_get_sqe(&g_ring);
        if (!sqe) {
            // SQ 满了，无法继续提交
            // 主循环会处理 CQE 并释放空间
            fprintf(stderr, "[WARNING] SQ full during read submission, stripe %ld, submitted %d/%d\n",
                    stripe_num, ctx->pending_ios, blk_cnt);
            break;  // pending_ios 准确反映已提交的数量
        }
        
        int col = ctx->plan[i].col % config.k;
        if (col < 0) col += config.k;
        int row = ctx->plan[i].row % config.w;
        if (row < 0) row += config.w;
        
        off_t offset = block_offset_bytes(stripe_num, row, config.packetsize);
        int fd_idx = map_physical_disk(col, stripe_num, g_total_disks, config.alloc);
        if (fd_idx < 0) fd_idx = 0;
        if (fd_idx >= g_total_disks) fd_idx %= g_total_disks;
        
        io_uring_prep_read(sqe, g_fds[fd_idx], 
                          ctx->old_data + (size_t)i * packet_bytes, 
                          packet_bytes, offset);
        // user_data 使用小的 ctx->id，保证 < PLR_USER_DATA_THRESHOLD，避免与 PLR header 指针冲突
        io_uring_sqe_set_data(sqe, (void*)(uintptr_t)ctx->id);
        ctx->pending_ios++;
    }
}

// 阶段 2: 处理读完成，计算 Delta 并提交写请求
static void async_process_read_done_and_submit_write(update_ctx_t *ctx, char **data, char **coding) {
    ctx->state = CTX_WRITING;
    int blk_cnt = ctx->plan_count;
    size_t packet_bytes = (size_t)config.packetsize;
    size_t stripe_bytes = (size_t)config.w * packet_bytes;
    int p = (config.p_prime > 0) ? config.p_prime : (config.w + 1);
    
    // 清空 Delta Buffers（只清零 mask 标记的区域）
    for (int r = 0; r < config.w; r++) {
        if (ctx->row_mask[r]) {
            memset(ctx->row_delta + (size_t)r * packet_bytes, 0, packet_bytes);
            ctx->row_mask[r] = 0;
        }
    }
    for (int d = 0; d < config.w; d++) {
        if (ctx->diag_mask[d]) {
            memset(ctx->diag_delta + (size_t)d * packet_bytes, 0, packet_bytes);
            ctx->diag_mask[d] = 0;
        }
    }
    
    // 1. 计算 Delta
    for (int i = 0; i < blk_cnt; i++) {
        int col = ctx->plan[i].col % config.k;
        if (col < 0) col += config.k;
        int r = ctx->plan[i].row % config.w;
        if (r < 0) r += config.w;
        int d = r + col;
        if (d >= p) d -= p;

        char *old_ptr = ctx->old_data + (size_t)i * packet_bytes;
        char *new_ptr = ctx->payload + (size_t)i * packet_bytes;
        
        // 计算 Delta -> xor_tmp
        memcpy(ctx->xor_tmp, old_ptr, packet_bytes);
        xor_update_simd(ctx->xor_tmp, new_ptr, packet_bytes);
        
        // 累加到 Delta Buffers
        if (!buffer_is_zero(ctx->xor_tmp, packet_bytes)) {
            xor_update_simd(ctx->row_delta + (size_t)r * packet_bytes, ctx->xor_tmp, packet_bytes);
            ctx->row_mask[r] = 1;
            
            if (d < config.w) { // 跳过缺失对角线
                xor_update_simd(ctx->diag_delta + (size_t)d * packet_bytes, ctx->xor_tmp, packet_bytes);
                ctx->diag_mask[d] = 1;
            }
            
            // 更新内存中的 parity（用于 -V 校验）
            if (coding) {
                if (coding[0]) {
                    xor_update_simd(coding[0] + (size_t)r * packet_bytes, ctx->xor_tmp, packet_bytes);
                }
                if (coding[1] && d < config.w) {
                    xor_update_simd(coding[1] + (size_t)d * packet_bytes, ctx->xor_tmp, packet_bytes);
                }
            }
        }
        
        // 更新内存数据
        if (data && data[col]) {
            memcpy(data[col] + (size_t)r * packet_bytes, new_ptr, packet_bytes);
        }
    }
    
    // 2. 提交写请求
    ctx->pending_ios = 0;
    ctx->pending_plr_headers = 0;
    
    // A. 提交 PLR Logs
    if (config.plr_enabled && g_plr_ctx) {
        // 为 PLR header 分配空间
        int plr_count = 0;
        for (int r = 0; r < config.w; r++) if (ctx->row_mask[r]) plr_count++;
        for (int d = 0; d < config.w; d++) if (ctx->diag_mask[d]) plr_count++;
        
        if (plr_count > 0) {
            if (ctx->plr_header_capacity < plr_count) {
                ctx->plr_header_ptrs = (void**)realloc(ctx->plr_header_ptrs, sizeof(void*) * (size_t)plr_count);
                ctx->plr_header_capacity = plr_count;
            }
            ctx->plr_header_count = 0;
        }
        
        // Row Parity Logs
        for (int r = 0; r < config.w; r++) {
            if (ctx->row_mask[r]) {
                plr_delta_descriptor_t desc = {
                    .stripe_index = ctx->stripe_num,
                    .payload_bytes = packet_bytes,
                    .logical_offset = (size_t)r * packet_bytes
                };
                off_t w_off = 0;
                if (plr_submit_delta_write(g_plr_ctx, 0, &g_ring, &desc,
                                          ctx->row_delta + (size_t)r * packet_bytes,
                                          packet_bytes, &w_off) == 0) {
                    // PLR 模块管理自己的 I/O，不计入 ctx->pending_ios
                    // （因为 PLR 的 CQE user_data 不是 ctx->id，不会递减 pending_ios）
                    ctx->pending_plr_headers++;
                }
            }
        }
        // Diag Parity Logs
        for (int d = 0; d < config.w; d++) {
            if (ctx->diag_mask[d]) {
                plr_delta_descriptor_t desc = {
                    .stripe_index = ctx->stripe_num,
                    .payload_bytes = packet_bytes,
                    .logical_offset = (size_t)d * packet_bytes
                };
                off_t w_off = 0;
                if (plr_submit_delta_write(g_plr_ctx, 1, &g_ring, &desc,
                                          ctx->diag_delta + (size_t)d * packet_bytes,
                                          packet_bytes, &w_off) == 0) {
                    // PLR 模块管理自己的 I/O，不计入 ctx->pending_ios
                    ctx->pending_plr_headers++;
                }
            }
        }
    }
    
    // B. 提交数据盘覆盖写
    for (int i = 0; i < blk_cnt; i++) {
        struct io_uring_sqe *sqe = io_uring_get_sqe(&g_ring);
        if (!sqe) {
            // SQ 满了，无法继续提交
            fprintf(stderr, "[WARNING] SQ full during write submission, stripe %ld, submitted %d/%d\n",
                    ctx->stripe_num, ctx->pending_ios, blk_cnt);
            break;  // pending_ios 准确反映已提交的数量
        }
        
        int col = ctx->plan[i].col % config.k;
        if (col < 0) col += config.k;
        int r = ctx->plan[i].row % config.w;
        if (r < 0) r += config.w;
        
        off_t offset = block_offset_bytes(ctx->stripe_num, r, config.packetsize);
        int fd_idx = map_physical_disk(col, ctx->stripe_num, g_total_disks, config.alloc);
        if (fd_idx < 0) fd_idx = 0;
        if (fd_idx >= g_total_disks) fd_idx %= g_total_disks;
        
        io_uring_prep_write(sqe, g_fds[fd_idx],
                           ctx->payload + (size_t)i * packet_bytes,
                           packet_bytes, offset);
        // 同样仅存 ctx->id，保持 < PLR_USER_DATA_THRESHOLD
        io_uring_sqe_set_data(sqe, (void*)(uintptr_t)ctx->id);
        ctx->pending_ios++;
    }
}

// RDP 更新包装：沿用通用 I/O 调度逻辑，语义上标识为 RDP 路径
void update_rdp_simple(char **data, char **coding, char *new_data,
                       int *fds, int total_disks, struct io_uring *ring,
                       long stripe_num, long *next_stripe_cursor) {
    int blocks_to_update = config.update_size / config.packetsize;
    if (blocks_to_update <= 0) return;
    size_t packet_bytes = (size_t)config.packetsize;
    size_t stripe_bytes = (size_t)config.w * packet_bytes;
    int p = (config.p_prime > 0) ? config.p_prime : (config.w + 1);

    // 准备待写入的数据负载（与 I/O 路径复用）
    char *payload = new_data;
    int allocated_payload = 0;
    if (!payload) {
        void *tmp = NULL;
        size_t payload_bytes = (size_t)blocks_to_update * packet_bytes;
        if (posix_memalign(&tmp, 4096, payload_bytes) != 0) {
            return;
        }
        payload = (char*)tmp;
        allocated_payload = 1;
        for (int i = 0; i < blocks_to_update; i++) {
            memset(payload + (size_t)i * packet_bytes,
                   (unsigned char)((stripe_num + i) & 0xff),
                   packet_bytes);
        }
    }

    // 复用/扩容全局计划缓存，避免热路径 malloc/free
    if (!g_hybrid_plan_cache || g_hybrid_plan_len < blocks_to_update) {
        BlockPos *buf = (BlockPos*)realloc(g_hybrid_plan_cache, sizeof(BlockPos) * (size_t)blocks_to_update);
        if (!buf) {
            if (allocated_payload) free(payload);
            return;
        }
        g_hybrid_plan_cache = buf;
        g_hybrid_plan_len = blocks_to_update;
    }
    BlockPos *plan = g_hybrid_plan_cache;
    const char *strategy = NULL;
    int planned = plan_block_positions(blocks_to_update, plan, 0, &strategy);
    if (planned < blocks_to_update) {
        if (allocated_payload) free(payload);
        return;
    }

    int ring_active = (ring && fds && total_disks > 0);
    int plr_active = ring_active && g_plr_ctx && config.plr_enabled;

    char *row_delta = NULL, *diag_delta = NULL, *s_delta = NULL, *delta_tmp = NULL, *old_blocks = NULL;
    unsigned char *row_mask = NULL, *diag_mask = NULL;
    char **old_ptrs = NULL;

    if (plr_active) {
        if (plr_prepare_buffers(blocks_to_update, packet_bytes, stripe_bytes, /*need_old=*/1,
                                &row_delta, &diag_delta, &s_delta, &delta_tmp,
                                &row_mask, &diag_mask, &old_ptrs, &old_blocks) != 0) {
            plr_active = 0;
        }
    }
    if (!delta_tmp) {
        delta_tmp = alloca(packet_bytes);
    }

    struct timespec upd_start_ts, upd_end_ts;
    clock_gettime(CLOCK_MONOTONIC, &upd_start_ts);

    if (ring_active) {
        if ((size_t)blocks_to_update > g_io_cache_capacity) {
            size_t new_cap = (size_t)blocks_to_update;
            char **nbuf = (char**)malloc(sizeof(char*) * new_cap);
            off_t *noff = (off_t*)malloc(sizeof(off_t) * new_cap);
            int *nfds = (int*)malloc(sizeof(int) * new_cap);
            if (!nbuf || !noff || !nfds) {
                free(nbuf); free(noff); free(nfds);
                if (allocated_payload) free(payload);
                return;
            }
            free(g_io_buffers_cache);
            free(g_io_offsets_cache);
            free(g_io_fds_cache);
            g_io_buffers_cache = nbuf;
            g_io_offsets_cache = noff;
            g_io_fds_cache = nfds;
            g_io_cache_capacity = new_cap;
        }

        char **buffers = g_io_buffers_cache;
        off_t *offsets = g_io_offsets_cache;
        int *req_fds = g_io_fds_cache;

        if (buffers && offsets && req_fds) {
    for (int i = 0; i < blocks_to_update; i++) {
                int col = plan[i].col % config.k;
                if (col < 0) col += config.k;
                int row = plan[i].row % config.w;
                if (row < 0) row += config.w;
                size_t row_off = (size_t)row * packet_bytes;

                buffers[i] = payload + (size_t)i * packet_bytes;
                offsets[i] = block_offset_bytes(stripe_num, row, config.packetsize);
                int physical_c = map_physical_disk(col, stripe_num, total_disks, config.alloc);
                if (physical_c < 0) physical_c = 0;
                if (physical_c >= total_disks) physical_c %= total_disks;
                req_fds[i] = fds[physical_c];

                if (plr_active && old_ptrs && old_blocks) {
                    old_ptrs[i] = old_blocks + (size_t)i * packet_bytes;
                }
            }

            // 读旧数据
            if (plr_active && old_ptrs && old_blocks) {
                batch_io_read_with_offsets(old_ptrs, req_fds, offsets, blocks_to_update, ring, packet_bytes);
        }

            // 计算 delta，更新内存状态
            for (int i = 0; i < blocks_to_update; i++) {
                int col = plan[i].col % config.k;
                if (col < 0) col += config.k;
                int row = plan[i].row % config.w;
                if (row < 0) row += config.w;
                size_t row_off = (size_t)row * packet_bytes;
                const char *old_ptr = NULL;

                if (plr_active && old_ptrs && old_blocks) {
                    old_ptr = old_ptrs[i];
                } else if (data && data[col]) {
                    old_ptr = data[col] + row_off;
                }

                if (old_ptr) {
                    memcpy(delta_tmp, old_ptr, packet_bytes);
                } else {
                    memset(delta_tmp, 0, packet_bytes);
                }
                xor_update_simd(delta_tmp, payload + (size_t)i * packet_bytes, packet_bytes);

                if (!buffer_is_zero(delta_tmp, packet_bytes)) {
                    if (row_delta) {
                        xor_update_simd(row_delta + row_off, delta_tmp, packet_bytes);
                        if (row_mask) row_mask[row] = 1;
                    }

                    int d = row + col;
            if (d >= p) d -= p;
            if (d < config.w) {
                        size_t diag_off = (size_t)d * packet_bytes;
                        if (diag_delta) {
                            xor_update_simd(diag_delta + diag_off, delta_tmp, packet_bytes);
                            if (diag_mask) diag_mask[d] = 1;
                        }
                    }

                    // 同步更新内存中的 parity（用于 -V 校验）
                    if (coding) {
                        if (coding[0]) {
                            xor_update_simd(coding[0] + row_off, delta_tmp, packet_bytes);
                        }
                        if (coding[1] && d < config.w) {
                            xor_update_simd(coding[1] + (size_t)d * packet_bytes, delta_tmp, packet_bytes);
                        }
                    }
                }

                // 更新内存数据
                if (data && data[col]) {
                    memcpy(data[col] + row_off, payload + (size_t)i * packet_bytes, packet_bytes);
                }
            }

            // 将累计的 parity delta 应用到内存中的 parity 缓冲
            if (row_delta && row_mask && coding && coding[0]) {
                for (int r = 0; r < config.w; r++) {
                    if (row_mask[r]) {
                        xor_update_simd(coding[0] + (size_t)r * packet_bytes,
                                        row_delta + (size_t)r * packet_bytes,
                                        packet_bytes);
                    }
                }
            }
            if (diag_delta && diag_mask && coding && coding[1]) {
                for (int d = 0; d < config.w; d++) {
                    if (diag_mask[d]) {
                        xor_update_simd(coding[1] + (size_t)d * packet_bytes,
                                        diag_delta + (size_t)d * packet_bytes,
                                        packet_bytes);
                    }
                }
            }

            // 提交 PLR delta 写
            int plr_success = 0;
            if (plr_active) {
                // 预估本次 PLR 需要的 I/O 数量（header+data）
                int plr_need = 0;
                for (int r = 0; r < config.w; r++) if (row_mask && row_mask[r]) plr_need += 2;
                for (int d = 0; d < config.w; d++) if (diag_mask && diag_mask[d]) plr_need += 2;
                ensure_queue_space(ring, (size_t)plr_need);

                for (int r = 0; r < config.w; r++) {
                    if (row_mask && row_mask[r]) {
                        size_t row_off = (size_t)r * packet_bytes;
                        if (!buffer_is_zero(row_delta + row_off, packet_bytes)) {
                            plr_delta_descriptor_t desc = {
                                .stripe_index = stripe_num,
                                .payload_bytes = packet_bytes,
                                .logical_offset = row_off
                            };
                            off_t write_off = 0;
                            if (plr_submit_delta_write(g_plr_ctx, 0, ring, &desc,
                                                       row_delta + row_off, packet_bytes, &write_off) == 0) {
                                plr_complete_delta_write(g_plr_ctx, 0, &desc, packet_bytes, write_off);
                                plr_success++;
                            }
                        }
                    }
                }
                for (int d = 0; d < config.w; d++) {
                    if (diag_mask && diag_mask[d]) {
                        size_t diag_off = (size_t)d * packet_bytes;
                        if (!buffer_is_zero(diag_delta + diag_off, packet_bytes)) {
                            plr_delta_descriptor_t desc = {
                                .stripe_index = stripe_num,
                                .payload_bytes = packet_bytes,
                                .logical_offset = diag_off
                            };
                            off_t write_off = 0;
                            if (plr_submit_delta_write(g_plr_ctx, 1, ring, &desc,
                                                       diag_delta + diag_off, packet_bytes, &write_off) == 0) {
                                plr_complete_delta_write(g_plr_ctx, 1, &desc, packet_bytes, write_off);
                                plr_success++;
                            }
                        }
                    }
                }
                if (plr_success > 0) {
                    int submit_rc = io_uring_submit(ring);
                    if (submit_rc > 0) {
                        g_pending_ops += (size_t)submit_rc; // 提交的 SQE 数量
                    }
                }
                // 清理已用的 delta/mask（按脏区局部清零，减少下轮 memset 开销）
                for (int r = 0; r < config.w; r++) {
                    if (row_mask && row_mask[r]) {
                        memset(row_delta + (size_t)r * packet_bytes, 0, packet_bytes);
                        row_mask[r] = 0;
                    }
                }
                for (int d = 0; d < config.w; d++) {
                    if (diag_mask && diag_mask[d]) {
                        memset(diag_delta + (size_t)d * packet_bytes, 0, packet_bytes);
                        diag_mask[d] = 0;
                    }
                }
            }

            // 写入数据块（使用异步写入以保持一致性）
            int submitted = 0;
            ensure_queue_space(ring, (size_t)blocks_to_update);
            submitted = submit_writes_no_wait_ex(buffers, req_fds, offsets, NULL, blocks_to_update, ring, packet_bytes);
            g_pending_ops += (size_t)submitted;
            if (submitted < blocks_to_update) {
                // 队列不足：回收部分 CQE 后尝试补交
                size_t remaining = (size_t)blocks_to_update - (size_t)submitted;
                int wait_batch = (int)(g_pending_ops > 16 ? 16 : g_pending_ops);
                if (wait_batch > 0) {
                    wait_cqes_n(ring, wait_batch);
                }
                ensure_queue_space(ring, remaining);
                int retry = submit_writes_no_wait_ex(buffers + submitted, req_fds + submitted,
                                                     offsets + submitted, NULL, (int)remaining, ring, packet_bytes);
                g_pending_ops += (size_t)retry;
                submitted += retry;
                if ((size_t)retry < remaining) {
                    fprintf(stderr, "[WARNING] SQ full, only submitted %d/%zu data writes (stripe %ld)\n",
                            retry, remaining, stripe_num);
                }
            }
        }
    } else {
        // 纯内存路径：直接将 payload 视作 delta 更新 data/coding
        for (int i = 0; i < blocks_to_update; i++) {
            int col = plan[i].col % config.k;
            if (col < 0) col += config.k;
            int row = plan[i].row % config.w;
            if (row < 0) row += config.w;
            size_t row_off = (size_t)row * packet_bytes;
            char *delta_src = payload + (size_t)i * packet_bytes;

            if (data && data[col]) {
                xor_update_simd(data[col] + row_off, delta_src, packet_bytes);
            }
            if (coding && coding[0]) {
                xor_update_simd(coding[0] + row_off, delta_src, packet_bytes);
            }
            int d = row + col;
            if (d >= p) d -= p;
            if (d < config.w && coding && coding[1]) {
                xor_update_simd(coding[1] + (size_t)d * packet_bytes, delta_src, packet_bytes);
            }
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &upd_end_ts);
    double elapsed = timespec_diff_sec(&upd_start_ts, &upd_end_ts);
    pthread_mutex_lock(&stats.lock);
    stats.compute_time += elapsed;
    stats.update_count++;
    pthread_mutex_unlock(&stats.lock);

    static RDP_ATOMIC_LONG plr_maintenance_counter = ATOMIC_VAR_INIT(0);  // 原子计数器
    if (plr_active && ((RDP_ATOMIC_FETCH_ADD(plr_maintenance_counter, 1) + 1) % 100 == 0)) {
        plr_background_maintenance(g_plr_ctx);
    }

    // 清理本次脏区域，避免全量 memset
    if (plr_active) {
        for (int r = 0; r < config.w; r++) {
            if (row_mask && row_mask[r]) {
                memset(row_delta + (size_t)r * packet_bytes, 0, packet_bytes);
                row_mask[r] = 0;
            }
            if (diag_mask && diag_mask[r]) {
                memset(diag_delta + (size_t)r * packet_bytes, 0, packet_bytes);
                diag_mask[r] = 0;
            }
        }
        if (s_delta) {
            memset(s_delta, 0, stripe_bytes);
        }
    }

    if (allocated_payload) {
        free(payload);
    }
}

// 简单的CRC32实现（完整256表，避免越界）
static uint32_t crc32(uint32_t crc, const unsigned char *buf, size_t len) {
    static const uint32_t crc32_table[256] = {
        0x00000000U, 0x77073096U, 0xEE0E612CU, 0x990951BAU, 0x076DC419U, 0x706AF48FU, 0xE963A535U, 0x9E6495A3U,
        0x0EDB8832U, 0x79DCB8A4U, 0xE0D5E91EU, 0x97D2D988U, 0x09B64C2BU, 0x7EB17CBDU, 0xE7B82D07U, 0x90BF1D91U,
        0x1DB71064U, 0x6AB020F2U, 0xF3B97148U, 0x84BE41DEU, 0x1ADAD47DU, 0x6DDDE4EBU, 0xF4D4B551U, 0x83D385C7U,
        0x136C9856U, 0x646BA8C0U, 0xFD62F97AU, 0x8A65C9ECU, 0x14015C4FU, 0x63066CD9U, 0xFA0F3D63U, 0x8D080DF5U,
        0x3B6E20C8U, 0x4C69105EU, 0xD56041E4U, 0xA2677172U, 0x3C03E4D1U, 0x4B04D447U, 0xD20D85FDU, 0xA50AB56BU,
        0x35B5A8FAU, 0x42B2986CU, 0xDBBBC9D6U, 0xACBCF940U, 0x32D86CE3U, 0x45DF5C75U, 0xDCD60DCFU, 0xABD13D59U,
        0x26D930ACU, 0x51DE003AU, 0xC8D75180U, 0xBFD06116U, 0x21B4F4B5U, 0x56B3C423U, 0xCFBA9599U, 0xB8BDA50FU,
        0x2802B89EU, 0x5F058808U, 0xC60CD9B2U, 0xB10BE924U, 0x2F6F7C87U, 0x58684C11U, 0xC1611DABU, 0xB6662D3DU,
        0x76DC4190U, 0x01DB7106U, 0x98D220BCU, 0xEFD5102AU, 0x71B18589U, 0x06B6B51FU, 0x9FBFE4A5U, 0xE8B8D433U,
        0x7807C9A2U, 0x0F00F934U, 0x9609A88EU, 0xE10E9818U, 0x7F6A0DBBU, 0x086D3D2DU, 0x91646C97U, 0xE6635C01U,
        0x6B6B51F4U, 0x1C6C6162U, 0x856530D8U, 0xF262004EU, 0x6C0695EDU, 0x1B01A57BU, 0x8208F4C1U, 0xF50FC457U,
        0x65B0D9C6U, 0x12B7E950U, 0x8BBEB8EAU, 0xFCB9887CU, 0x62DD1DDFU, 0x15DA2D49U, 0x8CD37CF3U, 0xFBD44C65U,
        0x4DB26158U, 0x3AB551CEU, 0xA3BC0074U, 0xD4BB30E2U, 0x4ADFA541U, 0x3DD895D7U, 0xA4D1C46DU, 0xD3D6F4FBU,
        0x4369E96AU, 0x346ED9FCU, 0xAD678846U, 0xDA60B8D0U, 0x44042D73U, 0x33031DE5U, 0xAA0A4C5FU, 0xDD0D7CC9U,
        0x5005713CU, 0x270241AAU, 0xBE0B1010U, 0xC90C2086U, 0x5768B525U, 0x206F85B3U, 0xB966D409U, 0xCE61E49FU,
        0x5EDEF90EU, 0x29D9C998U, 0xB0D09822U, 0xC7D7A8B4U, 0x59B33D17U, 0x2EB40D81U, 0xB7BD5C3BU, 0xC0BA6CADU,
        0xEDB88320U, 0x9ABFB3B6U, 0x03B6E20CU, 0x74B1D29AU, 0xEAD54739U, 0x9DD277AFU, 0x04DB2615U, 0x73DC1683U,
        0xE3630B12U, 0x94643B84U, 0x0D6D6A3EU, 0x7A6A5AA8U, 0xE40ECF0BU, 0x9309FF9DU, 0x0A00AE27U, 0x7D079EB1U,
        0xF00F9344U, 0x8708A3D2U, 0x1E01F268U, 0x6906C2FEU, 0xF762575DU, 0x806567CBU, 0x196C3671U, 0x6E6B06E7U,
        0xFED41B76U, 0x89D32BE0U, 0x10DA7A5AU, 0x67DD4ACCU, 0xF9B9DF6FU, 0x8EBEEFF9U, 0x17B7BE43U, 0x60B08ED5U,
        0xD6D6A3E8U, 0xA1D1937EU, 0x38D8C2C4U, 0x4FDFF252U, 0xD1BB67F1U, 0xA6BC5767U, 0x3FB506DDU, 0x48B2364BU,
        0xD80D2BDAU, 0xAF0A1B4CU, 0x36034AF6U, 0x41047A60U, 0xDF60EFC3U, 0xA867DF55U, 0x316E8EEFU, 0x4669BE79U,
        0xCB61B38CU, 0xBC66831AU, 0x256FD2A0U, 0x5268E236U, 0xCC0C7795U, 0xBB0B4703U, 0x220216B9U, 0x5505262FU,
        0xC5BA3BBEU, 0xB2BD0B28U, 0x2BB45A92U, 0x5CB36A04U, 0xC2D7FFA7U, 0xB5D0CF31U, 0x2CD99E8BU, 0x5BDEAE1DU,
        0x9B64C2B0U, 0xEC63F226U, 0x756AA39CU, 0x026D930AU, 0x9C0906A9U, 0xEB0E363FU, 0x72076785U, 0x05005713U,
        0x95BF4A82U, 0xE2B87A14U, 0x7BB12BAEU, 0x0CB61B38U, 0x92D28E9BU, 0xE5D5BE0DU, 0x7CDCEFB7U, 0x0BDBDF21U,
        0x86D3D2D4U, 0xF1D4E242U, 0x68DDB3F8U, 0x1FDA836EU, 0x81BE16CDU, 0xF6B9265BU, 0x6FB077E1U, 0x18B74777U,
        0x88085AE6U, 0xFF0F6A70U, 0x66063BCAU, 0x11010B5CU, 0x8F659EFFU, 0xF862AE69U, 0x616BFFD3U, 0x166CCF45U,
        0xA00AE278U, 0xD70DD2EEU, 0x4E048354U, 0x3903B3C2U, 0xA7672661U, 0xD06016F7U, 0x4969474DU, 0x3E6E77DBU,
        0xAED16A4AU, 0xD9D65ADCU, 0x40DF0B66U, 0x37D83BF0U, 0xA9BCAE53U, 0xDEBB9EC5U, 0x47B2CF7FU, 0x30B5FFE9U,
        0xBDBDF21CU, 0xCABAC28AU, 0x53B39330U, 0x24B4A3A6U, 0xBAD03605U, 0xCDD70693U, 0x54DE5729U, 0x23D967BFU,
        0xB3667A2EU, 0xC4614AB8U, 0x5D681B02U, 0x2A6F2B94U, 0xB40BBE37U, 0xC30C8EA1U, 0x5A05DF1BU, 0x2D02EF8DU
    };

    crc ^= 0xFFFFFFFFU;
    while (len--) {
        crc = crc32_table[(crc ^ *buf++) & 0xFFU] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFFU;
}

static void print_usage(const char *prog) {
    fprintf(stderr, "Usage: %s [options] [input_file]\n", prog);
    fprintf(stderr, "Options:\n");
    fprintf(stderr, "  -k <num>      Number of data disks (default %d; p auto > k)\n", config.k);
    fprintf(stderr, "  -m <num>      Parity disks (ignored; RDP uses %d)\n", 2);
    fprintf(stderr, "  -w <num>      Rows w (optional; w+1 must be prime > k, default %d)\n", config.w);
    fprintf(stderr, "  -p <bytes>    Packet size in bytes (default %d)\n", config.packetsize);
    fprintf(stderr, "  -u <bytes>    Update size in bytes (default %d)\n", config.update_size);
    fprintf(stderr, "  -n <count>    Number of updates to simulate (default %d)\n", config.n_updates);
    fprintf(stderr, "  -a <policy>   Address allocation policy (sequential,row,diag,auto,...)\n");
    fprintf(stderr, "  -L <dir>      Working directory for PARIX logs (default ./parix_local)\n");
    fprintf(stderr, "  -S            Enable strong consistency revalidation flag\n");
    fprintf(stderr, "  -V <count>    Enable verification (sample stripes, <=0 for full)\n");
    fprintf(stderr, "  -X <mode>     PARIX mode: off|parix|parix+alloc (default off)\n");
    fprintf(stderr, "  -P <dir>      Enable PLR and set base directory\n");
    fprintf(stderr, "  -R <bytes>    Reserved bytes per parity block for PLR (default auto)\n");
    fprintf(stderr, "  -E <alpha>    PLR EWMA alpha (0,1], default 0.25\n");
    fprintf(stderr, "  -G <cnt>      PLR merge interval (updates)\n");
    fprintf(stderr, "  -H <cnt>      PLR shrink interval (updates)\n");
    fprintf(stderr, "  -J <ratio>    PLR expand utilization threshold\n");
    fprintf(stderr, "  -K <ratio>    PLR shrink utilization threshold\n");
    fprintf(stderr, "  -h            Show this help and exit\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "If an input_file is provided its first stripe is encoded; otherwise synthetic data is used.\n");
}

// 防止文件描述符与标准输入输出冲突
static int ensure_nonstdio_fd(int fd, const char *who) {
    fprintf(stderr, "[FD-CHECK] %s: checking fd=%d\n", who ? who : "unknown", fd);
    if (fd >= 3) {
        fprintf(stderr, "[FD-CHECK] %s: fd %d is safe (>= 3)\n", who ? who : "unknown", fd);
        return fd;
    }
    fprintf(stderr, "[FD-CHECK] %s: fd %d is unsafe (< 3), duplicating...\n", who ? who : "unknown", fd);
    int dupfd = fcntl(fd, F_DUPFD, 3);
    if (dupfd < 0) {
        fprintf(stderr, "[FATAL] %s: dupfd failed for fd %d: %s\n",
                who ? who : "unknown", fd, strerror(errno));
        abort();
    }
    close(fd);
    fprintf(stderr, "[FD-CHECK] %s: duplicated fd %d -> %d to avoid stdio collision\n",
           who ? who : "unknown", fd, dupfd);
    return dupfd;
}

int main(int argc, char *argv[]) {
    int opt;
    const char *input_path = NULL;
    int exit_code = 0;
    int stats_lock_initialized = 0;
    int input_fd = -1;
    int input_exhausted = 0;
    int w_option_explicit = 0;

    while ((opt = getopt(argc, argv, "k:m:w:p:u:n:a:L:P:R:E:G:H:J:K:SV:X:Q:qh")) != -1) {
        switch (opt) {
            case 'k': config.k = atoi(optarg); break;
            case 'm': config.m = atoi(optarg); break;
            case 'w': config.w = atoi(optarg); w_option_explicit = 1; break;
            case 'p': config.packetsize = atoi(optarg); break;
            case 'u': config.update_size = atoi(optarg); break;
            case 'n': config.n_updates = atoi(optarg); break;
            case 'a': config.alloc = optarg; break;
            case 'L': config.parix_dir = optarg; break;
            case 'P':
                config.plr_enabled = 1;
                config.plr_dir = optarg;
                break;
            case 'R':
                config.plr_enabled = 1;
                config.plr_reserved_bytes = (size_t)strtoull(optarg, NULL, 0);
                break;
            case 'E':
                config.plr_enabled = 1;
                config.plr_alpha = atof(optarg);
                if (config.plr_alpha <= 0.0 || config.plr_alpha > 1.0) {
                    fprintf(stderr, "Invalid PLR alpha %.3f, fallback to 0.25\n", config.plr_alpha);
                    config.plr_alpha = 0.25;
                }
                break;
            case 'G':
                config.plr_enabled = 1;
                config.plr_merge_interval = atol(optarg);
                if (config.plr_merge_interval < 0) config.plr_merge_interval = 0;
                break;
            case 'H':
                config.plr_enabled = 1;
                config.plr_shrink_interval = atol(optarg);
                if (config.plr_shrink_interval < 0) config.plr_shrink_interval = 0;
                break;
            case 'J':
                config.plr_enabled = 1;
                config.plr_expand_util = atof(optarg);
                break;
            case 'K':
                config.plr_enabled = 1;
                config.plr_shrink_util = atof(optarg);
                break;
            case 'Q':
                config.queue_depth = atoi(optarg);
                if (config.queue_depth <= 0) config.queue_depth = 64;
                break;
            case 'q':
                config.use_sqpoll = 1;
                break;
            case 'S': config.strong = 1; break;
            case 'V':
                config.verify = 1;
                if (optarg) {
                    config.verify_samples = atol(optarg);
                } else {
                    config.verify_samples = 0; // 0 or negative => full verify
                }
                break;
            case 'X':
                if (strcmp(optarg, "off") == 0) {
                    config.parix_enabled = 0;
                    config.parix_use_alloc = 0;
                } else if (strcmp(optarg, "parix") == 0) {
                    config.parix_enabled = 1;
                    config.parix_use_alloc = 0;
                } else if (strcmp(optarg, "parix+alloc") == 0) {
                    config.parix_enabled = 1;
                    config.parix_use_alloc = 1;
                } else {
                    fprintf(stderr, "Unknown PARIX mode: %s\n", optarg);
                    return 1;
                }
                break;
            case 'h':
            default:
                print_usage(argv[0]);
                return opt == 'h' ? 0 : 1;
        }
    }

    if (optind < argc) {
        input_path = argv[optind];
    }

    if (config.k <= 0) {
        fprintf(stderr, "Invalid data disk count (%d)\n", config.k);
        return 1;
    }
    if (config.m != 2) {
        fprintf(stderr, "[INFO] RDP fixes parity disks to 2. Overriding m=%d -> 2\n", config.m);
        config.m = 2;
    }

    int p_candidate;
    if (w_option_explicit) {
        p_candidate = config.w + 1;
        if (!is_prime(p_candidate)) {
            int new_p = next_prime(p_candidate);
            fprintf(stderr, "[INFO] Adjusting p from %d to prime %d to satisfy RDP geometry\n", p_candidate, new_p);
            p_candidate = new_p;
        }
        if (p_candidate <= config.k) {
            p_candidate = next_prime(config.k + 1);
            config.w = p_candidate - 1;
            fprintf(stderr, "[INFO] Adjusting w to %d so that p=%d > k=%d\n", config.w, p_candidate, config.k);
        } else {
            config.w = p_candidate - 1;
        }
    } else {
        p_candidate = next_prime(config.k);
        if (p_candidate <= config.k) {
            p_candidate = next_prime(config.k + 1);
        }
        config.w = p_candidate - 1;
    }
    config.p_prime = p_candidate;

    if (config.p_prime <= config.k) {
        fprintf(stderr, "RDP requires prime p > k. Got p=%d, k=%d\n", config.p_prime, config.k);
        return 1;
    }
    if (config.w <= 0 || config.w > 512) {
        fprintf(stderr, "Invalid w value (%d). RDP needs w = p-1.\n", config.w);
        return 1;
    }
    if (config.packetsize <= 0) {
        fprintf(stderr, "Invalid packet size (%d)\n", config.packetsize);
        return 1;
    }

    if (config.parix_enabled && config.parix_use_alloc && config.alloc && strcmp(config.alloc, "sequential") == 0) {
        config.alloc = "auto";
    }

    if (pthread_mutex_init(&stats.lock, NULL) != 0) {
        perror("pthread_mutex_init");
        return 1;
    }
    stats_lock_initialized = 1;

    size_t stripe_bytes = (size_t)config.w * (size_t)config.packetsize;
    int pool_capacity = (config.k + config.m) * 4;
    if (pool_capacity < 4) pool_capacity = 4;
    global_pool = create_memory_pool(pool_capacity, stripe_bytes);
    if (!global_pool) {
        fprintf(stderr, "Failed to initialise memory pool\n");
        exit_code = 1;
        goto cleanup;
    }

    char **data = NULL;
    char **coding = NULL;
    data = calloc((size_t)config.k, sizeof(char*));
    coding = calloc((size_t)config.m, sizeof(char*));
    if (!data || !coding) {
        fprintf(stderr, "Out of memory allocating buffer tables\n");
        if (data) {
            free(data);
            data = NULL;
        }
        if (coding) {
            free(coding);
            coding = NULL;
        }
        exit_code = 1;
        goto cleanup;
    }

    for (int i = 0; i < config.k; i++) {
        data[i] = pool_alloc(global_pool);
        if (!data[i]) {
            fprintf(stderr, "Failed to allocate data buffer %d\n", i);
            exit_code = 1;
            goto cleanup;
        }
    }
    for (int i = 0; i < config.m; i++) {
        coding[i] = pool_alloc(global_pool);
        if (!coding[i]) {
            fprintf(stderr, "Failed to allocate parity buffer %d\n", i);
            exit_code = 1;
            goto cleanup;
        }
    }

    if (config.parix_enabled) {
        g_parix_mode = config.parix_use_alloc ? PARIX_MODE_WITH_ALLOC : PARIX_MODE_BASIC;
        g_parix_ctx = parix_local_init(config.parix_dir, config.k, config.w, (size_t)config.packetsize, g_parix_mode);
        if (!g_parix_ctx) {
            fprintf(stderr, "Failed to initialise PARIX module (mode=%s)\n",
                    config.parix_use_alloc ? "parix+alloc" : "parix");
            config.parix_enabled = 0;
        }
    }

    if (config.plr_enabled) {
        size_t reserve_bytes = config.plr_reserved_bytes;
        size_t packet_bytes_local = (size_t)config.packetsize;
        if (reserve_bytes == 0) {
            reserve_bytes = stripe_bytes / 2;
            if (reserve_bytes < packet_bytes_local * 4) {
                reserve_bytes = packet_bytes_local * 4;
            }
        }
        plr_config_t plr_cfg = {
            .enabled = 1,
            .parity_count = config.m,
            .parity_bytes = stripe_bytes,
            .reserved_bytes = reserve_bytes,
            .ewma_alpha = (config.plr_alpha > 0.0 && config.plr_alpha <= 1.0) ? config.plr_alpha : 0.25,
            .merge_interval_updates = config.plr_merge_interval,
            .shrink_interval_updates = config.plr_shrink_interval,
            .expand_util_threshold = config.plr_expand_util,
            .shrink_util_threshold = config.plr_shrink_util,
            .base_dir = config.plr_dir ? config.plr_dir : "./plr_store",
            .file_prefix = "plr",
            .read_only = 0,
            .verbose = 0
        };
        int plr_rc = plr_context_create(&plr_cfg, &g_plr_ctx);
        if (plr_rc != 0) {
            fprintf(stderr, "[PLR] Failed to initialise PLR context (rc=%d). Disable PLR.\n", plr_rc);
            g_plr_ctx = NULL;
            config.plr_enabled = 0;
        } else {
            // 设置日志分片策略（PLR日志分片：根据stripe_index决定日志写入的extent）
            plr_set_log_striping(g_plr_ctx, config.alloc, g_total_disks);
            
            plr_rc = plr_start_merge_thread(g_plr_ctx, config.k);
            if (plr_rc != 0) {
                fprintf(stderr, "[PLR] Failed to start merge thread (rc=%d), continuing without background merge.\n", plr_rc);
            }
        }
    }

    if (input_path) {
        input_fd = open(input_path, O_RDONLY);
        if (input_fd < 0) {
            fprintf(stderr, "Failed to open %s: %s\n", input_path, strerror(errno));
            exit_code = 1;
            goto cleanup;
        }
    }

    for (int disk = 0; disk < config.k; disk++) {
        for (int packet = 0; packet < config.w; packet++) {
            char *dst = data[disk] + (size_t)packet * (size_t)config.packetsize;
            if (input_fd >= 0 && !input_exhausted) {
                ssize_t bytes = read(input_fd, dst, (size_t)config.packetsize);
                if (bytes < 0) {
                    fprintf(stderr, "Read error from %s: %s\n", input_path, strerror(errno));
                    exit_code = 1;
                    goto cleanup;
                } else if (bytes == 0) {
                    memset(dst, 0, (size_t)config.packetsize);
                    input_exhausted = 1;
                } else if (bytes < config.packetsize) {
                    memset(dst + bytes, 0, (size_t)config.packetsize - (size_t)bytes);
                    input_exhausted = 1;
                }
            } else if (input_fd >= 0 && input_exhausted) {
                memset(dst, 0, (size_t)config.packetsize);
            } else {
                for (int b = 0; b < config.packetsize; b++) {
                    dst[b] = (char)((disk + 1) * 17 + packet * 3 + b);
                }
            }
        }
    }

    if (input_fd >= 0) {
        close(input_fd);
        input_fd = -1;
    }

    rdp_encode(data, coding, config.k, config.w, config.packetsize);

    printf("RDP demo (k=%d, p=%d, w=%d, packetsize=%d)\n",
           config.k, config.p_prime, config.w, config.packetsize);
    printf("Allocation strategy: %s\n", config.alloc);
    if (config.parix_enabled) {
        printf("PARIX mode: %s\n", config.parix_use_alloc ? "parix+alloc" : "parix");
    }
    if (config.plr_enabled) {
        printf("PLR: enabled (dir=%s)\n", config.plr_dir ? config.plr_dir : "./plr_store");
    }

    printf("\nData block CRC32 digests:\n");
    for (int i = 0; i < config.k; i++) {
        uint32_t crc = crc32(0, (const unsigned char*)data[i], stripe_bytes);
        printf("  D%02d -> %08x\n", i, crc);
    }

    printf("Parity block CRC32 digests (RDP):\n");
    uint32_t row_crc = crc32(0, (const unsigned char*)coding[0], stripe_bytes);
    uint32_t diag_crc = crc32(0, (const unsigned char*)coding[1], stripe_bytes);
    printf("  Row Parity  (P) -> %08x\n", row_crc);
    printf("  Diag Parity (Q) -> %08x\n", diag_crc);

    // === 性能测试循环（如果指定了更新次数） ===
    if (config.n_updates > 0 && config.update_size > 0) {
        int blocks_per_update = config.update_size / config.packetsize;
        int io_ready = 0;
        int bufpool_ready = 0;
        int queue_depth = 256;
        char *update_payload = NULL;
        int force_mem = 0;

        const char *no_io_env = getenv("RDP_NO_IO");
        if (no_io_env && atoi(no_io_env) != 0) {
            force_mem = 1;
            fprintf(stderr, "[INFO] RDP_NO_IO set, skipping io_uring/device path\n");
        }

        if (blocks_per_update > 0 && !force_mem) {
            // 使用配置的队列深度，如果没有配置则使用默认值
            int actual_queue_depth = config.queue_depth > 0 ? config.queue_depth : queue_depth;
            
            // 计算所需的 ring 大小（SQPOLL 和普通模式都使用相同的计算）
            int ios_per_stripe = blocks_per_update * 2 + config.w * 4;
            int required_entries = actual_queue_depth * ios_per_stripe;
            required_entries = (required_entries * 3) / 2; // 增加 50% 安全余量
            // 向上取整到 2 的幂
            int ring_size = 256;
            while (ring_size < required_entries) ring_size *= 2;
            if (ring_size > 32768) ring_size = 32768; // 上限
            
            fprintf(stderr, "[INFO] Initializing io_uring with %d entries (for QD=%d, IOs/stripe=%d)\n",
                    ring_size, actual_queue_depth, ios_per_stripe);
            
            // 尝试启用 SQPOLL（如果配置允许）
            int sqpoll_success = 0;
            if (config.use_sqpoll) {
                struct io_uring_params params;
                memset(&params, 0, sizeof(params));
                params.flags = IORING_SETUP_SQPOLL;
                params.sq_thread_idle = 2000; // 2秒空闲后停止轮询
                
                if (io_uring_queue_init_params(ring_size, &g_ring, &params) == 0) {
                    sqpoll_success = 1;
                    g_ring_queue_depth = ring_size;
                    fprintf(stderr, "[INFO] SQPOLL enabled\n");
                } else {
                    fprintf(stderr, "[WARNING] SQPOLL not supported, falling back to default\n");
                }
            }
            
            if (!sqpoll_success) {
                if (io_uring_queue_init(ring_size, &g_ring, 0) == 0) {
                    sqpoll_success = 1;
                    g_ring_queue_depth = ring_size;
                }
            }
            
            if (sqpoll_success) {
                g_ring_ready = 1;
                // g_ring_queue_depth 已经在上面正确设置，不要再覆盖
                g_pending_ops = 0;
                if (open_and_register_disks(config.k, config.m) == 0) {
                    if (bufpool_init(&g_ring, actual_queue_depth, (size_t)config.packetsize) == 0) {
                        bufpool_ready = 1;
                        // 初始化 Context Pool
                        if (ctx_pool_init(actual_queue_depth) == 0) {
                            io_ready = 1;
                        }
                    }
                }
            }
        }

        struct timespec t0, t1;
        double secs = 0.0;
        long total_bytes = (long)config.n_updates * (long)config.update_size;
        double mbps = 0.0;
        double iops = 0.0;

        if (io_ready) {
            size_t payload_bytes = (size_t)blocks_per_update * (size_t)config.packetsize;
            update_payload = (char*)malloc(payload_bytes);
            if (!update_payload) {
                io_ready = 0;
            } else {
                for (int i = 0; i < blocks_per_update; i++) {
                    memset(update_payload + (size_t)i * (size_t)config.packetsize,
                           (unsigned char)(i & 0xff),
                           (size_t)config.packetsize);
                }

                clock_gettime(CLOCK_MONOTONIC, &t0);
                struct timespec last_progress = t0;
                
                // ========== 异步流水线主循环 ==========
                RDP_ATOMIC_STORE(g_completed_updates, 0);
                RDP_ATOMIC_STORE(g_inflight_updates, 0);
                int issued_updates = 0;
                
                fprintf(stderr, "[INFO] Starting async pipeline (queue_depth=%d, ring_queue_depth=%d)\n", 
                        config.queue_depth > 0 ? config.queue_depth : 256, g_ring_queue_depth);
                
                while (RDP_ATOMIC_LOAD(g_completed_updates) < (size_t)config.n_updates) {
                    // 1. 先处理所有可用的完成事件（CQE），腾出空间
                    struct io_uring_cqe *cqe;
                    unsigned head;
                    int count = 0;
                    
                    io_uring_for_each_cqe(&g_ring, head, cqe) {
                        count++;
                        uintptr_t ud = (uintptr_t)io_uring_cqe_get_data(cqe);
                        
                        // 检查是否是 PLR Header（通过阈值判断）
                        if (ud >= PLR_USER_DATA_THRESHOLD) {
                            plr_free_header_ctx_buffer((void*)ud);
                        } else {
                            // 这是 Context 相关的操作
                            int ctx_id = (int)ud;
                            if (ctx_id >= 0 && ctx_id < g_ctx_pool_size) {
                                update_ctx_t *ctx = &g_ctx_pool[ctx_id];
                                
                                if (cqe->res < 0) {
                                    fprintf(stderr, "[ERROR] IO error on stripe %ld: %s\n", 
                                            ctx->stripe_num, strerror(-cqe->res));
                                }
                                
                                ctx->pending_ios--;
                                
                                if (ctx->pending_ios == 0) {
                                    if (ctx->state == CTX_READING) {
                                        // 读完成 -> 转入写阶段
                                        async_process_read_done_and_submit_write(ctx, data, coding);
                                    } else if (ctx->state == CTX_WRITING) {
                                        // 写完成 -> 结束
                                        ctx_pool_put_free(ctx);
                                    }
                                }
                            }
                        }
                    }
                    
                    if (count > 0) {
                        io_uring_cq_advance(&g_ring, count);
                    }
                    
                    // 2. 提交新任务（填满流水线）
                    while (issued_updates < config.n_updates && RDP_ATOMIC_LOAD(g_inflight_updates) < (config.queue_depth > 0 ? config.queue_depth : 256)) {
                        update_ctx_t *ctx = ctx_pool_get_free();
                        if (!ctx) break; // 没有空闲上下文
                        
                        async_submit_read_stage(ctx, issued_updates, update_payload);
                        issued_updates++;
                        RDP_ATOMIC_INC(g_inflight_updates);
                    }
                    
                    // 3. 提交所有准备好的 SQE
                    int submit_rc = io_uring_submit(&g_ring);
                    if (submit_rc < 0 && submit_rc != -EBUSY && submit_rc != -EAGAIN) {
                        fprintf(stderr, "[ERROR] io_uring_submit failed: %s\n", strerror(-submit_rc));
                        break;
                    }
                    
                    // 4. 如果没有处理任何 CQE 且还有 inflight 操作，等待至少一个完成
                    if (count == 0 && RDP_ATOMIC_LOAD(g_inflight_updates) > 0) {
                        io_uring_wait_cqe(&g_ring, &cqe);
                        // 下一轮循环会处理这个 CQE
                    }
                    
                    // 3. 进度输出
                    if (RDP_ATOMIC_LOAD(g_completed_updates) % 1000 == 0 || RDP_ATOMIC_LOAD(g_completed_updates) == 0) {
                        struct timespec now;
                        clock_gettime(CLOCK_MONOTONIC, &now);
                        double elapsed = (now.tv_sec - last_progress.tv_sec) + (now.tv_nsec - last_progress.tv_nsec) / 1e9;
                        size_t completed_count = RDP_ATOMIC_LOAD(g_completed_updates);
                        int inflight_count = RDP_ATOMIC_LOAD(g_inflight_updates);
                        if (elapsed >= 5.0 || completed_count % 1000 == 0) {
                            double total_elapsed = (now.tv_sec - t0.tv_sec) + (now.tv_nsec - t0.tv_nsec) / 1e9;
                            double progress = ((double)completed_count / (double)config.n_updates) * 100.0;
                            double est_total = total_elapsed / ((double)completed_count / (double)config.n_updates);
                            double est_remaining = est_total - total_elapsed;
                            if (est_total < total_elapsed) est_remaining = 0.0;
                            fprintf(stderr, "[PROGRESS] %zu/%d (%.1f%%) | elapsed=%.1fs | est_remaining=%.1fs | inflight=%d\n",
                                    completed_count, config.n_updates, progress, total_elapsed, est_remaining, inflight_count);
                            last_progress = now;
                        }
                    }
                }
                
                // 等待所有剩余的 I/O 完成
                while (RDP_ATOMIC_LOAD(g_inflight_updates) > 0) {
                    struct io_uring_cqe *cqe;
                    if (io_uring_wait_cqe(&g_ring, &cqe) == 0) {
                        uintptr_t ud = (uintptr_t)io_uring_cqe_get_data(cqe);
                        if (ud >= PLR_USER_DATA_THRESHOLD) {
                            plr_free_header_ctx_buffer((void*)ud);
                        } else {
                            int ctx_id = (int)ud;
                            if (ctx_id >= 0 && ctx_id < g_ctx_pool_size) {
                                update_ctx_t *ctx = &g_ctx_pool[ctx_id];
                                ctx->pending_ios--;
                                if (ctx->pending_ios == 0) {
                                    if (ctx->state == CTX_READING) {
                                        async_process_read_done_and_submit_write(ctx, data, coding);
                                        io_uring_submit(&g_ring);
                                    } else if (ctx->state == CTX_WRITING) {
                                        ctx_pool_put_free(ctx);
                                    }
                                }
                            }
                        }
                        io_uring_cqe_seen(&g_ring, cqe);
                    }
                }
                if (g_pending_ops > 0) {
                    fprintf(stderr, "[INFO] Waiting for %zu pending operations to complete...\n", g_pending_ops);
                    // 强制等待所有 CQE，但设置最大等待次数
                    int max_wait_rounds = 100;
                    int wait_rounds = 0;
                    while (g_pending_ops > 0 && wait_rounds < max_wait_rounds) {
                        wait_rounds++;
                        size_t old_pending = g_pending_ops;
                        wait_cqes_n(&g_ring, (int)(g_pending_ops > 32 ? 32 : g_pending_ops));
                        // 如果 pending_ops 没有减少，强制清零
                        if (g_pending_ops >= old_pending && wait_rounds > 10) {
                            fprintf(stderr, "[WARNING] pending_ops not decreasing, forcing cleanup (was %zu)\n", g_pending_ops);
                            // 尝试再等待一次，然后强制清零
                            struct io_uring_cqe *cqe;
                            for (int i = 0; i < 100; i++) {
                                if (io_uring_peek_cqe(&g_ring, &cqe) == 0 && cqe) {
                                    void *ud = io_uring_cqe_get_data(cqe);
                                    if (ud) {
                                        uintptr_t v = (uintptr_t)ud;
                                        if (v >= PLR_USER_DATA_THRESHOLD) {
                                            plr_free_header_ctx_buffer(ud);
                                        }
                                    }
                                    io_uring_cqe_seen(&g_ring, cqe);
                                    if (g_pending_ops > 0) g_pending_ops--;
                                } else {
                                    break;
                                }
                            }
                            break;
                        }
                    }
                    if (g_pending_ops > 0) {
                        fprintf(stderr, "[WARNING] Still have %zu pending operations after cleanup\n", g_pending_ops);
                    }
                    g_pending_ops = 0;
                }
                fsync_all_devices(&g_ring, g_fds, g_total_disks);
                clock_gettime(CLOCK_MONOTONIC, &t1);

                secs = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;
            }
        }

        if (!io_ready) {
            if (g_ring_ready) {
                if (g_pending_ops > 0) {
                    wait_cqes_n(&g_ring, (int)g_pending_ops);
                    g_pending_ops = 0;
                }
                close_all_disks();
                io_uring_queue_exit(&g_ring);
                g_ring_ready = 0;
                g_ring_queue_depth = 0;
            }
            if (bufpool_ready) {
                bufpool_destroy();
                bufpool_ready = 0;
            }

            fprintf(stderr, "警告: I/O 路径不可用或初始化失败，回退至内存基准模式。\n");
            clock_gettime(CLOCK_MONOTONIC, &t0);
            for (int rep = 0; rep < config.n_updates; rep++) {
                update_rdp_simple(data, coding, NULL, NULL, 0, NULL, rep, NULL);
            }
            clock_gettime(CLOCK_MONOTONIC, &t1);
            secs = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;
        }

        if (secs > 0.0) {
            mbps = total_bytes / (1024.0 * 1024.0) / secs;
            iops = ((double)config.n_updates * (config.update_size / config.packetsize)) / secs;
        }

        printf("[RESULT] elapsed=%.6fs, bytes=%ld, throughput=%.2f MB/s, IOPS=%.0f\n",
               secs, total_bytes, mbps, iops);

        if (update_payload) {
            free(update_payload);
        }
        if (io_ready) {
            if (bufpool_ready) {
                bufpool_destroy();
                bufpool_ready = 0;
            }
            if (g_pending_ops > 0) {
                fprintf(stderr, "[INFO] Final cleanup: waiting for %zu pending operations...\n", g_pending_ops);
                int max_wait_rounds = 100;
                int wait_rounds = 0;
                while (g_pending_ops > 0 && wait_rounds < max_wait_rounds) {
                    wait_rounds++;
                    size_t old_pending = g_pending_ops;
                    wait_cqes_n(&g_ring, (int)(g_pending_ops > 32 ? 32 : g_pending_ops));
                    if (g_pending_ops >= old_pending && wait_rounds > 10) {
                        // 强制清理剩余的 CQE
                        struct io_uring_cqe *cqe;
                        for (int i = 0; i < 100; i++) {
                            if (io_uring_peek_cqe(&g_ring, &cqe) == 0 && cqe) {
                                void *ud = io_uring_cqe_get_data(cqe);
                                if (ud) {
                                    uintptr_t v = (uintptr_t)ud;
                                    if (v >= PLR_USER_DATA_THRESHOLD) {
                                        plr_free_header_ctx_buffer(ud);
                                    }
                                }
                                io_uring_cqe_seen(&g_ring, cqe);
                                if (g_pending_ops > 0) g_pending_ops--;
                            } else {
                                break;
                            }
                        }
                        break;
                    }
                }
                if (g_pending_ops > 0) {
                    fprintf(stderr, "[WARNING] Final cleanup: still have %zu pending operations\n", g_pending_ops);
                }
                g_pending_ops = 0;
            }
            close_all_disks();
            if (g_ring_ready) {
                io_uring_queue_exit(&g_ring);
                g_ring_ready = 0;
                g_ring_queue_depth = 0;
            }
        }
    }

    // === 一致性校验（可选，-V 开启） ===
    if (config.verify) {
        // 分配临时校验缓冲区，重新编码并与当前 coding 比较
        char *tmp_coding[2] = { NULL, NULL };
        int verify_ok = 1;

        for (int i = 0; i < 2; i++) {
            tmp_coding[i] = pool_alloc(global_pool);
            if (!tmp_coding[i]) {
                fprintf(stderr, "[VERIFY] failed to alloc temp parity buffer %d\n", i);
                verify_ok = 0;
                break;
            }
            memset(tmp_coding[i], 0, stripe_bytes);
        }

        if (verify_ok) {
            rdp_encode(data, tmp_coding, config.k, config.w, config.packetsize);
            for (int i = 0; i < 2; i++) {
                if (memcmp(tmp_coding[i], coding[i], stripe_bytes) != 0) {
                    verify_ok = 0;
                    break;
                }
            }
        }

        if (verify_ok) {
            printf("[VERIFY] OK: incremental parity matches full re-encode\n");
        } else {
            printf("[VERIFY] FAIL: mismatch between incremental parity and re-encode\n");
            exit_code = exit_code ? exit_code : 2;
        }

        for (int i = 0; i < 2; i++) {
            if (tmp_coding[i] && global_pool) {
                pool_free(global_pool, tmp_coding[i]);
            }
        }
    }

    if (g_parix_ctx) {
        if (parix_local_replay(g_parix_ctx) != 0) {
            fprintf(stderr, "PARIX replay failed\n");
        }
    }

cleanup:
    if (input_fd >= 0) {
        close(input_fd);
    }
    if (g_ring_ready && g_pending_ops > 0) {
        wait_cqes_n(&g_ring, (int)g_pending_ops);
        g_pending_ops = 0;
    }
    close_all_disks();
    if (g_ring_ready) {
        io_uring_queue_exit(&g_ring);
        g_ring_ready = 0;
    }
    g_ring_queue_depth = 0;
    if (g_bufpool.capacity > 0) {
        bufpool_destroy();
    }
    if (g_ctx_pool_size > 0) {
        ctx_pool_destroy();
    }
    if (coding) {
        for (int i = 0; i < config.m; i++) {
            if (coding[i] && global_pool) {
                pool_free(global_pool, coding[i]);
            }
        }
        free(coding);
    }
    if (data) {
        for (int i = 0; i < config.k; i++) {
            if (data[i] && global_pool) {
                pool_free(global_pool, data[i]);
            }
        }
        free(data);
    }
    if (global_pool) {
        destroy_memory_pool(global_pool);
        global_pool = NULL;
    }
    if (g_plr_ctx) {
        plr_flush_metadata(g_plr_ctx);
        plr_context_destroy(g_plr_ctx);
        g_plr_ctx = NULL;
    }
    free(g_plr_row_delta); g_plr_row_delta = NULL;
    free(g_plr_diag_delta); g_plr_diag_delta = NULL;
    free(g_plr_s_delta); g_plr_s_delta = NULL;
    free(g_plr_delta_tmp); g_plr_delta_tmp = NULL; g_plr_delta_tmp_capacity = 0;
    free(g_plr_row_mask); g_plr_row_mask = NULL;
    free(g_plr_diag_mask); g_plr_diag_mask = NULL;
    free(g_plr_old_blocks); g_plr_old_blocks = NULL; g_plr_old_capacity = 0;
    free(g_plr_old_ptrs); g_plr_old_ptrs = NULL; g_plr_old_ptr_capacity = 0;
    g_plr_delta_capacity = 0;
    g_plr_mask_capacity = 0;
    if (g_parix_ctx) {
        parix_local_shutdown(g_parix_ctx);
        g_parix_ctx = NULL;
    }
    free(g_hybrid_plan_cache); g_hybrid_plan_cache = NULL; g_hybrid_plan_len = 0;
    g_hybrid_plan_name[0] = '\0'; g_hybrid_plan_alloc[0] = '\0';
    g_hybrid_plan_update_size = 0; g_hybrid_plan_k = 0; g_hybrid_plan_w = 0;
    free(g_io_buffers_cache); g_io_buffers_cache = NULL;
    free(g_io_offsets_cache); g_io_offsets_cache = NULL;
    free(g_io_fds_cache); g_io_fds_cache = NULL;
    g_io_cache_capacity = 0;
    if (stats_lock_initialized) {
        pthread_mutex_destroy(&stats.lock);
    }
    return exit_code;
}
