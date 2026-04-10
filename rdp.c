#define _GNU_SOURCE
#define _POSIX_C_SOURCE 199309L
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
    .parix_dir = NULL
};

perf_stats_t stats = {0};
memory_pool_t *global_pool = NULL;

// 记录本次更新触及的条带集合（用于最终轻量修复）
static long *g_touched_stripes = NULL;
static RDP_ATOMIC_INT g_touched_n = ATOMIC_VAR_INIT(0);
static int g_touched_cap = 0;
static pthread_mutex_t g_touched_lock = PTHREAD_MUTEX_INITIALIZER;

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

    if (g_ring_ready) {
        if (io_uring_register_files(&g_ring, g_fds, want) == 0) {
            g_fixed.enabled = 1;
        } else {
            perror("io_uring_register_files");
            g_fixed.enabled = 0;
        }
    }

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
        if (g_fixed.enabled) {
            io_uring_sqe_set_flags(sqes[i], IOSQE_FIXED_FILE | IOSQE_IO_LINK);
        }

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
                    if (g_fixed.enabled) io_uring_sqe_set_flags(retry_sqe, IOSQE_FIXED_FILE);
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
        if (g_fixed.enabled) io_uring_sqe_set_flags(sqes[i], IOSQE_FIXED_FILE);
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
        if (g_fixed.enabled) io_uring_sqe_set_flags(sqes[i], IOSQE_FIXED_FILE);
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
        if (g_fixed.enabled) io_uring_sqe_set_flags(sqes[i], IOSQE_FIXED_FILE);
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
            if (g_fixed.enabled) io_uring_sqe_set_flags(sqes[i], IOSQE_FIXED_FILE);
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
            if (g_fixed.enabled) io_uring_sqe_set_flags(sqes[i], IOSQE_FIXED_FILE);
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
    for (int i = 0; i < count; i++) {
        struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
        if (!sqe) { fprintf(stderr, "Failed to get sqe\n"); exit(1); }
        if (g_bufpool.enabled && buf_indices && buf_indices[i] >= 0) {
            io_uring_prep_write_fixed(sqe, fds[i], buffers[i], size, offsets[i], buf_indices[i]);
        } else {
            if (g_fixed.enabled) io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);
            io_uring_prep_write(sqe, fds[i], buffers[i], size, offsets[i]);
        }
    }
    io_uring_submit(ring);
    return count;
}

// 等待 n 个 CQE 完成
static void wait_cqes_n(struct io_uring *ring, int n) {
    struct io_uring_cqe *cqe;
    for (int completed = 0; completed < n; completed++) {
        if (io_uring_wait_cqe(ring, &cqe) < 0) { perror("io_uring_wait_cqe"); exit(1); }
        if (cqe->res < 0) {
            fprintf(stderr, "io cqe error: %s\n", strerror(abs(cqe->res)));
            exit(1);
        }
        io_uring_cqe_seen(ring, cqe);
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
        if (g_fixed.enabled) io_uring_sqe_set_flags(sqes[i], IOSQE_FIXED_FILE);
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

    if (plan_is_sequential && ring && RDP_ATOMIC_LOAD(g_pending_ops) > 0) {
        wait_cqes_n(ring, (int)RDP_ATOMIC_LOAD(g_pending_ops));
        RDP_ATOMIC_STORE(g_pending_ops, 0);
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
                size_t current_pending = RDP_ATOMIC_LOAD(g_pending_ops);
                current_pending += (size_t)blocks_to_update;
                RDP_ATOMIC_STORE(g_pending_ops, current_pending);
                
                size_t threshold = (g_ring_queue_depth > 0) ? (size_t)g_ring_queue_depth : 128;
                if (current_pending >= threshold && ring) {
                    size_t target = current_pending - threshold / 2;
                    if (target < 1 || target > current_pending) target = current_pending;
                    wait_cqes_n(ring, (int)target);
                    current_pending -= target;
                    RDP_ATOMIC_STORE(g_pending_ops, current_pending);
                }
            } else {
                batch_io_write_with_offsets(buffers, req_fds, offsets, blocks_to_update, ring, (size_t)config.packetsize);
            }
        }
    } else {
        // Fallback：在内存中简单 XOR，避免空循环
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

// RDP 更新包装：沿用通用 I/O 调度逻辑，语义上标识为 RDP 路径
void update_rdp_simple(char **data, char **coding, char *new_data,
                       int *fds, int total_disks, struct io_uring *ring,
                       long stripe_num, long *next_stripe_cursor) {
    int blocks_to_update = config.update_size / config.packetsize;
    if (blocks_to_update <= 0) return;

    // 准备待写入的数据负载（与 I/O 路径复用）
    char *payload = new_data;
    int allocated_payload = 0;
    if (!payload) {
        void *tmp = NULL;
        size_t payload_bytes = (size_t)blocks_to_update * (size_t)config.packetsize;
        if (posix_memalign(&tmp, 4096, payload_bytes) != 0) {
            return;
        }
        payload = (char*)tmp;
        allocated_payload = 1;
        for (int i = 0; i < blocks_to_update; i++) {
            memset(payload + (size_t)i * (size_t)config.packetsize,
                   (unsigned char)((stripe_num + i) & 0xff),
                   (size_t)config.packetsize);
        }
    }

    // 先复用原有 I/O 调度路径，确保磁盘写入行为保持一致
    int saved_compute_flag = g_update_evenodd_compute_enabled;
    g_update_evenodd_compute_enabled = 0; // 禁止 fallback 里的内存 XOR，避免重复计算
    update_evenodd_simple(data, coding, payload, fds, total_disks, ring, stripe_num, next_stripe_cursor);
    g_update_evenodd_compute_enabled = saved_compute_flag;

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

    // RDP 增量计算（Small-Write 近似：直接将 payload 视作 delta）
    struct timespec upd_start_ts, upd_end_ts;
    clock_gettime(CLOCK_MONOTONIC, &upd_start_ts);

    int p = (config.p_prime > 0) ? config.p_prime : (config.w + 1);
    for (int i = 0; i < blocks_to_update; i++) {
        int r = plan[i].row % config.w;
        if (r < 0) r += config.w;
        int c = plan[i].col % config.k;
        if (c < 0) c += config.k;

        char *delta_src = payload + (size_t)i * (size_t)config.packetsize;

        // 同步更新内存中的数据块，便于后续 -V 校验时重编码一致
        if (data && data[c]) {
            char *d_dst = data[c] + (size_t)r * (size_t)config.packetsize;
            xor_update_simd(d_dst, delta_src, config.packetsize);
        }

        // 行校验更新
        if (coding && coding[0]) {
            char *p_dst = coding[0] + (size_t)r * (size_t)config.packetsize;
            xor_update_simd(p_dst, delta_src, config.packetsize);
        }

        // 对角校验更新
        if (coding && coding[1]) {
            int d = r + c;
            if (d >= p) d -= p;
            if (d < config.w) {
                char *q_dst = coding[1] + (size_t)d * (size_t)config.packetsize;
                xor_update_simd(q_dst, delta_src, config.packetsize);
            }
        }
    }

    /* Physical P and Q writes with parity rotation (skip when PARIX owns parity). */
    if (!g_parix_ctx && ring && fds && total_disks > 0 && coding && coding[0] && coding[1]) {
        size_t stripe_bytes = (size_t)config.w * (size_t)config.packetsize;
        off_t parity_off = (off_t)stripe_num * (off_t)config.w * (off_t)config.packetsize;
        int physical_c_p = map_physical_disk(config.k, stripe_num, total_disks, config.alloc);
        int physical_c_q = map_physical_disk(config.k + 1, stripe_num, total_disks, config.alloc);
        if (physical_c_p < 0) physical_c_p = 0;
        if (physical_c_p >= total_disks) physical_c_p %= total_disks;
        if (physical_c_q < 0) physical_c_q = 0;
        if (physical_c_q >= total_disks) physical_c_q %= total_disks;

        struct io_uring_sqe *sqe_p = io_uring_get_sqe(ring);
        struct io_uring_sqe *sqe_q = io_uring_get_sqe(ring);
        if (sqe_p && sqe_q) {
            if (g_fixed.enabled) io_uring_sqe_set_flags(sqe_p, IOSQE_FIXED_FILE);
            io_uring_prep_write(sqe_p, fds[physical_c_p], coding[0], stripe_bytes, parity_off);
            io_uring_sqe_set_data(sqe_p, (void*)(uintptr_t)(-1));
            if (g_fixed.enabled) io_uring_sqe_set_flags(sqe_q, IOSQE_FIXED_FILE);
            io_uring_prep_write(sqe_q, fds[physical_c_q], coding[1], stripe_bytes, parity_off);
            io_uring_sqe_set_data(sqe_q, (void*)(uintptr_t)(-2));
            io_uring_submit(ring);
            size_t current = RDP_ATOMIC_LOAD(g_pending_ops);
            current += 2;
            RDP_ATOMIC_STORE(g_pending_ops, current);
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &upd_end_ts);
    double elapsed = timespec_diff_sec(&upd_start_ts, &upd_end_ts);
    pthread_mutex_lock(&stats.lock);
    stats.compute_time += elapsed;
    stats.update_count++;
    pthread_mutex_unlock(&stats.lock);

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
    fprintf(stderr, "  -a <policy>   Address allocation policy (sequential,optimized,row,diag,auto,...)\n");
    fprintf(stderr, "  -L <dir>      Working directory for PARIX logs (default ./parix_local)\n");
    fprintf(stderr, "  -S            Enable strong consistency revalidation flag\n");
    fprintf(stderr, "  -V <count>    Enable verification (sample stripes, <=0 for full)\n");
    fprintf(stderr, "  -X <mode>     PARIX mode: off|parix|parix+alloc (default off)\n");
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

    while ((opt = getopt(argc, argv, "k:m:w:p:u:n:a:L:SV:X:h")) != -1) {
        switch (opt) {
            case 'k': config.k = atoi(optarg); break;
            case 'm': config.m = atoi(optarg); break;
            case 'w': config.w = atoi(optarg); w_option_explicit = 1; break;
            case 'p': config.packetsize = atoi(optarg); break;
            case 'u': config.update_size = atoi(optarg); break;
            case 'n': config.n_updates = atoi(optarg); break;
            case 'a': config.alloc = optarg; break;
            case 'L': config.parix_dir = optarg; break;
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
            if (io_uring_queue_init(queue_depth, &g_ring, 0) == 0) {
                g_ring_ready = 1;
                g_ring_queue_depth = queue_depth;
                g_pending_ops = 0;
                if (open_and_register_disks(config.k, config.m) == 0) {
                    if (bufpool_init(&g_ring, queue_depth, (size_t)config.packetsize) == 0) {
                        bufpool_ready = 1;
                        io_ready = 1;
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
                for (int rep = 0; rep < config.n_updates; rep++) {
                    update_rdp_simple(data, coding, update_payload, g_fds, g_total_disks, &g_ring, rep, NULL);
                }
                if (g_pending_ops > 0) {
                    wait_cqes_n(&g_ring, (int)g_pending_ops);
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
                wait_cqes_n(&g_ring, (int)g_pending_ops);
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
