#define _GNU_SOURCE
#define _POSIX_C_SOURCE 199309L
#define PARIX_USER_DATA_THRESHOLD 10000000
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

// ========== PARIX 核心数据结构 ==========

// PARIX Journal Entry 类型
typedef enum {
    PARIX_ENTRY_BASE = 0,      // D0 (初始值)
    PARIX_ENTRY_UPDATE = 1,    // D(r) (更新值)
    PARIX_ENTRY_DELTA = 2      // Delta (优化：直接存增量)
} parix_entry_type_t;

// PARIX Journal Header
typedef struct {
    uint32_t magic;            // 魔数: 0x50415258 ("PARX")
    uint32_t checksum;         // CRC32校验和
    uint64_t stripe_id;        // 条带ID
    uint32_t version;          // 版本号
    parix_entry_type_t type;   // 条目类型
    uint32_t payload_size;     // 负载大小
    uint64_t timestamp;        // 时间戳
} __attribute__((packed)) parix_journal_header_t;

// PARIX Stripe 索引（内存中）
typedef struct {
    uint64_t stripe_id;
    bool has_d0;               // 是否已有基准值 D0
    off_t d0_offset;           // D0 在 Journal 中的偏移量
    off_t latest_offset;       // 最新数据在 Journal 中的偏移量
    uint32_t version;          // 当前版本号
    int update_count;          // 更新次数（用于触发重放）
} parix_stripe_index_t;

// PARIX Journal 管理器（每个 Parity 盘一个）
typedef struct {
    int parity_id;             // Parity 盘 ID (0=Row, 1=Diag)
    int journal_fd;            // Journal 文件描述符
    char *journal_path;        // Journal 路径
    off_t journal_offset;      // 当前写入偏移量
    
    // 索引管理（简化实现：使用数组+线性查找，生产环境应用 hash table）
    parix_stripe_index_t *index;
    int index_capacity;
    int index_count;
    
    // 统计信息
    uint64_t total_writes;
    uint64_t speculative_hits;
    uint64_t speculative_misses;
    
    pthread_mutex_t lock;
} parix_journal_t;

// PARIX 全局上下文
typedef struct {
    char *base_dir;            // 基础目录
    int k;                     // 数据盘数量
    int w;                     // 行数
    size_t packetsize;         // 数据包大小
    
    parix_journal_t *journals; // Journal 数组 (每个 parity 盘一个)
    int journal_count;         // Journal 数量 (m=2 for RDP)
    
    // 重放控制
    int replay_threshold;      // 触发重放的更新次数阈值
    int auto_replay;           // 是否自动重放
    
    pthread_mutex_t global_lock;
} parix_context_t;

// ========== 配置结构 ==========

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
    
    // PARIX 配置
    int parix_enabled;  // 是否启用 PARIX
    char *parix_dir;    // PARIX Journal 目录
    int parix_replay_threshold; // 重放阈值
    
    // 异步流水线配置
    int queue_depth;            // 并发上下文数量（流水线深度）
    int use_sqpoll;            // 是否启用 SQPOLL（内核轮询）
} config_t;

// 增强版性能统计
typedef struct {
    double total_io_time;
    double compute_time;
    int read_count;
    int write_count;
    double total_latency;
    int update_count;
    long long xor_count;
    pthread_mutex_t lock;
} perf_stats_t;

// 增强版内存池
typedef struct memory_pool {
    void **buffers;
    int *free_list;
    int free_count;
    int capacity;
    size_t buffer_size;
    pthread_mutex_t lock;
    uint64_t total_allocated;
    uint64_t peak_usage;
} memory_pool_t;

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
    .parix_enabled = 0,
    .parix_dir = NULL,
    .parix_replay_threshold = 100,
    .queue_depth = 64,
    .use_sqpoll = 0
};

perf_stats_t stats = {0};
memory_pool_t *global_pool = NULL;

// io_uring 全局状态
static struct io_uring g_ring;
static int g_ring_ready = 0;
static int *g_fds = NULL;
static int g_total_disks = 0;
static size_t g_pending_ops = 0;
static int g_ring_queue_depth = 0;

// PARIX 全局上下文
static parix_context_t *g_parix_ctx = NULL;

// 前向声明
void xor_update_simd(char *dst, const char *src, size_t size);
static uint32_t crc32(uint32_t crc, const unsigned char *buf, size_t len);
static int load_safe_disk_paths(char **disk_paths, int max_disks);
static int ensure_nonstdio_fd(int fd, const char *who);
static int open_and_register_disks(int k, int m);
static void close_all_disks(void);

// ========== 辅助函数 ==========

static inline double timespec_diff_sec(const struct timespec *start, const struct timespec *end) {
    return (end->tv_sec - start->tv_sec) + (end->tv_nsec - start->tv_nsec) / 1e9;
}

static inline off_t block_offset_bytes(long stripe_idx, int row, int packetsize) {
    return (off_t)((stripe_idx * (long)config.w + row) * (long)packetsize);
}

static inline int device_index_for_col(int col, int k, int m) {
    (void)k; (void)m; return col;
}

// ================= 核心优化：强力地址映射算法 =================
// 使用大素数 997 进行散列，确保相邻 Stripe 的物理落盘位置完全打散
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

// 素数检查
static int is_prime(int n) {
    if (n <= 1) return 0;
    if (n == 2) return 1;
    if (n % 2 == 0) return 0;
    for (int i = 3; i * i <= n; i += 2) {
        if (n % i == 0) return 0;
    }
    return 1;
}

static int next_prime(int n) {
    if (n <= 1) return 2;
    int candidate = n + 1;
    while (1) {
        if (is_prime(candidate)) return candidate;
        candidate++;
    }
}

// CRC32 实现
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

// ========== SIMD XOR 实现 ==========

void xor_update_simd(char *dst, const char *src, size_t size) {
    if (!dst || !src || size == 0) return;
    
    size_t i = 0;
    
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
        *(uint64_t*)(dst + i) ^= *(uint64_t*)(src + i);
    }

    // 剩余字节
    for (; i < size; i++) {
        dst[i] ^= src[i];
    }
}

// ========== 内存池实现 ==========

memory_pool_t* create_memory_pool(int capacity, size_t buffer_size) {
    memory_pool_t *pool = malloc(sizeof(memory_pool_t));
    if (!pool) {
        fprintf(stderr, "[MEMPOOL] Failed to allocate pool structure\n");
        return NULL;
    }
    
    memset(pool, 0, sizeof(memory_pool_t));
    pool->capacity = capacity;
    pool->buffer_size = buffer_size;
    pool->free_count = capacity;
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
    
    for (int i = 0; i < capacity; i++) {
        if (posix_memalign(&pool->buffers[i], 4096, buffer_size) != 0) {
            fprintf(stderr, "[MEMPOOL] Failed to allocate buffer %d (%zu bytes)\n", i, buffer_size);
            for (int j = 0; j < i; j++) {
                free(pool->buffers[j]);
            }
            free(pool->buffers);
            free(pool->free_list);
            pthread_mutex_destroy(&pool->lock);
            free(pool);
            return NULL;
        }
        memset(pool->buffers[i], 0, buffer_size);
        pool->free_list[i] = i;
    }
    
    pool->total_allocated = capacity * buffer_size;
    pool->peak_usage = pool->total_allocated;
    
    return pool;
}

void* pool_alloc(memory_pool_t *pool) {
    if (!pool) return NULL;
    
    pthread_mutex_lock(&pool->lock);
    
    if (pool->free_count == 0) {
        pthread_mutex_unlock(&pool->lock);
        void *buffer;
        if (posix_memalign(&buffer, 4096, pool->buffer_size) != 0) {
            return NULL;
        }
        memset(buffer, 0, pool->buffer_size);
        return buffer;
    }
    
    int idx = pool->free_list[--pool->free_count];
    void *buffer = pool->buffers[idx];
    
    pthread_mutex_unlock(&pool->lock);
    return buffer;
}

void pool_free(memory_pool_t *pool, void *buffer) {
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
        memset(buffer, 0, pool->buffer_size);
        pthread_mutex_unlock(&pool->lock);
    } else {
        pthread_mutex_unlock(&pool->lock);
        free(buffer);
    }
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

// ========== PARIX 核心实现 ==========

// 创建 PARIX Journal
static parix_journal_t* parix_journal_create(const char *base_dir, int parity_id, int initial_capacity) {
    parix_journal_t *journal = (parix_journal_t*)calloc(1, sizeof(parix_journal_t));
    if (!journal) {
        fprintf(stderr, "[PARIX] Failed to allocate journal structure\n");
        return NULL;
    }
    
    journal->parity_id = parity_id;
    journal->index_capacity = initial_capacity;
    journal->index_count = 0;
    
    // 分配索引数组
    journal->index = (parix_stripe_index_t*)calloc((size_t)initial_capacity, sizeof(parix_stripe_index_t));
    if (!journal->index) {
        fprintf(stderr, "[PARIX] Failed to allocate index array (capacity=%d)\n", initial_capacity);
        free(journal);
        return NULL;
    }
    
    // 创建 Journal 文件
    char path[512];
    snprintf(path, sizeof(path), "%s/parix_journal_p%d.log", base_dir, parity_id);
    journal->journal_path = strdup(path);
    
    journal->journal_fd = open(path, O_RDWR | O_CREAT | O_APPEND, 0644);
    if (journal->journal_fd < 0) {
        perror(path);
        free(journal->index);
        free(journal->journal_path);
        free(journal);
        return NULL;
    }
    
    // 获取当前文件大小
    struct stat st;
    if (fstat(journal->journal_fd, &st) == 0) {
        journal->journal_offset = st.st_size;
    }
    
    pthread_mutex_init(&journal->lock, NULL);
    
    fprintf(stderr, "[PARIX] Created journal for parity %d at %s (offset=%ld)\n", 
            parity_id, path, (long)journal->journal_offset);
    
    return journal;
}

// 查找 Stripe 索引
static parix_stripe_index_t* parix_find_stripe(parix_journal_t *journal, uint64_t stripe_id) {
    for (int i = 0; i < journal->index_count; i++) {
        if (journal->index[i].stripe_id == stripe_id) {
            return &journal->index[i];
        }
    }
    return NULL;
}

// 添加或更新 Stripe 索引
static parix_stripe_index_t* parix_get_or_create_stripe(parix_journal_t *journal, uint64_t stripe_id) {
    parix_stripe_index_t *stripe = parix_find_stripe(journal, stripe_id);
    if (stripe) return stripe;
    
    // 扩容检查
    if (journal->index_count >= journal->index_capacity) {
        int new_cap = journal->index_capacity * 2;
        parix_stripe_index_t *new_index = (parix_stripe_index_t*)realloc(
            journal->index, sizeof(parix_stripe_index_t) * (size_t)new_cap);
        if (!new_index) return NULL;
        journal->index = new_index;
        journal->index_capacity = new_cap;
    }
    
    // 添加新条目
    stripe = &journal->index[journal->index_count++];
    memset(stripe, 0, sizeof(parix_stripe_index_t));
    stripe->stripe_id = stripe_id;
    
    return stripe;
}

// 写入 Journal 条目（同步版本，用于兼容性）
static int parix_journal_append_sync(parix_journal_t *journal, uint64_t stripe_id, 
                                     parix_entry_type_t type, const char *data, size_t data_size) {
    pthread_mutex_lock(&journal->lock);
    
    // 构造 Header
    parix_journal_header_t header;
    header.magic = 0x50415258; // "PARX"
    header.stripe_id = stripe_id;
    header.version = 0; // TODO: 实现版本控制
    header.type = type;
    header.payload_size = (uint32_t)data_size;
    
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    header.timestamp = (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
    
    // 计算校验和
    header.checksum = crc32(0, (const unsigned char*)data, data_size);
    
    // 记录当前偏移量
    off_t entry_offset = journal->journal_offset;
    
    // 写入 Header
    ssize_t written = write(journal->journal_fd, &header, sizeof(header));
    if (written != sizeof(header)) {
        pthread_mutex_unlock(&journal->lock);
        return -1;
    }
    
    // 写入 Payload
    written = write(journal->journal_fd, data, data_size);
    if (written != (ssize_t)data_size) {
        pthread_mutex_unlock(&journal->lock);
        return -1;
    }
    
    journal->journal_offset += sizeof(header) + data_size;
    journal->total_writes++;
    
    pthread_mutex_unlock(&journal->lock);
    
    return 0;
}

// 异步写入 Journal 条目（使用 io_uring）
static int parix_journal_append_async(parix_journal_t *journal, uint64_t stripe_id, 
                                      parix_entry_type_t type, const char *data, size_t data_size,
                                      struct io_uring *ring, off_t *entry_offset_out) {
    if (1) { // Forced sync to prevent mem leak
        // 回退到同步写入
        return parix_journal_append_sync(journal, stripe_id, type, data, data_size);
    }
    
    pthread_mutex_lock(&journal->lock);
    
    // 构造 Header（需要分配内存，因为异步写入）
    parix_journal_header_t *header = (parix_journal_header_t*)malloc(sizeof(parix_journal_header_t));
    if (!header) {
        pthread_mutex_unlock(&journal->lock);
        return -1;
    }
    
    header->magic = 0x50415258; // "PARX"
    header->stripe_id = stripe_id;
    header->version = 0;
    header->type = type;
    header->payload_size = (uint32_t)data_size;
    
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    header->timestamp = (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
    
    // 计算校验和
    header->checksum = crc32(0, (const unsigned char*)data, data_size);
    
    // 记录当前偏移量
    off_t entry_offset = journal->journal_offset;
    *entry_offset_out = entry_offset;
    
    // 分配数据副本（因为异步写入，原始数据可能被释放）
    char *data_copy = (char*)malloc(data_size);
    if (!data_copy) {
        free(header);
        pthread_mutex_unlock(&journal->lock);
        return -1;
    }
    memcpy(data_copy, data, data_size);
    
    // 提交 Header 写入
    struct io_uring_sqe *sqe_header = io_uring_get_sqe(ring);
    if (!sqe_header) {
        free(header);
        free(data_copy);
        pthread_mutex_unlock(&journal->lock);
        return -1;
    }
    io_uring_prep_write(sqe_header, journal->journal_fd, header, sizeof(parix_journal_header_t), entry_offset);
    io_uring_sqe_set_data(sqe_header, (void*)(uintptr_t)(journal->parity_id * 1000 + 1)); // 标记为header
    
    // 提交 Payload 写入
    struct io_uring_sqe *sqe_data = io_uring_get_sqe(ring);
    if (!sqe_data) {
        free(header);
        free(data_copy);
        pthread_mutex_unlock(&journal->lock);
        return -1;
    }
    io_uring_prep_write(sqe_data, journal->journal_fd, data_copy, data_size, entry_offset + sizeof(parix_journal_header_t));
    io_uring_sqe_set_data(sqe_data, (void*)(uintptr_t)(journal->parity_id * 1000 + 2)); // 标记为data
    
    // 更新偏移量（注意：实际写入是异步的，这里只是预分配）
    journal->journal_offset += sizeof(parix_journal_header_t) + data_size;
    journal->total_writes++;
    
    pthread_mutex_unlock(&journal->lock);
    
    return 0;
}

// 兼容性别名
static int parix_journal_append(parix_journal_t *journal, uint64_t stripe_id, 
                                parix_entry_type_t type, const char *data, size_t data_size) {
    return parix_journal_append_sync(journal, stripe_id, type, data, data_size);
}

// PARIX 推测性写入 - Data Server 端逻辑（异步版本）
static int parix_speculative_write(parix_journal_t *journal, uint64_t stripe_id, 
                                   const char *new_data, size_t data_size,
                                   const char *old_data, int *need_d0,
                                   struct io_uring *ring) {
    pthread_mutex_lock(&journal->lock);
    
    parix_stripe_index_t *stripe = parix_find_stripe(journal, stripe_id);
    
    if (!stripe || !stripe->has_d0) {
        // 情况：没有 D0，推测失败
        if (!old_data) {
            *need_d0 = 1;
            journal->speculative_misses++;
            pthread_mutex_unlock(&journal->lock);
            return 1; // 需要 D0
        }
        
        // 收到补救请求，写入 D0 和新数据
        pthread_mutex_unlock(&journal->lock);
        
        off_t d0_offset = 0;
        // 优先使用异步写入（如果ring可用），避免阻塞关键路径
        if (ring) {
            if (parix_journal_append_async(journal, stripe_id, PARIX_ENTRY_BASE, old_data, data_size, ring, &d0_offset) != 0) {
                // 异步写入失败，回退到同步写入（不应该发生，但为了健壮性保留）
                if (parix_journal_append_sync(journal, stripe_id, PARIX_ENTRY_BASE, old_data, data_size) != 0) {
                    return -1;
                }
                pthread_mutex_lock(&journal->lock);
                d0_offset = journal->journal_offset - data_size - sizeof(parix_journal_header_t);
                pthread_mutex_unlock(&journal->lock);
            }
        } else {
            // 没有ring，使用同步写入
            if (parix_journal_append_sync(journal, stripe_id, PARIX_ENTRY_BASE, old_data, data_size) != 0) {
                return -1;
            }
            pthread_mutex_lock(&journal->lock);
            d0_offset = journal->journal_offset - data_size - sizeof(parix_journal_header_t);
            pthread_mutex_unlock(&journal->lock);
        }
        
        pthread_mutex_lock(&journal->lock);
        stripe = parix_get_or_create_stripe(journal, stripe_id);
        if (!stripe) {
            pthread_mutex_unlock(&journal->lock);
            return -1;
        }
        stripe->has_d0 = true;
        stripe->d0_offset = d0_offset;
        pthread_mutex_unlock(&journal->lock);
    } else {
        // 情况：已有 D0，推测成功
        journal->speculative_hits++;
        pthread_mutex_unlock(&journal->lock);
    }
    
    // 写入更新数据（优先使用异步写入，避免阻塞关键路径）
    off_t update_offset = 0;
    if (ring) {
        if (parix_journal_append_async(journal, stripe_id, PARIX_ENTRY_UPDATE, new_data, data_size, ring, &update_offset) != 0) {
            // 异步写入失败，回退到同步写入（不应该发生，但为了健壮性保留）
            if (parix_journal_append_sync(journal, stripe_id, PARIX_ENTRY_UPDATE, new_data, data_size) != 0) {
                return -1;
            }
            pthread_mutex_lock(&journal->lock);
            update_offset = journal->journal_offset - data_size - sizeof(parix_journal_header_t);
            pthread_mutex_unlock(&journal->lock);
        }
    } else {
        // 没有ring，使用同步写入
        if (parix_journal_append_sync(journal, stripe_id, PARIX_ENTRY_UPDATE, new_data, data_size) != 0) {
            return -1;
        }
        pthread_mutex_lock(&journal->lock);
        update_offset = journal->journal_offset - data_size - sizeof(parix_journal_header_t);
        pthread_mutex_unlock(&journal->lock);
    }
    
    // 更新索引
    pthread_mutex_lock(&journal->lock);
    stripe = parix_find_stripe(journal, stripe_id);
    if (stripe) {
        stripe->latest_offset = update_offset;
        stripe->update_count++;
        stripe->version++;
    }
    pthread_mutex_unlock(&journal->lock);
    
    *need_d0 = 0;
    return 0;
}

// PARIX Journal 重放
static int parix_journal_replay(parix_journal_t *journal, int parity_disk_fd, size_t stripe_size) {
    pthread_mutex_lock(&journal->lock);
    
    fprintf(stderr, "[PARIX] Replaying journal for parity %d (%d stripes)...\n", 
            journal->parity_id, journal->index_count);
    
    char *d0_buf = NULL;
    char *latest_buf = NULL;
    char *delta_buf = NULL;
    char *parity_buf = NULL;
    
    if (posix_memalign((void**)&d0_buf, 4096, stripe_size) != 0 ||
        posix_memalign((void**)&latest_buf, 4096, stripe_size) != 0 ||
        posix_memalign((void**)&delta_buf, 4096, stripe_size) != 0 ||
        posix_memalign((void**)&parity_buf, 4096, stripe_size) != 0) {
        pthread_mutex_unlock(&journal->lock);
        free(d0_buf);
        free(latest_buf);
        free(delta_buf);
        free(parity_buf);
        return -1;
    }
    
    int replayed = 0;
    
    for (int i = 0; i < journal->index_count; i++) {
        parix_stripe_index_t *stripe = &journal->index[i];
        
        if (!stripe->has_d0 || stripe->update_count == 0) {
            continue; // 跳过无效条目
        }
        
        // 读取 D0 header以获取实际数据大小
        parix_journal_header_t d0_header, latest_header;
        
        lseek(journal->journal_fd, stripe->d0_offset, SEEK_SET);
        ssize_t nr = read(journal->journal_fd, &d0_header, sizeof(d0_header));
        if (nr != sizeof(d0_header)) {
            fprintf(stderr, "[PARIX] Failed to read D0 header for stripe %lu (got %ld bytes)\n", 
                    stripe->stripe_id, (long)nr);
            continue;
        }
        
        // 验证header
        if (d0_header.magic != 0x50415258 || d0_header.stripe_id != stripe->stripe_id) {
            fprintf(stderr, "[PARIX] Invalid D0 header for stripe %lu (magic=%08x, id=%lu)\n",
                    stripe->stripe_id, d0_header.magic, d0_header.stripe_id);
            continue;
        }
        
        size_t d0_size = d0_header.payload_size;
        if (d0_size > stripe_size) {
            fprintf(stderr, "[PARIX] D0 size too large: %zu > %zu\n", d0_size, stripe_size);
            continue;
        }
        
        // 读取 D0 payload
        memset(d0_buf, 0, stripe_size);
        nr = read(journal->journal_fd, d0_buf, d0_size);
        if (nr != (ssize_t)d0_size) {
            fprintf(stderr, "[PARIX] Failed to read D0 data for stripe %lu (expected %zu, got %ld)\n", 
                    stripe->stripe_id, d0_size, (long)nr);
            continue;
        }
        
        // 读取最新数据 header
        lseek(journal->journal_fd, stripe->latest_offset, SEEK_SET);
        nr = read(journal->journal_fd, &latest_header, sizeof(latest_header));
        if (nr != sizeof(latest_header)) {
            fprintf(stderr, "[PARIX] Failed to read latest header for stripe %lu\n", stripe->stripe_id);
            continue;
        }
        
        if (latest_header.magic != 0x50415258 || latest_header.stripe_id != stripe->stripe_id) {
            fprintf(stderr, "[PARIX] Invalid latest header for stripe %lu\n", stripe->stripe_id);
            continue;
        }
        
        size_t latest_size = latest_header.payload_size;
        if (latest_size > stripe_size) {
            fprintf(stderr, "[PARIX] Latest size too large: %zu > %zu\n", latest_size, stripe_size);
            continue;
        }
        
        // 读取最新数据 payload
        memset(latest_buf, 0, stripe_size);
        nr = read(journal->journal_fd, latest_buf, latest_size);
        if (nr != (ssize_t)latest_size) {
            fprintf(stderr, "[PARIX] Failed to read latest data for stripe %lu (expected %zu, got %ld)\n",
                    stripe->stripe_id, latest_size, (long)nr);
            continue;
        }
        
        // 计算 Delta: Δ = D0 ⊕ D(r)
        // 注意：如果D0和latest大小不同，以较小的为准
        size_t delta_size = (d0_size < latest_size) ? d0_size : latest_size;
        memset(delta_buf, 0, stripe_size);
        memcpy(delta_buf, d0_buf, d0_size);
        xor_update_simd(delta_buf, latest_buf, delta_size);
        
        // 读取当前 Parity（仅在有真实磁盘时）
        if (parity_disk_fd >= 0) {
            off_t parity_offset = (off_t)stripe->stripe_id * (off_t)stripe_size;
            memset(parity_buf, 0, stripe_size);
            ssize_t npr = pread(parity_disk_fd, parity_buf, stripe_size, parity_offset);
            if (npr < 0) {
                fprintf(stderr, "[PARIX] Failed to read parity for stripe %lu: %s\n",
                        stripe->stripe_id, strerror(errno));
                continue;
            }
            
            // 更新 Parity: P' = P ⊕ Δ
            xor_update_simd(parity_buf, delta_buf, stripe_size);
            
            // 写回 Parity
            ssize_t npw = pwrite(parity_disk_fd, parity_buf, stripe_size, parity_offset);
            if (npw != (ssize_t)stripe_size) {
                fprintf(stderr, "[PARIX] Failed to write parity for stripe %lu: %s\n",
                        stripe->stripe_id, strerror(errno));
                continue;
            }
        }
        
        replayed++;
    }
    
    // 清空索引和 Journal
    journal->index_count = 0;
    journal->journal_offset = 0;
    ftruncate(journal->journal_fd, 0);
    lseek(journal->journal_fd, 0, SEEK_SET);
    
    pthread_mutex_unlock(&journal->lock);
    
    free(d0_buf);
    free(latest_buf);
    free(delta_buf);
    free(parity_buf);
    
    fprintf(stderr, "[PARIX] Replayed %d stripes for parity %d\n", replayed, journal->parity_id);
    
    return replayed;
}

// 销毁 Journal
static void parix_journal_destroy(parix_journal_t *journal) {
    if (!journal) return;
    
    if (journal->journal_fd >= 0) {
        close(journal->journal_fd);
    }
    
    free(journal->index);
    free(journal->journal_path);
    pthread_mutex_destroy(&journal->lock);
    free(journal);
}

// 创建 PARIX 全局上下文
parix_context_t* parix_context_create(const char *base_dir, int k, int w, size_t packetsize, int replay_threshold) {
    parix_context_t *ctx = (parix_context_t*)calloc(1, sizeof(parix_context_t));
    if (!ctx) return NULL;
    
    ctx->base_dir = strdup(base_dir ? base_dir : "./parix_journals");
    ctx->k = k;
    ctx->w = w;
    ctx->packetsize = packetsize;
    ctx->replay_threshold = replay_threshold;
    ctx->journal_count = 2; // RDP: Row + Diag
    ctx->auto_replay = 1;
    
    // 创建目录
    mkdir(ctx->base_dir, 0755);
    
    // 创建 Journals（分配结构体数组）
    ctx->journals = (parix_journal_t*)calloc((size_t)ctx->journal_count, sizeof(parix_journal_t));
    if (!ctx->journals) {
        free(ctx->base_dir);
        free(ctx);
        return NULL;
    }
    
    // 直接初始化 journals 数组中的每个元素
    int initial_capacity = 1024;
    for (int i = 0; i < ctx->journal_count; i++) {
        parix_journal_t *j = &ctx->journals[i];
        j->parity_id = i;
        j->index_capacity = initial_capacity;
        j->index_count = 0;
        
        // 分配索引数组
        j->index = (parix_stripe_index_t*)calloc((size_t)initial_capacity, sizeof(parix_stripe_index_t));
        if (!j->index) {
            // 清理已分配的资源
            for (int k = 0; k < i; k++) {
                if (ctx->journals[k].journal_fd >= 0) close(ctx->journals[k].journal_fd);
                free(ctx->journals[k].index);
                free(ctx->journals[k].journal_path);
                pthread_mutex_destroy(&ctx->journals[k].lock);
            }
            free(ctx->journals);
            free(ctx->base_dir);
            free(ctx);
            return NULL;
        }
        
        // 创建 Journal 文件
        char path[512];
        snprintf(path, sizeof(path), "%s/parix_journal_p%d.log", ctx->base_dir, i);
        j->journal_path = strdup(path);
        
        j->journal_fd = open(path, O_RDWR | O_CREAT | O_APPEND, 0644);
        if (j->journal_fd < 0) {
            fprintf(stderr, "[PARIX] Failed to open journal %s: %s\n", path, strerror(errno));
            // 清理
            free(j->index);
            free(j->journal_path);
            for (int k = 0; k < i; k++) {
                if (ctx->journals[k].journal_fd >= 0) close(ctx->journals[k].journal_fd);
                free(ctx->journals[k].index);
                free(ctx->journals[k].journal_path);
                pthread_mutex_destroy(&ctx->journals[k].lock);
            }
            free(ctx->journals);
            free(ctx->base_dir);
            free(ctx);
            return NULL;
        }
        
        // 获取当前文件大小
        struct stat st;
        if (fstat(j->journal_fd, &st) == 0) {
            j->journal_offset = st.st_size;
        }
        
        pthread_mutex_init(&j->lock, NULL);
        
        fprintf(stderr, "[PARIX] Created journal for parity %d at %s (offset=%ld)\n", 
                i, path, (long)j->journal_offset);
    }
    
    pthread_mutex_init(&ctx->global_lock, NULL);
    
    fprintf(stderr, "[PARIX] Context created: dir=%s, k=%d, w=%d, packetsize=%zu\n",
            ctx->base_dir, k, w, packetsize);
    
    return ctx;
}

// 销毁 PARIX 上下文
void parix_context_destroy(parix_context_t *ctx) {
    if (!ctx) return;
    
    if (ctx->journals) {
        for (int i = 0; i < ctx->journal_count; i++) {
            // 清理journal内部资源
            if (ctx->journals[i].journal_fd >= 0) {
                close(ctx->journals[i].journal_fd);
            }
            if (ctx->journals[i].journal_path) {
                free(ctx->journals[i].journal_path);
            }
            if (ctx->journals[i].index) {
                free(ctx->journals[i].index);
            }
            pthread_mutex_destroy(&ctx->journals[i].lock);
        }
        free(ctx->journals);
    }
    
    if (ctx->base_dir) {
        free(ctx->base_dir);
    }
    pthread_mutex_destroy(&ctx->global_lock);
    free(ctx);
}

// ========== RDP 编码实现 ==========

void rdp_encode(char **data, char **coding, int k, int w, int packetsize) {
    if (k <= 0 || w <= 0 || packetsize <= 0 || !data || !coding) {
        fprintf(stderr, "rdp_encode: invalid params k=%d w=%d packetsize=%d\n", k, w, packetsize);
        return;
    }

    int p = (config.p_prime > 0) ? config.p_prime : (w + 1);
    if (w != p - 1) {
        fprintf(stderr, "Error: RDP geometry mismatch. w=%d, p=%d\n", w, p);
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

    // 行校验 + 对角校验
    for (int c = 0; c < k; c++) {
        for (int r = 0; r < w; r++) {
            char *src = data[c] + (size_t)r * packet_bytes;
            
            // 行校验
            char *p_dst = coding[0] + (size_t)r * packet_bytes;
            xor_update_simd(p_dst, src, packetsize);
            
            // 对角校验（跳过缺失对角线 d = p-1）
            int d = r + c;
            if (d >= p) d -= p;
            if (d < w) {
                char *q_dst = coding[1] + (size_t)d * packet_bytes;
                xor_update_simd(q_dst, src, packetsize);
            }
        }
    }

    // 行校验盘参与对角校验
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

// ========== PARIX 更新函数 ==========

void update_rdp_parix(char **data, char **coding, char *new_data,
                     int *fds, int total_disks, struct io_uring *ring,
                     long stripe_num) {
    int blocks_to_update = config.update_size / config.packetsize;
    if (blocks_to_update <= 0) return;
    
    size_t packet_bytes = (size_t)config.packetsize;
    int p = (config.p_prime > 0) ? config.p_prime : (config.w + 1);
    
    // 如果PARIX未启用，使用简单的内存更新模式
    if (!g_parix_ctx || !config.parix_enabled) {
        // 简单模式：直接更新内存中的数据和校验
        for (int i = 0; i < blocks_to_update; i++) {
            int col = i % config.k;
            int row = (i / config.k) % config.w;
            
            char *new_block = new_data + (size_t)i * packet_bytes;
            char *old_block = (data && data[col]) ? data[col] + (size_t)row * packet_bytes : NULL;
            
            // 计算 Delta
            char *delta = (char*)alloca(packet_bytes);
            if (old_block) {
                memcpy(delta, old_block, packet_bytes);
                xor_update_simd(delta, new_block, packet_bytes);
            } else {
                memcpy(delta, new_block, packet_bytes);
            }
            
            // 更新 Row Parity
            if (coding && coding[0]) {
                xor_update_simd(coding[0] + (size_t)row * packet_bytes, delta, packet_bytes);
            }
            
            // 更新 Diag Parity
            int d = row + col;
            if (d >= p) d -= p;
            if (d < config.w && coding && coding[1]) {
                xor_update_simd(coding[1] + (size_t)d * packet_bytes, delta, packet_bytes);
            }
            
            // 更新内存数据
            if (data && data[col]) {
                memcpy(data[col] + (size_t)row * packet_bytes, new_block, packet_bytes);
            }
        }
        return;
    }
    
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    
    // 分配缓冲区
    char *old_data_buf = NULL;
    char *row_delta = NULL;
    char *diag_delta = NULL;
    int need_old_data = 0;
    
    if (posix_memalign((void**)&old_data_buf, 4096, (size_t)config.update_size) != 0 ||
        posix_memalign((void**)&row_delta, 4096, packet_bytes * (size_t)config.w) != 0 ||
        posix_memalign((void**)&diag_delta, 4096, packet_bytes * (size_t)config.w) != 0) {
        fprintf(stderr, "[PARIX] Failed to allocate buffers\n");
        free(old_data_buf);
        free(row_delta);
        free(diag_delta);
        return;
    }
    
    memset(row_delta, 0, packet_bytes * (size_t)config.w);
    memset(diag_delta, 0, packet_bytes * (size_t)config.w);
    
    // 第一次推测性尝试（检查Row Parity）
    for (int i = 0; i < blocks_to_update; i++) {
        int col = i % config.k;
        int row = (i / config.k) % config.w;
        
        int need_d0 = 0;
        char *new_block = new_data + (size_t)i * packet_bytes;
        
        // 尝试推测性写入 Row Parity Journal（使用异步写入）
        parix_speculative_write(&g_parix_ctx->journals[0], 
                               (uint64_t)stripe_num, 
                               new_block, packet_bytes, 
                               NULL, &need_d0, ring);
        
        if (need_d0) {
            need_old_data = 1;
            break; // 需要读取旧数据
        }
    }
    
    // 如果需要旧数据，读取并补救
    if (need_old_data && fds && total_disks > 0 && ring) {
        // TODO: 实现异步读取旧数据
        fprintf(stderr, "[PARIX] Need to read old data for stripe %ld\n", stripe_num);
        
        // 读取旧数据块
        for (int i = 0; i < blocks_to_update; i++) {
            int col = i % config.k;
            int row = (i / config.k) % config.w;
            
            off_t offset = block_offset_bytes(stripe_num, row, config.packetsize);
            int total_disks = config.k + config.m;
            int fd_idx = map_physical_disk(col, stripe_num, total_disks, config.alloc);
            if (fd_idx < 0) fd_idx = 0;
            if (fd_idx >= total_disks) fd_idx %= total_disks;
            
            pread(fds[fd_idx], old_data_buf + (size_t)i * packet_bytes, packet_bytes, offset);
        }
        
        // 使用旧数据重新写入 Journal（Row和Diag）
        for (int i = 0; i < blocks_to_update; i++) {
            int need_d0_row = 0, need_d0_diag = 0;
            char *new_block = new_data + (size_t)i * packet_bytes;
            char *old_block = old_data_buf + (size_t)i * packet_bytes;
            
            // 写入 Row Parity Journal（使用异步写入）
            parix_speculative_write(&g_parix_ctx->journals[0], 
                                   (uint64_t)stripe_num, 
                                   new_block, packet_bytes, 
                                   old_block, &need_d0_row, ring);
            
            // 写入 Diag Parity Journal（使用异步写入）
            parix_speculative_write(&g_parix_ctx->journals[1], 
                                   (uint64_t)stripe_num, 
                                   new_block, packet_bytes, 
                                   old_block, &need_d0_diag, ring);
        }
    }
    
    // 写入新数据到数据盘
    if (fds && total_disks > 0 && ring) {
        for (int i = 0; i < blocks_to_update; i++) {
            int col = i % config.k;
            int row = (i / config.k) % config.w;
            
            off_t offset = block_offset_bytes(stripe_num, row, config.packetsize);
            int total_disks = config.k + config.m;
            int fd_idx = map_physical_disk(col, stripe_num, total_disks, config.alloc);
            if (fd_idx < 0) fd_idx = 0;
            if (fd_idx >= total_disks) fd_idx %= total_disks;
            
            // 使用 io_uring 异步写入数据（与日志写入一起提交）
            struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
            if (sqe) {
                io_uring_prep_write(sqe, fds[fd_idx], new_data + (size_t)i * packet_bytes, packet_bytes, offset);
                io_uring_sqe_set_data(sqe, (void*)(uintptr_t)(col + 100)); // 标记为数据写入
            } else {
                // 如果队列满，快速尝试释放几个CQE
                struct io_uring_cqe *cqe;
                for (int retry = 0; retry < 10 && !sqe; retry++) {
                    if (io_uring_peek_cqe(ring, &cqe) == 0) {
                        io_uring_cqe_seen(ring, cqe);
                        sqe = io_uring_get_sqe(ring);
                    } else {
                        break;
                    }
                }
                if (sqe) {
                    io_uring_prep_write(sqe, fds[fd_idx], new_data + (size_t)i * packet_bytes, packet_bytes, offset);
                    io_uring_sqe_set_data(sqe, (void*)(uintptr_t)(col + 100));
                } else {
                    // 最后回退到同步写入（应该很少发生）
                    pwrite(fds[fd_idx], new_data + (size_t)i * packet_bytes, packet_bytes, offset);
                }
            }
        }
        // 批量提交所有异步写入
        if (ring && g_ring_ready) {
            io_uring_submit(ring);
        }
    }
    
    // 更新内存中的数据和校验（用于验证）
    for (int i = 0; i < blocks_to_update; i++) {
        int col = i % config.k;
        int row = (i / config.k) % config.w;
        
        char *new_block = new_data + (size_t)i * packet_bytes;
        char *old_block = (data && data[col]) ? data[col] + (size_t)row * packet_bytes : NULL;
        
        // 计算 Delta
        char delta[packet_bytes];
        if (old_block) {
            memcpy(delta, old_block, packet_bytes);
            xor_update_simd(delta, new_block, packet_bytes);
        } else {
            memcpy(delta, new_block, packet_bytes);
        }
        
        // 更新 Row Parity
        if (coding && coding[0]) {
            xor_update_simd(coding[0] + (size_t)row * packet_bytes, delta, packet_bytes);
        }
        
        // 更新 Diag Parity
        int d = row + col;
        if (d >= p) d -= p;
        if (d < config.w && coding && coding[1]) {
            xor_update_simd(coding[1] + (size_t)d * packet_bytes, delta, packet_bytes);
        }
        
        // 更新内存数据
        if (data && data[col]) {
            memcpy(data[col] + (size_t)row * packet_bytes, new_block, packet_bytes);
        }
    }
    
    // 定期触发重放
    static int update_counter = 0;
    update_counter++;
    
    if (g_parix_ctx->auto_replay && 
        update_counter >= g_parix_ctx->replay_threshold) {
        fprintf(stderr, "[PARIX] Triggering replay (counter=%d)\n", update_counter);
        
        if (fds && total_disks >= config.k + config.m) {
            size_t stripe_size = packet_bytes * (size_t)config.w;
            parix_journal_replay(&g_parix_ctx->journals[0], fds[config.k], stripe_size);
            parix_journal_replay(&g_parix_ctx->journals[1], fds[config.k + 1], stripe_size);
        }
        
        update_counter = 0;
    }
    
    free(old_data_buf);
    free(row_delta);
    free(diag_delta);
    
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double elapsed = timespec_diff_sec(&t0, &t1);
    
    pthread_mutex_lock(&stats.lock);
    stats.compute_time += elapsed;
    stats.update_count++;
    pthread_mutex_unlock(&stats.lock);
}

// ========== 磁盘管理 ==========

static int load_safe_disk_paths(char **disk_paths, int max_disks) {
    FILE *safe_disk_file = fopen("safe_disks.txt", "r");
    int disk_count = 0;
    
    if (!safe_disk_file) {
        printf("警告: 未找到safe_disks.txt，使用默认磁盘路径\n");
        const char *default_paths[] = {
            "/dev/nvme1n1", "/dev/nvme2n1", "/dev/nvme3n1", "/dev/nvme4n1", 
            "/dev/nvme5n1", "/dev/nvme6n1"
        };
        
        int default_count = sizeof(default_paths) / sizeof(default_paths[0]);
        for (int i = 0; i < default_count && i < max_disks; i++) {
            if (access(default_paths[i], F_OK) == 0) {
                disk_paths[i] = strdup(default_paths[i]);
                disk_count++;
            }
        }
        return disk_count;
    }
    
    char line[256];
    while (fgets(line, sizeof(line), safe_disk_file) && disk_count < max_disks) {
        line[strcspn(line, "\n")] = 0;
        if (line[0] == '\0' || line[0] == '#') continue;
        
        if (access(line, F_OK) == 0) {
            disk_paths[disk_count] = strdup(line);
            disk_count++;
        }
    }
    
    fclose(safe_disk_file);
    return disk_count;
}

static int ensure_nonstdio_fd(int fd, const char *who) {
    if (fd >= 3) return fd;
    
    int dupfd = fcntl(fd, F_DUPFD, 3);
    if (dupfd < 0) {
        fprintf(stderr, "[FATAL] %s: dupfd failed for fd %d: %s\n",
                who ? who : "unknown", fd, strerror(errno));
        abort();
    }
    close(fd);
    return dupfd;
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
        fprintf(stderr, "需要 %d 块设备，但只找到 %d 个\n", want, found);
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
    return 0;
}

static void close_all_disks(void) {
    if (g_fds) {
        for (int i = 0; i < g_total_disks; i++) {
            if (g_fds[i] >= 0) close(g_fds[i]);
        }
        free(g_fds);
        g_fds = NULL;
    }
    g_total_disks = 0;
    g_pending_ops = 0;
}

// ========== 主函数 ==========

static void print_usage(const char *prog) {
    fprintf(stderr, "Usage: %s [options]\n", prog);
    fprintf(stderr, "Options:\n");
    fprintf(stderr, "  -k <num>      Number of data disks (default %d)\n", config.k);
    fprintf(stderr, "  -w <num>      Rows w (default %d)\n", config.w);
    fprintf(stderr, "  -p <bytes>    Packet size (default %d)\n", config.packetsize);
    fprintf(stderr, "  -u <bytes>    Update size (default %d)\n", config.update_size);
    fprintf(stderr, "  -n <count>    Number of updates (default %d)\n", config.n_updates);
    fprintf(stderr, "  -a <policy>   Address allocation policy (sequential|optimized|row|diag|auto)\n");
    fprintf(stderr, "  -X            Enable PARIX\n");
    fprintf(stderr, "  -L <dir>      PARIX journal directory (default ./parix_journals)\n");
    fprintf(stderr, "  -T <count>    PARIX replay threshold (default 100)\n");
    fprintf(stderr, "  -V            Enable verification\n");
    fprintf(stderr, "  -h            Show this help\n");
}

int main(int argc, char *argv[]) {
    int opt;
    int stats_lock_initialized = 0;
    int exit_code = 0;
    
    while ((opt = getopt(argc, argv, "k:w:p:u:n:a:XL:T:Vh")) != -1) {
        switch (opt) {
            case 'k': config.k = atoi(optarg); break;
            case 'w': config.w = atoi(optarg); break;
            case 'p': config.packetsize = atoi(optarg); break;
            case 'u': config.update_size = atoi(optarg); break;
            case 'n': config.n_updates = atoi(optarg); break;
            case 'a': config.alloc = optarg; break;
            case 'X': config.parix_enabled = 1; break;
            case 'L': config.parix_dir = optarg; break;
            case 'T': config.parix_replay_threshold = atoi(optarg); break;
            case 'V': config.verify = 1; break;
            case 'h':
            default:
                print_usage(argv[0]);
                return opt == 'h' ? 0 : 1;
        }
    }
    
    // 自动调整参数
    config.m = 2; // RDP 固定 2 个校验盘
    int p_candidate = next_prime(config.k);
    if (p_candidate <= config.k) {
        p_candidate = next_prime(config.k + 1);
    }
    config.w = p_candidate - 1;
    config.p_prime = p_candidate;
    
    printf("=== RDP with PARIX ===\n");
    printf("Configuration:\n");
    printf("  k=%d, m=%d, w=%d, p=%d\n", config.k, config.m, config.w, config.p_prime);
    printf("  packetsize=%d, update_size=%d, n_updates=%d\n", 
           config.packetsize, config.update_size, config.n_updates);
    printf("  PARIX: %s\n", config.parix_enabled ? "enabled" : "disabled");
    
    if (pthread_mutex_init(&stats.lock, NULL) != 0) {
        perror("pthread_mutex_init");
        return 1;
    }
    stats_lock_initialized = 1;
    
    // 创建内存池
    size_t stripe_bytes = (size_t)config.w * (size_t)config.packetsize;
    int pool_capacity = (config.k + config.m) * 4;
    global_pool = create_memory_pool(pool_capacity, stripe_bytes);
    if (!global_pool) {
        fprintf(stderr, "Failed to create memory pool\n");
        exit_code = 1;
        goto cleanup;
    }
    
    // 分配数据和校验缓冲区
    char **data = calloc((size_t)config.k, sizeof(char*));
    char **coding = calloc((size_t)config.m, sizeof(char*));
    if (!data || !coding) {
        fprintf(stderr, "Out of memory\n");
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
        // 初始化测试数据
        for (size_t j = 0; j < stripe_bytes; j++) {
            data[i][j] = (char)((i + 1) * 17 + j);
        }
    }
    
    for (int i = 0; i < config.m; i++) {
        coding[i] = pool_alloc(global_pool);
        if (!coding[i]) {
            fprintf(stderr, "Failed to allocate coding buffer %d\n", i);
            exit_code = 1;
            goto cleanup;
        }
    }
    
    // 初始编码
    rdp_encode(data, coding, config.k, config.w, config.packetsize);
    
    printf("\nInitial Encoding:\n");
    for (int i = 0; i < config.k; i++) {
        uint32_t crc = crc32(0, (const unsigned char*)data[i], stripe_bytes);
        printf("  D%d: CRC32=%08x\n", i, crc);
    }
    printf("  Row Parity:  CRC32=%08x\n", crc32(0, (const unsigned char*)coding[0], stripe_bytes));
    printf("  Diag Parity: CRC32=%08x\n", crc32(0, (const unsigned char*)coding[1], stripe_bytes));
    
    // 初始化 PARIX
    if (config.parix_enabled) {
        g_parix_ctx = parix_context_create(
            config.parix_dir ? config.parix_dir : "./parix_journals",
            config.k, config.w, (size_t)config.packetsize, 
            config.parix_replay_threshold);
        
        if (!g_parix_ctx) {
            fprintf(stderr, "Failed to create PARIX context\n");
            config.parix_enabled = 0;
        }
    }
    
    // 打开磁盘
    int io_ready = 0;
    if (config.n_updates > 0) {
        if (io_uring_queue_init(256, &g_ring, 0) == 0) {
            g_ring_ready = 1;
            g_ring_queue_depth = 256;
            
            if (open_and_register_disks(config.k, config.m) == 0) {
                io_ready = 1;
            }
        }
    }
    
    // 性能测试
    if (config.n_updates > 0) {
        char *update_payload = (char*)malloc((size_t)config.update_size);
        if (!update_payload) {
            fprintf(stderr, "Failed to allocate update payload\n");
            exit_code = 1;
            goto cleanup;
        }
        
        for (int i = 0; i < config.update_size; i++) {
            update_payload[i] = (char)(i & 0xFF);
        }
        
        struct timespec t0, t1;
        clock_gettime(CLOCK_MONOTONIC, &t0);
        
        if (!io_ready) {
            fprintf(stderr, "WARNING: No disks available, running in memory-only mode\n");
        }
        
        printf("\nRunning %d updates...\n", config.n_updates);
        
        for (int i = 0; i < config.n_updates; i++) {
            if (io_ready) {
                update_rdp_parix(data, coding, update_payload, 
                               g_fds, g_total_disks, &g_ring, i);
            } else {
                // 纯内存模式：只更新内存中的数据和校验
                update_rdp_parix(data, coding, update_payload, 
                               NULL, 0, NULL, i);
            }
            
            if ((i + 1) % 100 == 0) {
                fprintf(stderr, "  Progress: %d/%d\n", i + 1, config.n_updates);
            }
        }
        
        // 最终重放（仅当有磁盘时）
        if (io_ready && g_parix_ctx && g_fds && g_total_disks >= config.k + config.m) {
            fprintf(stderr, "\nFinal replay...\n");
            size_t stripe_size = (size_t)config.packetsize * (size_t)config.w;
            parix_journal_replay(&g_parix_ctx->journals[0], g_fds[config.k], stripe_size);
            parix_journal_replay(&g_parix_ctx->journals[1], g_fds[config.k + 1], stripe_size);
        }
        
        clock_gettime(CLOCK_MONOTONIC, &t1);
        double secs = timespec_diff_sec(&t0, &t1);
        
        long total_bytes = (long)config.n_updates * (long)config.update_size;
        double mbps = total_bytes / (1024.0 * 1024.0) / secs;
        
        printf("\n[RESULT] elapsed=%.6fs, throughput=%.2f MB/s\n", secs, mbps);
        
        if (g_parix_ctx) {
            printf("\nPARIX Statistics:\n");
            for (int j = 0; j < g_parix_ctx->journal_count; j++) {
                const char *name = (j == 0) ? "Row" : "Diag";
                printf("  Journal %d (%s):\n", j, name);
                printf("    Total writes: %lu\n", g_parix_ctx->journals[j].total_writes);
                printf("    Speculative hits: %lu\n", g_parix_ctx->journals[j].speculative_hits);
                printf("    Speculative misses: %lu\n", g_parix_ctx->journals[j].speculative_misses);
                if (g_parix_ctx->journals[j].speculative_hits + g_parix_ctx->journals[j].speculative_misses > 0) {
                    printf("    Hit rate: %.2f%%\n", 
                           100.0 * g_parix_ctx->journals[j].speculative_hits / 
                           (g_parix_ctx->journals[j].speculative_hits + g_parix_ctx->journals[j].speculative_misses));
                }
            }
        }
        
        free(update_payload);
    }
    
    // 验证
    if (config.verify) {
        printf("\nVerifying parity...\n");
        char *tmp_coding[2];
        tmp_coding[0] = pool_alloc(global_pool);
        tmp_coding[1] = pool_alloc(global_pool);
        
        if (tmp_coding[0] && tmp_coding[1]) {
            rdp_encode(data, tmp_coding, config.k, config.w, config.packetsize);
            
            int ok = (memcmp(tmp_coding[0], coding[0], stripe_bytes) == 0) &&
                     (memcmp(tmp_coding[1], coding[1], stripe_bytes) == 0);
            
            printf("[VERIFY] %s\n", ok ? "PASS" : "FAIL");
            if (!ok) exit_code = 2;
            
            pool_free(global_pool, tmp_coding[0]);
            pool_free(global_pool, tmp_coding[1]);
        }
    }
    
cleanup:
    if (g_parix_ctx) {
        parix_context_destroy(g_parix_ctx);
        g_parix_ctx = NULL;
    }
    
    close_all_disks();
    
    if (g_ring_ready) {
        io_uring_queue_exit(&g_ring);
        g_ring_ready = 0;
    }
    
    if (data) {
        for (int i = 0; i < config.k; i++) {
            if (data[i] && global_pool) pool_free(global_pool, data[i]);
        }
        free(data);
    }
    
    if (coding) {
        for (int i = 0; i < config.m; i++) {
            if (coding[i] && global_pool) pool_free(global_pool, coding[i]);
        }
        free(coding);
    }
    
    if (global_pool) {
        destroy_memory_pool(global_pool);
        global_pool = NULL;
    }
    
    if (stats_lock_initialized) {
        pthread_mutex_destroy(&stats.lock);
    }
    
    return exit_code;
}
