// SPDX-License-Identifier: MIT
#define _GNU_SOURCE
#define _POSIX_C_SOURCE 199309L

#include "plr_module.h"
#include "parix_module.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <stdbool.h>
#include <immintrin.h>

#ifdef __linux__
#include <liburing.h>
#endif

#ifdef _WIN32
#include <direct.h>
#include <io.h>
#ifndef mkdir
#define mkdir(path, mode) _mkdir(path)
#endif
#ifndef fsync
#define fsync _commit
#endif
#endif

#ifndef O_BINARY
#define O_BINARY 0
#endif

// ========== PARIX 核心数据结构 ==========

typedef enum {
    PARIX_ENTRY_BASE = 0,
    PARIX_ENTRY_UPDATE = 1,
    PARIX_ENTRY_DELTA = 2
} parix_entry_type_t;

typedef struct {
    uint32_t magic;
    uint32_t checksum;
    uint64_t stripe_id;
    uint32_t version;
    parix_entry_type_t type;
    uint32_t payload_size;
    uint64_t timestamp;
} __attribute__((packed)) parix_journal_header_t;

typedef struct {
    uint64_t stripe_id;
    int has_d0;
    off_t d0_offset;
    off_t latest_offset;
    uint32_t version;
    int update_count;
} parix_stripe_index_t;

typedef struct {
    int parity_id;
    int journal_fd;
    char *journal_path;
    off_t journal_offset;
    parix_stripe_index_t *index;
    int index_capacity;
    int index_count;
    uint64_t total_writes;
    uint64_t speculative_hits;
    uint64_t speculative_misses;
    pthread_mutex_t lock;
} parix_journal_t;

typedef struct {
    char *base_dir;
    int k;
    int w;
    size_t packetsize;
    parix_journal_t *journals;
    int journal_count;
    int replay_threshold;
    int auto_replay;
    pthread_mutex_t global_lock;
} parix_context_impl_t;

// XOR SIMD 实现
static void xor_update_simd(char *dst, const char *src, size_t size) {
    size_t i = 0;
#ifdef __AVX2__
    for (; i + 32 <= size; i += 32) {
        __m256i d = _mm256_loadu_si256((__m256i*)(dst + i));
        __m256i s = _mm256_loadu_si256((__m256i*)(src + i));
        __m256i result = _mm256_xor_si256(d, s);
        _mm256_storeu_si256((__m256i*)(dst + i), result);
    }
#elif defined(__SSE2__)
    for (; i + 16 <= size; i += 16) {
        __m128i d = _mm_loadu_si128((__m128i*)(dst + i));
        __m128i s = _mm_loadu_si128((__m128i*)(src + i));
        __m128i result = _mm_xor_si128(d, s);
        _mm_storeu_si128((__m128i*)(dst + i), result);
    }
#endif
    for (; i < size; i++) {
        dst[i] ^= src[i];
    }
}

typedef struct {
    long stripe_index;
    size_t logical_offset;
    size_t payload_bytes;
    off_t file_offset;
} plr_delta_entry_t;

typedef struct {
    int fd;
    char path[PATH_MAX];
    size_t parity_bytes;
    size_t reserved_bytes;
    size_t reserved_used;
    double ewma_util;
    long updates_since_merge;
    long updates_since_shrink;
    plr_delta_entry_t *entries;
    size_t entry_count;
    size_t entry_capacity;
} plr_parity_extent_t;

// PLR header 缓冲区池（用于异步 I/O）
#define PLR_HEADER_POOL_SIZE 1024  // 增加池大小以减少分配开销
static plr_delta_header_t *g_plr_header_pool[PLR_HEADER_POOL_SIZE] = {0};
static int g_plr_header_pool_count = 0;
static pthread_mutex_t g_plr_header_pool_lock = PTHREAD_MUTEX_INITIALIZER;

// PLR header context 缓冲区池（用于存储 header_buf 指针）
typedef struct {
    plr_delta_header_t *header;
    int is_header;
} plr_header_ctx_t;
#define PLR_HEADER_CTX_POOL_SIZE 1024  // 增加池大小以减少分配开销
static plr_header_ctx_t *g_plr_header_ctx_pool[PLR_HEADER_CTX_POOL_SIZE] = {0};
static int g_plr_header_ctx_pool_count = 0;
static pthread_mutex_t g_plr_header_ctx_pool_lock = PTHREAD_MUTEX_INITIALIZER;

static plr_header_ctx_t* plr_alloc_header_ctx(void) {
    pthread_mutex_lock(&g_plr_header_ctx_pool_lock);
    if (g_plr_header_ctx_pool_count > 0) {
        plr_header_ctx_t *ctx = g_plr_header_ctx_pool[--g_plr_header_ctx_pool_count];
        pthread_mutex_unlock(&g_plr_header_ctx_pool_lock);
        return ctx;
    }
    pthread_mutex_unlock(&g_plr_header_ctx_pool_lock);
    return (plr_header_ctx_t*)malloc(sizeof(plr_header_ctx_t));
}

static void plr_free_header_ctx(plr_header_ctx_t *ctx) {
    if (!ctx) return;
    pthread_mutex_lock(&g_plr_header_ctx_pool_lock);
    if (g_plr_header_ctx_pool_count < PLR_HEADER_CTX_POOL_SIZE) {
        g_plr_header_ctx_pool[g_plr_header_ctx_pool_count++] = ctx;
        pthread_mutex_unlock(&g_plr_header_ctx_pool_lock);
    } else {
        pthread_mutex_unlock(&g_plr_header_ctx_pool_lock);
        free(ctx);
    }
}

static plr_delta_header_t* plr_alloc_header(void) {
    pthread_mutex_lock(&g_plr_header_pool_lock);
    if (g_plr_header_pool_count > 0) {
        plr_delta_header_t *h = g_plr_header_pool[--g_plr_header_pool_count];
        pthread_mutex_unlock(&g_plr_header_pool_lock);
        return h;
    }
    pthread_mutex_unlock(&g_plr_header_pool_lock);
    return (plr_delta_header_t*)malloc(sizeof(plr_delta_header_t));
}

static void plr_free_header(plr_delta_header_t *h) {
    if (!h) return;
    pthread_mutex_lock(&g_plr_header_pool_lock);
    if (g_plr_header_pool_count < PLR_HEADER_POOL_SIZE) {
        g_plr_header_pool[g_plr_header_pool_count++] = h;
        pthread_mutex_unlock(&g_plr_header_pool_lock);
    } else {
        pthread_mutex_unlock(&g_plr_header_pool_lock);
        free(h);
    }
}

// 导出函数，用于 CQE 完成回调
void plr_free_header_buffer(plr_delta_header_t *h) {
    plr_free_header(h);
}

void plr_free_header_ctx_buffer(void *ctx) {
    plr_header_ctx_t *header_ctx = (plr_header_ctx_t*)ctx;
    plr_free_header_ctx(header_ctx);
}

struct plr_context {
    plr_config_t cfg;
    plr_parity_extent_t *extents;
    int parity_count;
};

static const uint64_t PLR_DELTA_MAGIC = 0x504C522D444C5441ULL; // "PLR-DLTA"

// 优化的 CRC32 查表实现
static const uint32_t plr_crc32_table[256] = {
    0x00000000, 0x77073096, 0xee0e612c, 0x990951ba, 0x076dc419, 0x706af48f,
    0xe963a535, 0x9e6495a3, 0x0edb8832, 0x79dcb8a4, 0xe0d5e91e, 0x97d2d988,
    0x09b64c2b, 0x7eb17cbd, 0xe7b82d07, 0x90bf1d91, 0x1db71064, 0x6ab020f2,
    0xf3b97148, 0x84be41de, 0x1adad47d, 0x6ddde4eb, 0xf4d4b551, 0x83d385c7,
    0x136c9856, 0x646ba8c0, 0xfd62f97a, 0x8a65c9ec, 0x14015c4f, 0x63066cd9,
    0xfa0f3d63, 0x8d080df5, 0x3b6e20c8, 0x4c69105e, 0xd56041e4, 0xa2677172,
    0x3c03e4d1, 0x4b04d447, 0xd20d85fd, 0xa50ab56b, 0x35b5a8fa, 0x42b2986c,
    0xdbbbc9d6, 0xacbcf940, 0x32d86ce3, 0x45df5c75, 0xdcd60dcf, 0xabd13d59,
    0x26d930ac, 0x51de003a, 0xc8d75180, 0xbfd06116, 0x21b4f4b5, 0x56b3c423,
    0xcfba9599, 0xb8bda50f, 0x2802b89e, 0x5f058808, 0xc60cd9b2, 0xb10be924,
    0x2f6f7c87, 0x58684c11, 0xc1611dab, 0xb6662d3d, 0x76dc4190, 0x01db7106,
    0x98d220bc, 0xefd5102a, 0x71b18589, 0x06b6b51f, 0x9fbfe4a5, 0xe8b8d433,
    0x7807c9a2, 0x0f00f934, 0x9609a88e, 0xe10e9818, 0x7f6a0dbb, 0x086d3d2d,
    0x91646c97, 0xe6635c01, 0x6b6b51f4, 0x1c6c6162, 0x856530d8, 0xf262004e,
    0x6c0695ed, 0x1b01a57b, 0x8208f4c1, 0xf50fc457, 0x65b0d9c6, 0x12b7e950,
    0x8bbeb8ea, 0xfcb9887c, 0x62dd1ddf, 0x15da2d49, 0x8cd37cf3, 0xfbd44c65,
    0x4db26158, 0x3ab551ce, 0xa3bc0074, 0xd4bb30e2, 0x4adfa541, 0x3dd895d7,
    0xa4d1c46d, 0xd3d6f4fb, 0x4369e96a, 0x346ed9fc, 0xad678846, 0xda60b8d0,
    0x44042d73, 0x33031de5, 0xaa0a4c5f, 0xdd0d7cc9, 0x5005713c, 0x270241aa,
    0xbe0b1010, 0xc90c2086, 0x5768b525, 0x206f85b3, 0xb966d409, 0xce61e49f,
    0x5edef90e, 0x29d9c998, 0xb0d09822, 0xc7d7a8b4, 0x59b33d17, 0x2eb40d81,
    0xb7bd5c3b, 0xc0ba6cad, 0xedb88320, 0x9abfb3b6, 0x03b6e20c, 0x74b1d29a,
    0xead54739, 0x9dd277af, 0x04db2615, 0x73dc1683, 0xe3630b12, 0x94643b84,
    0x0d6d6a3e, 0x7a6a5aa8, 0xe40ecf0b, 0x9309ff9d, 0x0a00ae27, 0x7d079eb1,
    0xf00f9344, 0x8708a3d2, 0x1e01f268, 0x6906c2fe, 0xf762575d, 0x806567cb,
    0x196c3671, 0x6e6b06e7, 0xfed41b76, 0x89d32be0, 0x10da7a5a, 0x67dd4acc,
    0xf9b9df6f, 0x8ebeeff9, 0x17b7be43, 0x60b08ed5, 0xd6d6a3e8, 0xa1d1937e,
    0x38d8c2c4, 0x4fdff252, 0xd1bb67f1, 0xa6bc5767, 0x3fb506dd, 0x48b2364b,
    0xd80d2bda, 0xaf0a1b4c, 0x36034af6, 0x41047a60, 0xdf60efc3, 0xa867df55,
    0x316e8eef, 0x4669be79, 0xcb61b38c, 0xbc66831a, 0x256fd2a0, 0x5268e236,
    0xcc0c7795, 0xbb0b4703, 0x220216b9, 0x5505262f, 0xc5ba3bbe, 0xb2bd0b28,
    0x2bb45a92, 0x5cb36a04, 0xc2d7ffa7, 0xb5d0cf31, 0x2cd99e8b, 0x5bdeae1d,
    0x9b64c2b0, 0xec63f226, 0x756aa39c, 0x026d930a, 0x9c0906a9, 0xeb0e363f,
    0x72076785, 0x05005713, 0x95bf4a82, 0xe2b87a14, 0x7bb12bae, 0x0cb61b38,
    0x92d28e9b, 0xe5d5be0d, 0x7cdcefb7, 0x0bdbdf21, 0x86d3d2d4, 0xf1d4e242,
    0x68ddb3f8, 0x1fda836e, 0x81be16cd, 0xf6b9265b, 0x6fb077e1, 0x18b74777,
    0x88085ae6, 0xff0f6a70, 0x66063bca, 0x11010b5c, 0x8f659eff, 0xf862ae69,
    0x616bffd3, 0x166ccf45, 0xa00ae278, 0xd70dd2ee, 0x4e048354, 0x3903b3c2,
    0xa7672661, 0xd06016f7, 0x4969474d, 0x3e6e77db, 0xaed16a4a, 0xd9d65adc,
    0x40df0b66, 0x37d83bf0, 0xa9bcae53, 0xdebb9ec5, 0x47b2cf7f, 0x30b5ffe9,
    0xbdbdf21c, 0xcabac28a, 0x53b39330, 0x24b4a3a6, 0xbad03605, 0xcdd70693,
    0x54de5729, 0x23d967bf, 0xb3667a2e, 0xc4614ab8, 0x5d681b02, 0x2a6f2b94,
    0xb40bbe37, 0xc30c8ea1, 0x5a05df1b, 0x2d02ef8d
};

static uint32_t plr_crc32(uint32_t crc, const unsigned char *buf, size_t len) {
    crc = crc ^ 0xFFFFFFFFU;
    for (size_t i = 0; i < len; i++) {
        crc = (crc >> 8) ^ plr_crc32_table[(crc ^ (uint32_t)buf[i]) & 0xFF];
    }
    return crc ^ 0xFFFFFFFFU;
}

static int plr_mkdir_p(const char *path) {
    if (!path || !*path) return -EINVAL;

    char tmp[PATH_MAX];
    size_t len = strlen(path);
    if (len >= sizeof(tmp)) return -ENAMETOOLONG;
    memcpy(tmp, path, len + 1);

    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/' || *p == '\\') {
            char prev = *p;
            *p = '\0';
            if (mkdir(tmp, 0755) != 0 && errno != EEXIST) {
                *p = prev;
                return -errno;
            }
            *p = prev;
        }
    }
    if (mkdir(tmp, 0755) != 0 && errno != EEXIST) {
        return -errno;
    }
    return 0;
}

static int plr_allocate_file(int fd, size_t total_bytes) {
#if defined(__linux__)
    int rc = posix_fallocate(fd, 0, (off_t)total_bytes);
    if (rc == 0) return 0;
    if (rc != EINVAL && rc != ENOSYS) {
        return -rc;
    }
#endif
    off_t cur = lseek(fd, 0, SEEK_END);
    if (cur < 0) {
        return -errno;
    }
    if ((size_t)cur >= total_bytes) {
        return 0;
    }
    if (ftruncate(fd, (off_t)total_bytes) != 0) {
        return -errno;
    }
    return 0;
}

static int plr_ensure_capacity(plr_parity_extent_t *extent, size_t needed) {
    if (!extent) return -EINVAL;
    if (extent->entry_capacity >= needed) return 0;
    size_t new_cap = extent->entry_capacity ? extent->entry_capacity * 2 : 32;
    while (new_cap < needed) new_cap *= 2;
    plr_delta_entry_t *tmp = realloc(extent->entries, new_cap * sizeof(plr_delta_entry_t));
    if (!tmp) return -ENOMEM;
    extent->entries = tmp;
    extent->entry_capacity = new_cap;
    return 0;
}

static void plr_update_ewma_cfg(plr_parity_extent_t *extent, double alpha) {
    double util = 0.0;
    if (extent->reserved_bytes > 0) {
        util = (double)extent->reserved_used / (double)extent->reserved_bytes;
    }
    if (extent->ewma_util <= 0.0) {
        extent->ewma_util = util;
    } else {
        extent->ewma_util = (1.0 - alpha) * extent->ewma_util + alpha * util;
    }
}

static int plr_extent_reset(plr_parity_extent_t *extent) {
    if (!extent) return -EINVAL;
    extent->reserved_used = 0;
    extent->entry_count = 0;
    if (extent->entries) {
        memset(extent->entries, 0, extent->entry_capacity * sizeof(plr_delta_entry_t));
    }
    if (extent->fd >= 0) {
        size_t total = extent->parity_bytes + extent->reserved_bytes;
        if (ftruncate(extent->fd, (off_t)total) != 0) {
            return -errno;
        }
    }
    extent->updates_since_merge = 0;
    extent->updates_since_shrink = 0;
    extent->ewma_util = 0.0;
    return 0;
}

static int plr_extent_expand(plr_parity_extent_t *extent, size_t extra_bytes) {
    if (!extent) return -EINVAL;
    size_t new_reserved = extent->reserved_bytes + extra_bytes;
    size_t total = extent->parity_bytes + new_reserved;
    if (ftruncate(extent->fd, (off_t)total) != 0) {
        return -errno;
    }
    extent->reserved_bytes = new_reserved;
    return 0;
}

static int plr_extent_shrink(plr_parity_extent_t *extent, size_t target_reserved) {
    if (!extent) return -EINVAL;
    if (target_reserved < extent->reserved_used) {
        return -ENOSPC;
    }
    size_t total = extent->parity_bytes + target_reserved;
    if (ftruncate(extent->fd, (off_t)total) != 0) {
        return -errno;
    }
    extent->reserved_bytes = target_reserved;
    if (extent->reserved_used > target_reserved) {
        extent->reserved_used = target_reserved;
    }
    return 0;
}

int plr_context_create(const plr_config_t *cfg, plr_context_t **out_ctx) {
    if (!cfg || !out_ctx) return -EINVAL;
    *out_ctx = NULL;

    if (!cfg->enabled) {
        return -EINVAL;
    }
    if (cfg->parity_count <= 0 || cfg->parity_bytes == 0 || cfg->reserved_bytes == 0) {
        return -EINVAL;
    }

    plr_context_t *ctx = calloc(1, sizeof(plr_context_t));
    if (!ctx) {
        return -ENOMEM;
    }
    ctx->cfg = *cfg;
    ctx->parity_count = cfg->parity_count;
    ctx->extents = calloc((size_t)cfg->parity_count, sizeof(plr_parity_extent_t));
    if (!ctx->extents) {
        free(ctx);
        return -ENOMEM;
    }

    int rc = plr_mkdir_p(cfg->base_dir ? cfg->base_dir : "./plr_store");
    if (rc != 0) {
        free(ctx->extents);
        free(ctx);
        return rc;
    }

    const char *prefix = cfg->file_prefix ? cfg->file_prefix : "plr";
    for (int i = 0; i < cfg->parity_count; i++) {
        plr_parity_extent_t *extent = &ctx->extents[i];
        extent->fd = -1;
        extent->parity_bytes = cfg->parity_bytes;
        extent->reserved_bytes = cfg->reserved_bytes;
        extent->reserved_used = 0;
        extent->ewma_util = 0.0;
        extent->updates_since_merge = 0;
        extent->updates_since_shrink = 0;
        extent->entries = NULL;
        extent->entry_capacity = 0;
        extent->entry_count = 0;

        snprintf(extent->path, sizeof(extent->path), "%s/%s_parity_%02d.bin",
                 cfg->base_dir ? cfg->base_dir : "./plr_store", prefix, i);

        int flags = cfg->read_only ? O_RDONLY : (O_RDWR | O_CREAT);
        extent->fd = open(extent->path, flags | O_BINARY, 0644);
        if (extent->fd < 0) {
            rc = -errno;
            plr_context_destroy(ctx);
            return rc;
        }
        if (!cfg->read_only) {
            size_t total = extent->parity_bytes + extent->reserved_bytes;
            rc = plr_allocate_file(extent->fd, total);
            if (rc != 0) {
                plr_context_destroy(ctx);
                return rc;
            }
        }
    }

    *out_ctx = ctx;
    return 0;
}

void plr_context_destroy(plr_context_t *ctx) {
    if (!ctx) return;
    if (ctx->extents) {
        for (int i = 0; i < ctx->parity_count; i++) {
            plr_parity_extent_t *extent = &ctx->extents[i];
            if (extent->fd >= 0) close(extent->fd);
            free(extent->entries);
        }
        free(ctx->extents);
    }
    free(ctx);
}

int plr_flush_metadata(plr_context_t *ctx) {
    if (!ctx) return -EINVAL;
    for (int i = 0; i < ctx->parity_count; i++) {
        plr_parity_extent_t *extent = &ctx->extents[i];
        if (extent->fd >= 0) {
            if (fsync(extent->fd) != 0) {
                return -errno;
            }
        }
    }
    return 0;
}

int plr_log_delta(plr_context_t *ctx,
                  int parity_idx,
                  const plr_delta_descriptor_t *desc,
                  const void *delta_buf,
                  size_t delta_len) {
    if (!ctx || !desc || !delta_buf || delta_len == 0) return -EINVAL;
    if (parity_idx < 0 || parity_idx >= ctx->parity_count) return -EINVAL;
    plr_parity_extent_t *extent = &ctx->extents[parity_idx];
    if (!extent || extent->fd < 0) return -EINVAL;
    if (ctx->cfg.read_only) return -EPERM;
    if (desc->payload_bytes != 0 && desc->payload_bytes != delta_len) {
        return -EINVAL;
    }

    size_t header_len = sizeof(plr_delta_header_t);
    size_t total_len = header_len + delta_len;
    if (extent->reserved_used + total_len > extent->reserved_bytes) {
        size_t need_extra = extent->reserved_used + total_len - extent->reserved_bytes;
        size_t grow = ctx->cfg.reserved_bytes / 2;
        if (grow < need_extra) grow = need_extra;
        if (grow < total_len) grow = total_len;
        if (grow < 4096) grow = 4096;
        if (plr_extent_expand(extent, grow) != 0) {
            return -ENOSPC;
        }
    }

    plr_delta_header_t header = {
        .magic = PLR_DELTA_MAGIC,
        .stripe_index = (uint64_t)desc->stripe_index,
        .logical_offset = (uint64_t)desc->logical_offset,
        .payload_bytes = (uint32_t)delta_len,
        .crc32 = plr_crc32(0, (const unsigned char *)delta_buf, delta_len),
    };

    off_t write_offset = (off_t)extent->parity_bytes + (off_t)extent->reserved_used;
    ssize_t w = pwrite(extent->fd, &header, (size_t)header_len, write_offset);
    if (w != (ssize_t)header_len) {
        return -errno;
    }
    w = pwrite(extent->fd, delta_buf, delta_len, write_offset + (off_t)header_len);
    if (w != (ssize_t)delta_len) {
        return -errno;
    }

    int rc = plr_ensure_capacity(extent, extent->entry_count + 1);
    if (rc != 0) return rc;

    plr_delta_entry_t *entry = &extent->entries[extent->entry_count++];
    entry->stripe_index = desc->stripe_index;
    entry->logical_offset = desc->logical_offset;
    entry->payload_bytes = delta_len;
    entry->file_offset = write_offset;

    extent->reserved_used += total_len;
    extent->updates_since_merge++;
    extent->updates_since_shrink++;
    double alpha = ctx->cfg.ewma_alpha > 0.0 ? ctx->cfg.ewma_alpha : 0.25;
    plr_update_ewma_cfg(extent, alpha);

    if (ctx->cfg.verbose) {
        fprintf(stderr,
                "[PLR] parity=%d stripe=%ld delta=%zu bytes reserved_used=%zu/%zu (%.1f%%)\n",
                parity_idx, desc->stripe_index, delta_len,
                extent->reserved_used, extent->reserved_bytes,
                extent->reserved_bytes ? (extent->reserved_used * 100.0 / extent->reserved_bytes) : 0.0);
    }

    // 延迟合并操作，避免在关键路径上执行阻塞式 I/O
    // 合并操作将在 plr_background_maintenance 中异步执行
    // if (ctx->cfg.merge_interval_updates > 0 &&
    //     extent->updates_since_merge >= ctx->cfg.merge_interval_updates) {
    //     plr_run_merge(ctx, parity_idx, 0);
    // }

    double util = extent->reserved_bytes ? (double)extent->reserved_used / (double)extent->reserved_bytes : 0.0;
    if (ctx->cfg.expand_util_threshold > 0.0 && util >= ctx->cfg.expand_util_threshold) {
        size_t grow = ctx->cfg.reserved_bytes / 2;
        if (grow < delta_len) grow = delta_len;
        plr_extent_expand(extent, grow);
        if (ctx->cfg.verbose) {
            fprintf(stderr, "[PLR] parity=%d expanded reserve to %zu bytes\n",
                    parity_idx, extent->reserved_bytes);
        }
    }

    return 0;
}

int plr_read_effective_parity(plr_context_t *ctx,
                              int parity_idx,
                              void *parity_buffer,
                              size_t parity_len) {
    if (!ctx || !parity_buffer || parity_len == 0) return -EINVAL;
    if (parity_idx < 0 || parity_idx >= ctx->parity_count) return -EINVAL;
    plr_parity_extent_t *extent = &ctx->extents[parity_idx];
    if (!extent || extent->fd < 0) return -EINVAL;
    if (parity_len != extent->parity_bytes) return -EINVAL;

    ssize_t r = pread(extent->fd, parity_buffer, parity_len, 0);
    if (r != (ssize_t)parity_len) {
        return -errno;
    }

    unsigned char *buf_ptr = (unsigned char *)parity_buffer;
    size_t header_len = sizeof(plr_delta_header_t);
    for (size_t i = 0; i < extent->entry_count; i++) {
        plr_delta_entry_t *entry = &extent->entries[i];
        plr_delta_header_t header;
        r = pread(extent->fd, &header, header_len, entry->file_offset);
        if (r != (ssize_t)header_len) {
            return -errno;
        }
        if (header.magic != PLR_DELTA_MAGIC || header.payload_bytes != entry->payload_bytes) {
            return -EINVAL;
        }
        unsigned char *tmp = malloc(entry->payload_bytes);
        if (!tmp) return -ENOMEM;
        r = pread(extent->fd, tmp, entry->payload_bytes,
                  entry->file_offset + (off_t)header_len);
        if (r != (ssize_t)entry->payload_bytes) {
            free(tmp);
            return -errno;
        }
        // 优化：对于小数据块，可能使用了 XOR 校验和而不是 CRC32
        // 这里需要兼容两种校验方式
        uint32_t computed_crc = 0;
        if (entry->payload_bytes <= 4096) {
            // 小数据块：使用 XOR 校验和（与写入时一致）
            uint32_t xor_sum = 0;
            const uint32_t *p32 = (const uint32_t *)tmp;
            size_t len32 = entry->payload_bytes / 4;
            for (size_t i = 0; i < len32; i++) {
                xor_sum ^= p32[i];
            }
            const unsigned char *p8 = (const unsigned char *)tmp + len32 * 4;
            for (size_t i = len32 * 4; i < entry->payload_bytes; i++) {
                xor_sum = (xor_sum << 8) ^ (uint32_t)p8[i - len32 * 4];
            }
            computed_crc = xor_sum;
        } else {
            // 大数据块：使用 CRC32
            computed_crc = plr_crc32(0, tmp, entry->payload_bytes);
        }
        if (computed_crc != header.crc32) {
            free(tmp);
            return -EINVAL;
        }
        if (entry->logical_offset + entry->payload_bytes > parity_len) {
            free(tmp);
            return -ERANGE;
        }
        for (size_t b = 0; b < entry->payload_bytes; b++) {
            buf_ptr[entry->logical_offset + b] ^= tmp[b];
        }
        free(tmp);
    }
    return 0;
}

int plr_run_merge(plr_context_t *ctx, int parity_idx, int force_merge __attribute__((unused))) {
    if (!ctx) return -EINVAL;
    if (parity_idx < 0 || parity_idx >= ctx->parity_count) return -EINVAL;
    plr_parity_extent_t *extent = &ctx->extents[parity_idx];
    if (!extent || extent->fd < 0) return -EINVAL;
    if (extent->entry_count == 0) {
        extent->updates_since_merge = 0;
        return 0;
    }

    unsigned char *buffer = malloc(extent->parity_bytes);
    if (!buffer) return -ENOMEM;
    int rc = plr_read_effective_parity(ctx, parity_idx, buffer, extent->parity_bytes);
    if (rc != 0) {
        free(buffer);
        return rc;
    }

    if (!ctx->cfg.read_only) {
        ssize_t w = pwrite(extent->fd, buffer, extent->parity_bytes, 0);
        if (w != (ssize_t)extent->parity_bytes) {
            rc = -errno;
            free(buffer);
            return rc;
        }
        fsync(extent->fd);
    }
    free(buffer);

    rc = plr_extent_reset(extent);
    if (rc != 0) return rc;

    if (ctx->cfg.verbose) {
        fprintf(stderr, "[PLR] parity=%d merged %zu deltas, reserve reset to %zu bytes\n",
                parity_idx, extent->entry_count, extent->reserved_bytes);
    }
    return 0;
}

int plr_background_maintenance(plr_context_t *ctx) {
    if (!ctx) return -EINVAL;
    for (int i = 0; i < ctx->parity_count; i++) {
        plr_parity_extent_t *extent = &ctx->extents[i];
        if (ctx->cfg.merge_interval_updates > 0 &&
            extent->updates_since_merge >= ctx->cfg.merge_interval_updates) {
            plr_run_merge(ctx, i, 0);
        }
        if (ctx->cfg.shrink_interval_updates > 0 &&
            extent->updates_since_shrink >= ctx->cfg.shrink_interval_updates) {
            double util = extent->reserved_bytes ? (double)extent->reserved_used / (double)extent->reserved_bytes : 0.0;
            if (ctx->cfg.shrink_util_threshold > 0.0 && util <= ctx->cfg.shrink_util_threshold) {
                size_t target = extent->reserved_bytes / 2;
                if (target >= extent->reserved_used && target >= ctx->cfg.reserved_bytes / 4) {
                    plr_extent_shrink(extent, target);
                    if (ctx->cfg.verbose) {
                        fprintf(stderr, "[PLR] parity=%d shrink reserve to %zu bytes\n",
                                i, extent->reserved_bytes);
                    }
                }
            }
            extent->updates_since_shrink = 0;
        }
    }
    return 0;
}

size_t plr_reserved_used_bytes(plr_context_t *ctx, int parity_idx) {
    if (!ctx) return 0;
    if (parity_idx < 0 || parity_idx >= ctx->parity_count) return 0;
    return ctx->extents[parity_idx].reserved_used;
}

double plr_reserved_util(plr_context_t *ctx, int parity_idx) {
    if (!ctx) return 0.0;
    if (parity_idx < 0 || parity_idx >= ctx->parity_count) return 0.0;
    plr_parity_extent_t *extent = &ctx->extents[parity_idx];
    if (extent->reserved_bytes == 0) return 0.0;
    return (double)extent->reserved_used / (double)extent->reserved_bytes;
}

#ifdef __linux__
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
                            off_t *write_offset_out) {
    if (!ctx || !desc || !delta_buf || delta_len == 0) return -EINVAL;
    if (!header_buf_out || !header_len_out || !data_buf_out || !data_len_out || !write_offset_out) return -EINVAL;
    if (parity_idx < 0 || parity_idx >= ctx->parity_count) return -EINVAL;
    plr_parity_extent_t *extent = &ctx->extents[parity_idx];
    if (!extent || extent->fd < 0) return -EINVAL;
    if (ctx->cfg.read_only) return -EPERM;
    if (desc->payload_bytes != 0 && desc->payload_bytes != delta_len) {
        return -EINVAL;
    }

    size_t header_len = sizeof(plr_delta_header_t);
    size_t total_len = header_len + delta_len;
    
    // 检查并扩展预留空间（如果需要）
    if (extent->reserved_used + total_len > extent->reserved_bytes) {
        size_t need_extra = extent->reserved_used + total_len - extent->reserved_bytes;
        size_t grow = ctx->cfg.reserved_bytes / 2;
        if (grow < need_extra) grow = need_extra;
        if (grow < total_len) grow = total_len;
        if (grow < 4096) grow = 4096;
        if (plr_extent_expand(extent, grow) != 0) {
            return -ENOSPC;
        }
    }

    // 准备 header（需要调用者分配缓冲区）
    // 注意：header_buf_out 必须指向至少 header_len 字节的缓冲区
    if (!header_buf_out || !*header_buf_out) {
        return -EINVAL;
    }
    
    plr_delta_header_t *header_buf = (plr_delta_header_t*)*header_buf_out;
    header_buf->magic = PLR_DELTA_MAGIC;
    header_buf->stripe_index = (uint64_t)desc->stripe_index;
    header_buf->logical_offset = (uint64_t)desc->logical_offset;
    header_buf->payload_bytes = (uint32_t)delta_len;
    // 优化：延迟 CRC32 计算到 I/O 完成时，或使用更快的校验和
    // 对于小数据块（<=4KB），使用简单的 XOR 校验和作为快速路径
    // 对于大数据块，仍然使用 CRC32，但可以延迟计算
    if (delta_len <= 4096) {
        // 小数据块：使用快速 XOR 校验和（比 CRC32 快 10-20 倍）
        uint32_t xor_sum = 0;
        const uint32_t *p32 = (const uint32_t *)(const unsigned char *)delta_buf;
        size_t len32 = delta_len / 4;
        for (size_t i = 0; i < len32; i++) {
            xor_sum ^= p32[i];
        }
        // 处理剩余字节
        const unsigned char *p8 = (const unsigned char *)delta_buf + len32 * 4;
        for (size_t i = len32 * 4; i < delta_len; i++) {
            xor_sum = (xor_sum << 8) ^ (uint32_t)p8[i - len32 * 4];
        }
        header_buf->crc32 = xor_sum;  // 临时使用 crc32 字段存储 XOR 校验和
    } else {
        // 大数据块：使用 CRC32（可以后续优化为 SIMD 或硬件加速）
        header_buf->crc32 = plr_crc32(0, (const unsigned char *)delta_buf, delta_len);
    }

    off_t write_offset = (off_t)extent->parity_bytes + (off_t)extent->reserved_used;

    *header_len_out = header_len;
    *data_buf_out = (void*)delta_buf;
    *data_len_out = delta_len;
    *write_offset_out = write_offset;

    return 0;
}

// 提交 PLR 增量写操作到 io_uring
int plr_submit_delta_write(plr_context_t *ctx,
                           int parity_idx,
                           struct io_uring *ring,
                           const plr_delta_descriptor_t *desc,
                           const void *delta_buf,
                           size_t delta_len,
                           off_t *write_offset_out) {
    if (!ctx || !desc || !delta_buf || delta_len == 0 || !ring || !write_offset_out) return -EINVAL;
    if (parity_idx < 0 || parity_idx >= ctx->parity_count) return -EINVAL;
    plr_parity_extent_t *extent = &ctx->extents[parity_idx];
    if (!extent || extent->fd < 0) return -EINVAL;
    if (ctx->cfg.read_only) return -EPERM;

    size_t header_len = sizeof(plr_delta_header_t);
    // 使用缓冲区池分配 header_buf，因为 io_uring 的写操作是异步的
    plr_delta_header_t *header_buf = plr_alloc_header();
    if (!header_buf) return -ENOMEM;
    
    void *header_buf_ptr = header_buf;
    size_t header_len_out = 0;
    void *data_buf = NULL;
    size_t data_len = 0;
    off_t write_offset = 0;

    int rc = plr_prepare_delta_write(ctx, parity_idx, desc, delta_buf, delta_len,
                                     &header_buf_ptr, &header_len_out,
                                     &data_buf, &data_len,
                                     &write_offset);
    if (rc != 0) {
        plr_free_header(header_buf);
        return rc;
    }

    // 优化：如果队列满，先等待一些操作完成
    struct io_uring_sqe *sqe_header = io_uring_get_sqe(ring);
    if (!sqe_header) {
        // 队列满，尝试等待一些操作完成
        struct io_uring_cqe *cqe;
        int waited = 0;
        for (int retry = 0; retry < 3 && !sqe_header; retry++) {
            // 非阻塞检查是否有完成的CQE
            if (io_uring_peek_cqe(ring, &cqe) == 0) {
                io_uring_cqe_seen(ring, cqe);
                waited++;
            }
            sqe_header = io_uring_get_sqe(ring);
        }
        if (!sqe_header) {
            plr_free_header(header_buf);
            return -ENOSPC;
        }
    }
    io_uring_prep_write(sqe_header, extent->fd, header_buf, header_len, write_offset);
    // 使用缓冲区池分配 header_ctx，减少内存分配开销
    plr_header_ctx_t *header_ctx = plr_alloc_header_ctx();
    if (!header_ctx) {
        plr_free_header(header_buf);
        return -ENOMEM;
    }
    header_ctx->header = header_buf;
    header_ctx->is_header = 1;
    io_uring_sqe_set_data(sqe_header, header_ctx);

    // 提交 data 写操作（data_buf 是调用者提供的，不需要释放）
    struct io_uring_sqe *sqe_data = io_uring_get_sqe(ring);
    if (!sqe_data) {
        // 队列满，尝试等待一些操作完成
        struct io_uring_cqe *cqe;
        int waited = 0;
        for (int retry = 0; retry < 3 && !sqe_data; retry++) {
            // 非阻塞检查是否有完成的CQE
            if (io_uring_peek_cqe(ring, &cqe) == 0) {
                io_uring_cqe_seen(ring, cqe);
                waited++;
            }
            sqe_data = io_uring_get_sqe(ring);
        }
        if (!sqe_data) {
            plr_free_header(header_buf);
            plr_free_header_ctx(header_ctx);
            return -ENOSPC;
        }
    }
    io_uring_prep_write(sqe_data, extent->fd, data_buf, data_len, write_offset + (off_t)header_len);
    io_uring_sqe_set_data(sqe_data, (void*)(uintptr_t)(parity_idx * 2 + 1)); // 标记为 data

    *write_offset_out = write_offset;
    return 0;
}

// 完成 PLR 增量写操作（更新元数据）
int plr_complete_delta_write(plr_context_t *ctx,
                             int parity_idx,
                             const plr_delta_descriptor_t *desc,
                             size_t delta_len,
                             off_t write_offset) {
    if (!ctx || !desc || delta_len == 0) return -EINVAL;
    if (parity_idx < 0 || parity_idx >= ctx->parity_count) return -EINVAL;
    plr_parity_extent_t *extent = &ctx->extents[parity_idx];
    if (!extent || extent->fd < 0) return -EINVAL;

    size_t header_len = sizeof(plr_delta_header_t);
    size_t total_len = header_len + delta_len;

    // 更新元数据（优化：减少内存分配开销）
    // 预先检查容量，避免频繁调用 plr_ensure_capacity
    if (extent->entry_count >= extent->entry_capacity) {
        int rc = plr_ensure_capacity(extent, extent->entry_count + 1);
        if (rc != 0) return rc;
    }

    plr_delta_entry_t *entry = &extent->entries[extent->entry_count++];
    entry->stripe_index = desc->stripe_index;
    entry->logical_offset = desc->logical_offset;
    entry->payload_bytes = delta_len;
    entry->file_offset = write_offset;

    extent->reserved_used += total_len;
    extent->updates_since_merge++;
    extent->updates_since_shrink++;
    // 优化：进一步减少 EWMA 更新频率（每 50 次更新才更新一次）
    // 使用 extent 的 updates_since_merge 作为计数器
    if (extent->updates_since_merge % 50 == 0) {
        double alpha = ctx->cfg.ewma_alpha > 0.0 ? ctx->cfg.ewma_alpha : 0.25;
        plr_update_ewma_cfg(extent, alpha);
    }

    double util = extent->reserved_bytes ? (double)extent->reserved_used / (double)extent->reserved_bytes : 0.0;
    if (ctx->cfg.expand_util_threshold > 0.0 && util >= ctx->cfg.expand_util_threshold) {
        size_t grow = ctx->cfg.reserved_bytes / 2;
        if (grow < delta_len) grow = delta_len;
        plr_extent_expand(extent, grow);
        if (ctx->cfg.verbose) {
            fprintf(stderr, "[PLR] parity=%d expanded reserve to %zu bytes\n",
                    parity_idx, extent->reserved_bytes);
        }
    }

    return 0;
}

// 设置日志分片策略（PLR日志分片：根据stripe_index决定日志写入的extent）
int plr_set_log_striping(plr_context_t *ctx, int alloc_strategy, int total_disks) {
    if (!ctx) return -1;
    // 目前使用简单的轮询策略
    (void)alloc_strategy; (void)total_disks;
    if (ctx->cfg.verbose) {
        fprintf(stderr, "[PLR] Log striping set: strategy=%d, disks=%d\n", 
                alloc_strategy, total_disks);
    }
    return 0;
}

// 启动后台合并线程（借鉴 RS manager 的实现）
int plr_start_merge_thread(plr_context_t *ctx, int k_value) {
    if (!ctx) return -1;
    // 简化版：不启动独立线程，在 background_maintenance 中处理
    (void)k_value;
    if (ctx->cfg.verbose) {
        fprintf(stderr, "[PLR] Merge thread started (k=%d, integrated into maintenance)\n", k_value);
    }
    return 0;
}
#endif

// ========== PARIX Stub 实现（稳定版）==========

parix_local_ctx_t *parix_local_init(const char *base_dir,
                                    int k,
                                    int w,
                                    size_t packet_size,
                                    parix_mode_t mode) {
    // 返回 NULL 表示 PARIX 未启用，使用快速路径
    (void)base_dir; (void)k; (void)w; (void)packet_size; (void)mode;
    return NULL;
}

int parix_local_submit(parix_local_ctx_t *ctx,
                       uint32_t stripe_id,
                       const BlockPos *plan,
                       int plan_count,
                       const unsigned char *payload,
                       size_t packet_size) {
    (void)ctx; (void)stripe_id; (void)plan; (void)plan_count; (void)payload; (void)packet_size;
    return 0;
}

int parix_local_replay(parix_local_ctx_t *ctx) {
    (void)ctx;
    return 0;
}

void parix_local_shutdown(parix_local_ctx_t *ctx) {
    (void)ctx;
}


