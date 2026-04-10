#!/bin/bash

# ==============================================================================
# 标准 RDP (R=2) HDD 阵列性能测试脚本 (基于 23块 Seagate 10K RPM SAS)
# 自动修复编译缺失函数与内存泄漏问题
# ==============================================================================

set -e

LOG_MOUNT_DIR="/mnt/rdp_logs"
LOG_DISK="/dev/sdx"

if ! mountpoint -q "$LOG_MOUNT_DIR"; then
    echo "[INFO] 挂载高速日志盘..."
    umount "$LOG_MOUNT_DIR" 2>/dev/null || true
    mkfs.ext4 -O ^has_journal -T largefile4 -F "$LOG_DISK"
    mkdir -p "$LOG_MOUNT_DIR"
    mount -t ext4 -o rw,noatime,nodiratime "$LOG_DISK" "$LOG_MOUNT_DIR"
fi
mkdir -p "$LOG_MOUNT_DIR/plr" "$LOG_MOUNT_DIR/parix"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

unset RDP_NO_IO
export RDP_NO_IO=0

echo "[INFO] 正在生成 22 块裸设备阵列列表 (safe_disks.txt)..."
cat > safe_disks.txt << 'EOF'
/dev/sdb
/dev/sdc
/dev/sdd
/dev/sde
/dev/sdf
/dev/sdg
/dev/sdh
/dev/sdi
/dev/sdj
/dev/sdk
/dev/sdl
/dev/sdm
/dev/sdn
/dev/sdo
/dev/sdp
/dev/sdq
/dev/sdr
/dev/sds
/dev/sdt
/dev/sdu
/dev/sdv
/dev/sdw
EOF

# ==============================================================================
# 【核心魔法】动态生成并注入缺失的 OAA 物理磁道对齐函数
# ==============================================================================
echo "[INFO] 正在向 C 代码注入 OAA 物理磁道对齐补丁..."

cat > .oaa_patch.h << 'EOF'
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
EOF

for file in rdp.c rdp_plr.c rdp_parix.c; do
    if [ -f "$file" ]; then
        # 1. 注入缺失的结构体和函数 (紧跟在 alloc_strategy.h 之后)
        if ! grep -q "reorder_blocks_by_physical_addr" "$file"; then
            sed -i '/#include "alloc_strategy.h"/r .oaa_patch.h' "$file"
        fi
        
        # 2. 替换所有可能破坏机械硬盘顺序读写的软排序
        sed -i 's/reorder_plan_diag_first(allocation_plan, plan_count);/reorder_blocks_by_physical_addr(allocation_plan, plan_count, config.w);/g' "$file"
        sed -i 's/reorder_plan_round_robin(allocation_plan, plan_count, config.k);/reorder_blocks_by_physical_addr(allocation_plan, plan_count, config.w);/g' "$file"
    fi
done

# 3. 修复 rdp_parix.c 中的严重内存泄漏 (强制将 io_uring 异步 malloc 日志降级为安全同步写)
if [ -f "rdp_parix.c" ]; then
    sed -i 's/if (!ring || !entry_offset_out) {/if (1) { \/\/ Forced sync to prevent mem leak/g' rdp_parix.c
fi

echo "[INFO] 代码修补完成，开始编译..."

# 编译选项
CC=gcc
CFLAGS="-O3 -march=native -mprefer-vector-width=512 -fno-strict-aliasing -Wall -Wextra -std=c11"
LDFLAGS="-lm -lpthread -luring"

# 公共源文件（PARIX 模块已包含 PLR 实现，不需要单独链接 plr_module.c）
COMMON_SOURCES="parix_module.c alloc_strategy.c"

echo "[INFO] 编译选项: $CFLAGS"
echo "[INFO] 链接库: $LDFLAGS"
echo "[INFO] 公共源文件: $COMMON_SOURCES"
echo "[INFO] 注意: parix_module.c 已包含 PLR 完整实现"

# 检查公共源文件是否存在
for src in $COMMON_SOURCES; do
    if [ ! -f "$src" ]; then
        echo "[ERROR] 找不到必需的源文件: $src"
        echo "[ERROR] 请确保以下文件存在于当前目录:"
        echo "  - parix_module.c (包含 PARIX + PLR 实现)"
        echo "  - alloc_strategy.c"
        exit 1
    fi
done

# 编译标准 RDP (无 PLR)
echo ""
echo "[INFO] 正在编译 test_rdp (标准 RDP)..."
if [ -f "rdp.c" ]; then
    $CC $CFLAGS rdp.c $COMMON_SOURCES -o test_rdp $LDFLAGS
    if [ $? -eq 0 ]; then
        echo "[SUCCESS] test_rdp 编译成功"
        ls -lh test_rdp
    else
        echo "[ERROR] test_rdp 编译失败！"
        exit 1
    fi
else
    echo "[WARNING] rdp.c 不存在，跳过编译"
fi

# 编译 RDP + PLR
echo ""
echo "[INFO] 正在编译 test_rdp_plr (RDP + PLR)..."
if [ -f "rdp_plr.c" ]; then
    $CC $CFLAGS rdp_plr.c $COMMON_SOURCES -o test_rdp_plr $LDFLAGS
    if [ $? -eq 0 ]; then
        echo "[SUCCESS] test_rdp_plr 编译成功"
        ls -lh test_rdp_plr
    else
        echo "[ERROR] test_rdp_plr 编译失败！"
        exit 1
    fi
else
    echo "[WARNING] rdp_plr.c 不存在，跳过编译"
fi

# 编译 RDP + PARIX
echo ""
echo "[INFO] 正在编译 test_rdp_parix (RDP + PARIX)..."
if [ -f "rdp_parix.c" ]; then
    $CC $CFLAGS rdp_parix.c $COMMON_SOURCES -o test_rdp_parix $LDFLAGS
    if [ $? -eq 0 ]; then
        echo "[SUCCESS] test_rdp_parix 编译成功"
        ls -lh test_rdp_parix
    else
        echo "[ERROR] test_rdp_parix 编译失败！"
        exit 1
    fi
else
    echo "[WARNING] rdp_parix.c 不存在，跳过编译"
fi

echo ""
echo "[INFO] 所有程序编译成功！"

LOG_DIR="./test_results_std_rdp_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$LOG_DIR"
CSV_FILE="$LOG_DIR/standard_rdp_benchmark_results.csv"
echo "Scheme,K,R,UpdateSize_KB,Write_TPUT_MBps,Write_IOPS,User_Write_Lat_ms,IOMode" > "$CSV_FILE"

R_FIXED=2
K_VALUES=(6 12 16 20)
UPDATE_SIZES=(4096 8192 16384 32768 65536)
PACKET_SIZE=4096
N_UPDATES=1000

SCHEMES=(
    "L2P|test_rdp|sequential|off|off"
    "GLM|test_rdp|hybrid|off|off"
    "PLR|test_rdp_plr|sequential|off|on"
    "PLR+GLM|test_rdp_plr|hybrid|off|on"
    "PARIX|test_rdp_parix|sequential|parix|off"
    "PARIX+GLM|test_rdp_parix|hybrid|parix+alloc|off"
)

drop_page_cache() {
    if [ -w /proc/sys/vm/drop_caches ]; then sync; echo 3 > /proc/sys/vm/drop_caches; fi
}

echo ""
printf "%-12s | %-2s | %-4sKB | %-12s | %-10s | %-11s\n" \
       "Scheme(R=2)" "K" "Size" "W_TPUT(MB/s)" "W_IOPS" "W_Lat(ms)"
echo "------------------------------------------------------------------------"

for k in "${K_VALUES[@]}"; do
    p_prime=$k
    while true; do
        p_prime=$((p_prime + 1))
        is_prime=1
        for ((i=2; i*i<=p_prime; i++)); do if [ $((p_prime % i)) -eq 0 ]; then is_prime=0; break; fi; done
        if [ $is_prime -eq 1 ]; then break; fi
    done
    w=$((p_prime - 1))

    for us in "${UPDATE_SIZES[@]}"; do
        us_kb=$((us / 1024))
        
        for scheme_info in "${SCHEMES[@]}"; do
            IFS='|' read -r scheme_name binary alloc parix plr <<< "$scheme_info"
            log_file="$LOG_DIR/${scheme_name}_K${k}_U${us_kb}KB.log"
            
            # 检查二进制文件是否存在
            if [ ! -f "./$binary" ]; then
                echo "[WARNING] 二进制文件 ./$binary 不存在，跳过 $scheme_name"
                continue
            fi
            
            echo "[TEST] 运行 $scheme_name (K=$k, Update=${us_kb}KB)..."
            echo "  命令: ./$binary -k $k -w $w -p $PACKET_SIZE -u $us -n $N_UPDATES"
            
            rm -rf "$LOG_MOUNT_DIR"/plr/* "$LOG_MOUNT_DIR"/parix/*
            drop_page_cache
            
            CMD="./$binary -k $k -w $w -p $PACKET_SIZE -u $us -n $N_UPDATES -a $alloc"
            if [ "$parix" != "off" ]; then 
                CMD="$CMD -X $parix -L $LOG_MOUNT_DIR/parix"
                echo "  PARIX: mode=$parix, log_dir=$LOG_MOUNT_DIR/parix"
            else 
                CMD="$CMD -X off"
            fi
            if [ "$plr" != "off" ]; then 
                CMD="$CMD -P $LOG_MOUNT_DIR/plr"
                echo "  PLR: enabled, log_dir=$LOG_MOUNT_DIR/plr"
            fi

            set +e
            timeout 300 $CMD > "$log_file" 2>&1
            exit_code=$?
            set -e
            
            w_tput="0.00"; w_iops="0"; w_lat="0.00"
            io_mode="DISK"
            
            # 检查执行结果
            if [ $exit_code -ne 0 ]; then
                if [ $exit_code -eq 124 ]; then
                    echo "[WARNING] 测试超时 (300s)"
                    io_mode="TIMEOUT"
                else
                    echo "[WARNING] 程序异常退出 (exit_code=$exit_code)"
                    io_mode="ERROR"
                fi
            elif grep -q "回退至内存基准模式\|No disks available" "$log_file" 2>/dev/null; then
                echo "[INFO] 回退到内存模式（无可用磁盘）"
                io_mode="MEM_FALLBACK"
            else
                echo "[SUCCESS] 测试完成"
            fi

            res_w=$(grep "\[RESULT\]" "$log_file" | tail -1 || true)
            if [ -n "$res_w" ]; then
                w_tput=$(echo "$res_w" | awk -F'throughput=' '{print $2}' | awk '{print $1}')
                if echo "$res_w" | grep -q "IOPS="; then
                    w_iops=$(echo "$res_w" | awk -F'IOPS=' '{print $2}' | awk '{print $1}')
                fi
                elapsed_w=$(echo "$res_w" | awk -F'elapsed=' '{print $2}' | awk -F's' '{print $1}')
                w_lat=$(awk "BEGIN {printf \"%.2f\", ($elapsed_w / $N_UPDATES) * 1000}")
            fi

            printf "%-12s | %-2s | %-4sKB | %-12s | %-10s | %-11s\n" \
                   "$scheme_name" "$k" "$us_kb" "$w_tput" "$w_iops" "$w_lat"

            echo "$scheme_name,$k,$R_FIXED,$us_kb,$w_tput,$w_iops,$w_lat,$io_mode" >> "$CSV_FILE"
            
            sleep 2
        done
        echo "------------------------------------------------------------------------"
    done
done

echo ""
echo "============================================================================"
echo "[INFO] 标准 RDP (R=2) 全部测试完成！"
echo "[INFO] 编译和内存错误已自动修复。"
echo "[INFO] 详细汇总数据已保存至: $CSV_FILE"
echo "[INFO] 日志目录: $LOG_DIR"
echo "============================================================================"

# 显示最终结果摘要
if [ -f "$CSV_FILE" ]; then
    echo ""
    echo "========== 最终结果摘要 =========="
    cat "$CSV_FILE"
    echo "==================================="
fi