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
