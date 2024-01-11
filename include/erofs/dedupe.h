/* SPDX-License-Identifier: GPL-2.0+ OR Apache-2.0 */
/*
 * Copyright (C) 2022 Alibaba Cloud
 */
#ifndef __EROFS_DEDUPE_H
#define __EROFS_DEDUPE_H

#ifdef __cplusplus
extern "C"
{
#endif

#include "internal.h"

#define EROFS_DEDUPE_GLOBAL_BUCKET_NUM (65536)
#define EROFS_DEDUPE_LOCAL_BUCKET_NUM (4096)

struct z_erofs_inmem_extent {
	erofs_blk_t blkaddr;
	unsigned int compressedblks;
	unsigned int length;
	bool raw, partial, dedupe;
	struct list_head list;
};

struct z_erofs_dedupe_ctx {
	u8		*start, *end;
	u8		*cur;
	struct z_erofs_inmem_extent	e;
};

struct z_erofs_dedupe_item {
	struct list_head list;
	struct z_erofs_dedupe_item *chain;
	long long	hash;
	u8		prefix_sha256[32];
	u64		prefix_xxh64;

	erofs_blk_t	compressed_blkaddr;
	unsigned int	compressed_blks;

	int		original_length;
	bool		partial, raw;
	u8		extra_data[];
};

int z_erofs_dedupe_match(struct list_head *local_hashmap,
			 struct z_erofs_dedupe_ctx *ctx);
int z_erofs_dedupe_insert(struct list_head *local_hashmap, int hashmap_size,
			  struct z_erofs_dedupe_item **local_list,
			  struct z_erofs_inmem_extent *e, void *original_data);
void z_erofs_dedupe_commit(struct z_erofs_dedupe_item **local_lists,
			   size_t size, bool drop);
int z_erofs_dedupe_init(unsigned int wsiz);
void z_erofs_dedupe_exit(void);

void z_erofs_dedupe_blkmap_add(erofs_blk_t vblkaddr, erofs_blk_t pblkaddr,
			       erofs_blk_t blks);
erofs_blk_t z_erofs_dedupe_blkmap_get(erofs_blk_t vblkaddr);
void z_erofs_dedupe_blkmap_free(void);
void z_erofs_dedupe_merge(void);
#ifdef __cplusplus
}
#endif

#endif
