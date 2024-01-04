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

struct z_erofs_inmem_extent {
	erofs_blk_t blkaddr;
	unsigned int compressedblks;
	unsigned int length;
	bool raw, partial;
	struct list_head list;
};

struct z_erofs_dedupe_ctx {
	u8		*start, *end;
	u8		*cur;
	struct z_erofs_inmem_extent	e;
};

int z_erofs_dedupe_match(struct list_head *hashmap, struct z_erofs_dedupe_ctx *ctx);
int z_erofs_dedupe_insert(struct list_head *hashmap,
			  struct z_erofs_inmem_extent *e, void *original_data);
void z_erofs_dedupe_commit(bool drop);
int z_erofs_dedupe_init(unsigned int wsiz);
void z_erofs_dedupe_exit(struct list_head *hashmap);

#ifdef __cplusplus
}
#endif

#endif
