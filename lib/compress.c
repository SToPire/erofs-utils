// SPDX-License-Identifier: GPL-2.0+ OR Apache-2.0
/*
 * Copyright (C) 2018-2019 HUAWEI, Inc.
 *             http://www.huawei.com/
 * Created by Miao Xie <miaoxie@huawei.com>
 * with heavy changes by Gao Xiang <gaoxiang25@huawei.com>
 */
#ifndef _LARGEFILE64_SOURCE
#define _LARGEFILE64_SOURCE
#endif
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include "erofs/print.h"
#include "erofs/io.h"
#include "erofs/cache.h"
#include "erofs/compress.h"
#include "erofs/dedupe.h"
#include "compressor.h"
#include "erofs/block_list.h"
#include "erofs/compress_hints.h"
#include "erofs/fragments.h"

/* compressing configuration specified by users */
struct erofs_compress_cfg {
	struct erofs_compress handle;
	unsigned int algorithmtype;
	bool enable;
} erofs_ccfg[EROFS_MAX_COMPR_CFGS];

struct z_erofs_vle_compress_ctx {
	u8 queue[EROFS_CONFIG_COMPR_MAX_SZ * 2];
	struct list_head elist;		/* (lookahead) extent list */

	struct erofs_inode *inode;
	struct erofs_compress_cfg *ccfg;

	u8 *metacur;
	unsigned int head, tail;
	erofs_off_t remaining;
	unsigned int pclustersize;
	erofs_blk_t blkaddr;		/* pointing to the next blkaddr */
	u16 clusterofs;

	u32 tof_chksum;
	bool fix_dedupedfrag;
	bool fragemitted;
};

#define Z_EROFS_LEGACY_MAP_HEADER_SIZE	Z_EROFS_FULL_INDEX_ALIGN(0)

static void z_erofs_update_clusterofs(struct z_erofs_vle_compress_ctx *ctx)
{
	struct z_erofs_inmem_extent *e;
	unsigned int blksz = erofs_blksiz(ctx->inode->sbi);
	unsigned int offset;

	if (list_empty(&ctx->elist))
		return;

	e = list_last_entry(&ctx->elist, struct z_erofs_inmem_extent, list);
	if (e->length == 0)
		return;

	offset = e->length + ctx->clusterofs;
	ctx->clusterofs = (offset < blksz) ? 0 : offset % blksz;
}

static void z_erofs_write_index(struct z_erofs_vle_compress_ctx *ctx,
				struct z_erofs_inmem_extent *e)
{
	struct erofs_inode *inode = ctx->inode;
	struct erofs_sb_info *sbi = inode->sbi;
	unsigned int count = e->length;
	unsigned int d0 = 0, d1 = (ctx->clusterofs + count) / erofs_blksiz(sbi);
	struct z_erofs_lcluster_index di;
	unsigned int type, advise;

	if (!count)
		return;

	di.di_clusterofs = cpu_to_le16(ctx->clusterofs);

	/* whether the tail-end (un)compressed block or not */
	if (!d1) {
		/*
		 * A lcluster cannot have three parts with the middle one which
		 * is well-compressed for !ztailpacking cases.
		 */
		DBG_BUGON(!e->raw && !cfg.c_ztailpacking && !cfg.c_fragments);
		DBG_BUGON(e->partial);
		type = e->raw ? Z_EROFS_LCLUSTER_TYPE_PLAIN :
			Z_EROFS_LCLUSTER_TYPE_HEAD1;
		advise = type << Z_EROFS_LI_LCLUSTER_TYPE_BIT;
		di.di_advise = cpu_to_le16(advise);

		if (inode->datalayout == EROFS_INODE_COMPRESSED_FULL &&
			!e->compressedblks)
			di.di_u.blkaddr = cpu_to_le32(inode->fragmentoff >> 32);
		else
			di.di_u.blkaddr = cpu_to_le32(e->blkaddr);
		memcpy(ctx->metacur, &di, sizeof(di));
		ctx->metacur += sizeof(di);

		/* don't add the final index if the tail-end block exists */
		ctx->clusterofs = 0;
		return;
	}

	do {
		advise = 0;
		/* XXX: big pcluster feature should be per-inode */
		if (d0 == 1 && erofs_sb_has_big_pcluster(sbi)) {
			type = Z_EROFS_LCLUSTER_TYPE_NONHEAD;
			di.di_u.delta[0] = cpu_to_le16(e->compressedblks |
						       Z_EROFS_LI_D0_CBLKCNT);
			di.di_u.delta[1] = cpu_to_le16(d1);
		} else if (d0) {
			type = Z_EROFS_LCLUSTER_TYPE_NONHEAD;

			/*
			 * If the |Z_EROFS_VLE_DI_D0_CBLKCNT| bit is set, parser
			 * will interpret |delta[0]| as size of pcluster, rather
			 * than distance to last head cluster. Normally this
			 * isn't a problem, because uncompressed extent size are
			 * below Z_EROFS_VLE_DI_D0_CBLKCNT * BLOCK_SIZE = 8MB.
			 * But with large pcluster it's possible to go over this
			 * number, resulting in corrupted compressed indices.
			 * To solve this, we replace d0 with
			 * Z_EROFS_VLE_DI_D0_CBLKCNT-1.
			 */
			if (d0 >= Z_EROFS_LI_D0_CBLKCNT)
				di.di_u.delta[0] = cpu_to_le16(
						Z_EROFS_LI_D0_CBLKCNT - 1);
			else
				di.di_u.delta[0] = cpu_to_le16(d0);
			di.di_u.delta[1] = cpu_to_le16(d1);
		} else {
			type = e->raw ? Z_EROFS_LCLUSTER_TYPE_PLAIN :
				Z_EROFS_LCLUSTER_TYPE_HEAD1;

			if (inode->datalayout == EROFS_INODE_COMPRESSED_FULL &&
				!e->compressedblks)
				di.di_u.blkaddr = cpu_to_le32(inode->fragmentoff >> 32);
			else
				di.di_u.blkaddr = cpu_to_le32(e->blkaddr);

			if (e->partial) {
				DBG_BUGON(e->raw);
				advise |= Z_EROFS_LI_PARTIAL_REF;
			}
		}
		advise |= type << Z_EROFS_LI_LCLUSTER_TYPE_BIT;
		di.di_advise = cpu_to_le16(advise);

		memcpy(ctx->metacur, &di, sizeof(di));
		ctx->metacur += sizeof(di);

		count -= erofs_blksiz(sbi) - ctx->clusterofs;
		ctx->clusterofs = 0;

		++d0;
		--d1;
	} while (ctx->clusterofs + count >= erofs_blksiz(sbi));

	ctx->clusterofs = count;
}

static void z_erofs_write_indexes(struct z_erofs_vle_compress_ctx *ctx)
{
	struct z_erofs_inmem_extent *e, *n;
	struct z_erofs_lcluster_index di;

	if (list_empty(&ctx->elist))
		return;

	ctx->clusterofs = 0;

	list_for_each_entry_safe(e, n, &ctx->elist, list) {
		/*
		 * Uncompressed lcluster is aligned to lcluster boundary if
		 * 0padding is disabled. See write_uncompressed_extent().
		 */
		if (!erofs_sb_has_lz4_0padding(ctx->inode->sbi) && e->raw &&
		    ctx->clusterofs)
			ctx->clusterofs = 0;

		z_erofs_write_index(ctx, e);

		list_del(&e->list);
		free(e);
	}

	if (ctx->clusterofs) {
		di.di_clusterofs = cpu_to_le16(ctx->clusterofs);
		di.di_u.blkaddr = 0;
		di.di_advise = cpu_to_le16(Z_EROFS_LCLUSTER_TYPE_PLAIN
					   << Z_EROFS_LI_LCLUSTER_TYPE_BIT);
		memcpy(ctx->metacur, &di, sizeof(di));
		ctx->metacur += sizeof(di);
	}
}

static int z_erofs_compress_dedupe(struct z_erofs_vle_compress_ctx *ctx,
				   unsigned int *len)
{
	struct erofs_inode *inode = ctx->inode;
	const unsigned int lclustermask = (1 << inode->z_logical_clusterbits) - 1;
	struct erofs_sb_info *sbi = inode->sbi;
	struct z_erofs_inmem_extent *e, *newe;
	int elen = 0;
	int ret = 0;

	if (list_empty(&ctx->elist)) {
		e = NULL;
		elen = 0;
	} else {
		e = list_last_entry(&ctx->elist, struct z_erofs_inmem_extent,
				    list);
		elen = e->length;
	}

	/*
	 * No need dedupe for packed inode since it is composed of
	 * fragments which have already been deduplicated.
	 */
	if (erofs_is_packed_inode(inode))
		goto out;

	do {
		struct z_erofs_dedupe_ctx dctx = {
			.start = ctx->queue + ctx->head - ({ int rc;
				if (elen <= erofs_blksiz(sbi))
					rc = 0;
				else if (elen - erofs_blksiz(sbi) >= ctx->head)
					rc = ctx->head;
				else
					rc = elen - erofs_blksiz(sbi);
				rc; }),
			.end = ctx->queue + ctx->head + *len,
			.cur = ctx->queue + ctx->head,
		};
		int delta;

		if (z_erofs_dedupe_match(&dctx))
			break;

		delta = ctx->queue + ctx->head - dctx.cur;
		/*
		 * For big pcluster dedupe, leave two indices at least to store
		 * CBLKCNT as the first step.  Even laterly, an one-block
		 * decompresssion could be done as another try in practice.
		 */
		if (dctx.e.compressedblks > 1 &&
		    ((ctx->clusterofs + elen - delta) & lclustermask) +
			dctx.e.length < 2 * (lclustermask + 1))
			break;

		if (delta) {
			DBG_BUGON(delta < 0);
			DBG_BUGON(!e);

			/*
			 * For big pcluster dedupe, if we decide to shorten the
			 * previous big pcluster, make sure that the previous
			 * CBLKCNT is still kept.
			 */
			if (e->compressedblks > 1 &&
			    (ctx->clusterofs & lclustermask) + e->length
				- delta < 2 * (lclustermask + 1))
				break;
			e->partial = true;
			e->length -= delta;
		}

		/* fall back to noncompact indexes for deduplication */
		inode->z_advise &= ~Z_EROFS_ADVISE_COMPACTED_2B;
		inode->datalayout = EROFS_INODE_COMPRESSED_FULL;
		erofs_sb_set_dedupe(sbi);

		sbi->saved_by_deduplication +=
			dctx.e.compressedblks * erofs_blksiz(sbi);
		erofs_dbg("Dedupe %u %scompressed data (delta %d) to %u of %u blocks",
			  dctx.e.length, dctx.e.raw ? "un" : "",
			  delta, dctx.e.blkaddr, dctx.e.compressedblks);
		z_erofs_update_clusterofs(ctx);

		newe = malloc(sizeof(*newe));
		if (!newe) {
			ret = -ENOMEM;
			goto out;
		}
		memcpy(newe, &dctx.e, sizeof(*newe));
		list_add_tail(&newe->list, &ctx->elist);

		ctx->head += dctx.e.length - delta;
		DBG_BUGON(*len < dctx.e.length - delta);
		*len -= dctx.e.length - delta;

		if (ctx->head >= EROFS_CONFIG_COMPR_MAX_SZ) {
			const unsigned int qh_aligned =
				round_down(ctx->head, erofs_blksiz(sbi));
			const unsigned int qh_after = ctx->head - qh_aligned;

			memmove(ctx->queue, ctx->queue + qh_aligned,
				*len + qh_after);
			ctx->head = qh_after;
			ctx->tail = qh_after + *len;
			ret = -EAGAIN;
			break;
		}
	} while (*len);

out:
	z_erofs_update_clusterofs(ctx);
	return ret;
}

static int write_uncompressed_extent(struct z_erofs_vle_compress_ctx *ctx,
				     unsigned int *len, char *dst)
{
	int ret;
	struct erofs_sb_info *sbi = ctx->inode->sbi;
	unsigned int count, interlaced_offset, rightpart;

	/* reset clusterofs to 0 if permitted */
	if (!erofs_sb_has_lz4_0padding(sbi) && ctx->clusterofs &&
	    ctx->head >= ctx->clusterofs) {
		ctx->head -= ctx->clusterofs;
		*len += ctx->clusterofs;
		ctx->clusterofs = 0;
	}

	count = min(erofs_blksiz(sbi), *len);

	/* write interlaced uncompressed data if needed */
	if (ctx->inode->z_advise & Z_EROFS_ADVISE_INTERLACED_PCLUSTER)
		interlaced_offset = ctx->clusterofs;
	else
		interlaced_offset = 0;
	rightpart = min(erofs_blksiz(sbi) - interlaced_offset, count);

	memset(dst, 0, erofs_blksiz(sbi));

	memcpy(dst + interlaced_offset, ctx->queue + ctx->head, rightpart);
	memcpy(dst, ctx->queue + ctx->head + rightpart, count - rightpart);

	erofs_dbg("Writing %u uncompressed data to block %u",
		  count, ctx->blkaddr);
	ret = blk_write(sbi, dst, ctx->blkaddr, 1);
	if (ret)
		return ret;
	return count;
}

static unsigned int z_erofs_get_max_pclustersize(struct erofs_inode *inode)
{
	unsigned int pclusterblks;

	if (erofs_is_packed_inode(inode))
		pclusterblks = cfg.c_pclusterblks_packed;
#ifndef NDEBUG
	else if (cfg.c_random_pclusterblks)
		pclusterblks = 1 + rand() % cfg.c_pclusterblks_max;
#endif
	else if (cfg.c_compress_hints_file) {
		z_erofs_apply_compress_hints(inode);
		DBG_BUGON(!inode->z_physical_clusterblks);
		pclusterblks = inode->z_physical_clusterblks;
	} else {
		pclusterblks = cfg.c_pclusterblks_def;
	}
	return pclusterblks * erofs_blksiz(inode->sbi);
}

static int z_erofs_fill_inline_data(struct erofs_inode *inode, void *data,
				    unsigned int len, bool raw)
{
	inode->z_advise |= Z_EROFS_ADVISE_INLINE_PCLUSTER;
	inode->idata_size = len;
	inode->compressed_idata = !raw;

	inode->idata = malloc(inode->idata_size);
	if (!inode->idata)
		return -ENOMEM;
	erofs_dbg("Recording %u %scompressed inline data",
		  inode->idata_size, raw ? "un" : "");
	memcpy(inode->idata, data, inode->idata_size);
	return len;
}

static void tryrecompress_trailing(struct z_erofs_vle_compress_ctx *ctx,
				   struct erofs_compress *ec,
				   void *in, unsigned int *insize,
				   void *out, unsigned int *compressedsize)
{
	struct erofs_sb_info *sbi = ctx->inode->sbi;
	static char tmp[Z_EROFS_PCLUSTER_MAX_SIZE];
	unsigned int count;
	int ret = *compressedsize;

	/* no need to recompress */
	if (!(ret & (erofs_blksiz(sbi) - 1)))
		return;

	count = *insize;
	ret = erofs_compress_destsize(ec, in, &count, (void *)tmp,
				      rounddown(ret, erofs_blksiz(sbi)));
	if (ret <= 0 || ret + (*insize - count) >=
			roundup(*compressedsize, erofs_blksiz(sbi)))
		return;

	/* replace the original compressed data if any gain */
	memcpy(out, tmp, ret);
	*insize = count;
	*compressedsize = ret;
}

static bool z_erofs_fixup_deduped_fragment(struct z_erofs_vle_compress_ctx *ctx,
					   unsigned int len)
{
	struct erofs_inode *inode = ctx->inode;
	struct erofs_sb_info *sbi = inode->sbi;
	const unsigned int newsize = ctx->remaining + len;

	DBG_BUGON(!inode->fragment_size);

	/* try to fix again if it gets larger (should be rare) */
	if (inode->fragment_size < newsize) {
		ctx->pclustersize = min_t(erofs_off_t, z_erofs_get_max_pclustersize(inode),
					  roundup(newsize - inode->fragment_size,
						  erofs_blksiz(sbi)));
		return false;
	}

	inode->fragmentoff += inode->fragment_size - newsize;
	inode->fragment_size = newsize;

	erofs_dbg("Reducing fragment size to %u at %llu",
		  inode->fragment_size, inode->fragmentoff | 0ULL);

	/* it's the end */
	DBG_BUGON(ctx->tail - ctx->head + ctx->remaining != newsize);
	ctx->head = ctx->tail;
	ctx->remaining = 0;
	return true;
}

static int vle_compress_one(struct z_erofs_vle_compress_ctx *ctx)
{
	static char dstbuf[EROFS_CONFIG_COMPR_MAX_SZ + EROFS_MAX_BLOCK_SIZE];
	struct erofs_inode *inode = ctx->inode;
	struct erofs_sb_info *sbi = inode->sbi;
	unsigned int blksz = erofs_blksiz(sbi);
	char *const dst = dstbuf + blksz;
	struct erofs_compress *const h = &ctx->ccfg->handle;
	struct z_erofs_inmem_extent *e = NULL;
	unsigned int len = ctx->tail - ctx->head;
	bool is_packed_inode = erofs_is_packed_inode(inode);
	bool final = !ctx->remaining;
	int ret = 0;

	while (len) {
		bool may_packing = (cfg.c_fragments && final &&
				   !is_packed_inode);
		bool may_inline = (cfg.c_ztailpacking && final &&
				  !may_packing);
		bool fix_dedupedfrag = ctx->fix_dedupedfrag;
		unsigned int compressedsize;

		if (z_erofs_compress_dedupe(ctx, &len) && !final)
			break;

		e = malloc(sizeof(*e));
		if (!e)
			return -ENOMEM;

		if (len <= ctx->pclustersize) {
			if (!final || !len)
				goto free_extent;
			if (may_packing) {
				if (inode->fragment_size && !fix_dedupedfrag) {
					ctx->pclustersize = roundup(len, blksz);
					goto fix_dedupedfrag;
				}
				e->length = len;
				goto frag_packing;
			}
			if (!may_inline && len <= blksz)
				goto nocompression;
		}

		e->length = min(len,
				cfg.c_max_decompressed_extent_bytes);

		ret = erofs_compress_destsize(h, ctx->queue + ctx->head,
				&e->length, dst, ctx->pclustersize);
		if (ret <= 0) {
			erofs_err("failed to compress %s: %s", inode->i_srcpath,
				  erofs_strerror(ret));
			goto free_extent;
		}

		compressedsize = ret;
		/* even compressed size is smaller, there is no real gain */
		if (!(may_inline && e->length == len && ret < blksz))
			ret = roundup(ret, blksz);

		/* check if there is enough gain to keep the compressed data */
		if (ret * h->compress_threshold / 100 >= e->length) {
			if (may_inline && len < blksz) {
				ret = z_erofs_fill_inline_data(inode,
						ctx->queue + ctx->head,
						len, true);
			} else {
				may_inline = false;
				may_packing = false;
nocompression:
				ret = write_uncompressed_extent(ctx, &len, dst);
			}

			if (ret < 0)
				goto free_extent;
			e->length = ret;

			/*
			 * XXX: For now, we have to leave `ctx->compressedblks
			 * = 1' since there is no way to generate compressed
			 * indexes after the time that ztailpacking is decided.
			 */
			e->compressedblks = 1;
			e->raw = true;
		} else if (may_packing && len == e->length &&
			   compressedsize < ctx->pclustersize &&
			   (!inode->fragment_size || fix_dedupedfrag)) {
frag_packing:
			ret = z_erofs_pack_fragments(inode,
						     ctx->queue + ctx->head,
						     len, ctx->tof_chksum);
			if (ret < 0)
				goto free_extent;
			e->compressedblks = 0; /* indicate a fragment */
			e->raw = false;
			ctx->fragemitted = true;
			fix_dedupedfrag = false;
		/* tailpcluster should be less than 1 block */
		} else if (may_inline && len == e->length &&
			   compressedsize < blksz) {
			if (ctx->clusterofs + len <= blksz) {
				inode->eof_tailraw = malloc(len);
				if (!inode->eof_tailraw) {
					ret = -ENOMEM;
					goto free_extent;
				}

				memcpy(inode->eof_tailraw,
				       ctx->queue + ctx->head, len);
				inode->eof_tailrawsize = len;
			}

			ret = z_erofs_fill_inline_data(inode, dst,
					compressedsize, false);
			if (ret < 0)
				goto free_extent;
			e->compressedblks = 1;
			e->raw = false;
		} else {
			unsigned int tailused, padding;

			/*
			 * If there's space left for the last round when
			 * deduping fragments, try to read the fragment and
			 * recompress a little more to check whether it can be
			 * filled up. Fix up the fragment if succeeds.
			 * Otherwise, just drop it and go to packing.
			 */
			if (may_packing && len == e->length &&
			    (compressedsize & (blksz - 1)) &&
			    ctx->tail < sizeof(ctx->queue)) {
				ctx->pclustersize =
					roundup(compressedsize, blksz);
				goto fix_dedupedfrag;
			}

			if (may_inline && len == e->length)
				tryrecompress_trailing(ctx, h,
						ctx->queue + ctx->head,
						&e->length, dst,
						&compressedsize);

			e->compressedblks = BLK_ROUND_UP(sbi, compressedsize);
			DBG_BUGON(e->compressedblks * blksz >= e->length);

			padding = 0;
			tailused = compressedsize & (blksz - 1);
			if (tailused)
				padding = blksz - tailused;

			/* zero out garbage trailing data for non-0padding */
			if (!erofs_sb_has_lz4_0padding(sbi)) {
				memset(dst + compressedsize, 0, padding);
				padding = 0;
			}

			/* write compressed data */
			erofs_dbg("Writing %u compressed data to %u of %u blocks",
				  e->length, ctx->blkaddr,
				  e->compressedblks);

			ret = blk_write(sbi, dst - padding, ctx->blkaddr,
					e->compressedblks);
			if (ret)
				goto free_extent;
			e->raw = false;
			may_inline = false;
			may_packing = false;
		}
		e->partial = false;
		e->blkaddr = ctx->blkaddr;
		if (!may_inline && !may_packing && !is_packed_inode)
			(void)z_erofs_dedupe_insert(e, ctx->queue + ctx->head);
		ctx->blkaddr += e->compressedblks;
		ctx->head += e->length;
		len -= e->length;

		list_add_tail(&e->list, &ctx->elist);

		if (fix_dedupedfrag &&
		    z_erofs_fixup_deduped_fragment(ctx, len))
			break;

		if (!final && ctx->head >= EROFS_CONFIG_COMPR_MAX_SZ) {
			const unsigned int qh_aligned =
				round_down(ctx->head, blksz);
			const unsigned int qh_after = ctx->head - qh_aligned;

			memmove(ctx->queue, ctx->queue + qh_aligned,
				len + qh_after);
			ctx->head = qh_after;
			ctx->tail = qh_after + len;
			break;
		}
	}
	return 0;

fix_dedupedfrag:
	DBG_BUGON(!inode->fragment_size);
	ctx->remaining += inode->fragment_size;
	ctx->fix_dedupedfrag = true;
	e->length = 0;
	list_add_tail(&e->list, &ctx->elist);
	return 0;

free_extent:
	free(e);
	return ret;
}

struct z_erofs_compressindex_vec {
	union {
		erofs_blk_t blkaddr;
		u16 delta[2];
	} u;
	u16 clusterofs;
	u8  clustertype;
};

static void *parse_legacy_indexes(struct z_erofs_compressindex_vec *cv,
				  unsigned int nr, void *metacur)
{
	struct z_erofs_lcluster_index *const db = metacur;
	unsigned int i;

	for (i = 0; i < nr; ++i, ++cv) {
		struct z_erofs_lcluster_index *const di = db + i;
		const unsigned int advise = le16_to_cpu(di->di_advise);

		cv->clustertype = (advise >> Z_EROFS_LI_LCLUSTER_TYPE_BIT) &
			((1 << Z_EROFS_LI_LCLUSTER_TYPE_BITS) - 1);
		cv->clusterofs = le16_to_cpu(di->di_clusterofs);

		if (cv->clustertype == Z_EROFS_LCLUSTER_TYPE_NONHEAD) {
			cv->u.delta[0] = le16_to_cpu(di->di_u.delta[0]);
			cv->u.delta[1] = le16_to_cpu(di->di_u.delta[1]);
		} else {
			cv->u.blkaddr = le32_to_cpu(di->di_u.blkaddr);
		}
	}
	return db + nr;
}

static void *write_compacted_indexes(u8 *out,
				     struct z_erofs_compressindex_vec *cv,
				     erofs_blk_t *blkaddr_ret,
				     unsigned int destsize,
				     unsigned int lclusterbits,
				     bool final, bool *dummy_head,
				     bool update_blkaddr)
{
	unsigned int vcnt, lobits, encodebits, pos, i, cblks;
	erofs_blk_t blkaddr;

	if (destsize == 4)
		vcnt = 2;
	else if (destsize == 2 && lclusterbits <= 12)
		vcnt = 16;
	else
		return ERR_PTR(-EINVAL);
	lobits = max(lclusterbits, ilog2(Z_EROFS_LI_D0_CBLKCNT) + 1U);
	encodebits = (vcnt * destsize * 8 - 32) / vcnt;
	blkaddr = *blkaddr_ret;

	pos = 0;
	for (i = 0; i < vcnt; ++i) {
		unsigned int offset, v;
		u8 ch, rem;

		if (cv[i].clustertype == Z_EROFS_LCLUSTER_TYPE_NONHEAD) {
			if (cv[i].u.delta[0] & Z_EROFS_LI_D0_CBLKCNT) {
				cblks = cv[i].u.delta[0] & ~Z_EROFS_LI_D0_CBLKCNT;
				offset = cv[i].u.delta[0];
				blkaddr += cblks;
				*dummy_head = false;
			} else if (i + 1 == vcnt) {
				offset = min_t(u16, cv[i].u.delta[1],
						(1 << lobits) - 1);
			} else {
				offset = cv[i].u.delta[0];
			}
		} else {
			offset = cv[i].clusterofs;
			if (*dummy_head) {
				++blkaddr;
				if (update_blkaddr)
					*blkaddr_ret = blkaddr;
			}
			*dummy_head = true;
			update_blkaddr = false;

			if (cv[i].u.blkaddr != blkaddr) {
				if (i + 1 != vcnt)
					DBG_BUGON(!final);
				DBG_BUGON(cv[i].u.blkaddr);
			}
		}
		v = (cv[i].clustertype << lobits) | offset;
		rem = pos & 7;
		ch = out[pos / 8] & ((1 << rem) - 1);
		out[pos / 8] = (v << rem) | ch;
		out[pos / 8 + 1] = v >> (8 - rem);
		out[pos / 8 + 2] = v >> (16 - rem);
		pos += encodebits;
	}
	DBG_BUGON(destsize * vcnt * 8 != pos + 32);
	*(__le32 *)(out + destsize * vcnt - 4) = cpu_to_le32(*blkaddr_ret);
	*blkaddr_ret = blkaddr;
	return out + destsize * vcnt;
}

int z_erofs_convert_to_compacted_format(struct erofs_inode *inode,
					erofs_blk_t blkaddr,
					unsigned int legacymetasize,
					void *compressmeta)
{
	const unsigned int mpos = roundup(inode->inode_isize +
					  inode->xattr_isize, 8) +
				  sizeof(struct z_erofs_map_header);
	const unsigned int totalidx = (legacymetasize -
			Z_EROFS_LEGACY_MAP_HEADER_SIZE) /
				sizeof(struct z_erofs_lcluster_index);
	const unsigned int logical_clusterbits = inode->z_logical_clusterbits;
	u8 *out, *in;
	struct z_erofs_compressindex_vec cv[16];
	struct erofs_sb_info *sbi = inode->sbi;
	/* # of 8-byte units so that it can be aligned with 32 bytes */
	unsigned int compacted_4b_initial, compacted_4b_end;
	unsigned int compacted_2b;
	bool dummy_head;
	bool big_pcluster = erofs_sb_has_big_pcluster(sbi);

	if (logical_clusterbits < sbi->blkszbits)
		return -EINVAL;
	if (logical_clusterbits > 14) {
		erofs_err("compact format is unsupported for lcluster size %u",
			  1 << logical_clusterbits);
		return -EOPNOTSUPP;
	}

	if (inode->z_advise & Z_EROFS_ADVISE_COMPACTED_2B) {
		if (logical_clusterbits > 12) {
			erofs_err("compact 2B is unsupported for lcluster size %u",
				  1 << logical_clusterbits);
			return -EINVAL;
		}

		compacted_4b_initial = (32 - mpos % 32) / 4;
		if (compacted_4b_initial == 32 / 4)
			compacted_4b_initial = 0;

		if (compacted_4b_initial > totalidx) {
			compacted_4b_initial = compacted_2b = 0;
			compacted_4b_end = totalidx;
		} else {
			compacted_2b = rounddown(totalidx -
						 compacted_4b_initial, 16);
			compacted_4b_end = totalidx - compacted_4b_initial -
					   compacted_2b;
		}
	} else {
		compacted_2b = compacted_4b_initial = 0;
		compacted_4b_end = totalidx;
	}

	out = in = compressmeta;

	out += sizeof(struct z_erofs_map_header);
	in += Z_EROFS_LEGACY_MAP_HEADER_SIZE;

	dummy_head = false;
	/* prior to bigpcluster, blkaddr was bumped up once coming into HEAD */
	if (!big_pcluster) {
		--blkaddr;
		dummy_head = true;
	}

	/* generate compacted_4b_initial */
	while (compacted_4b_initial) {
		in = parse_legacy_indexes(cv, 2, in);
		out = write_compacted_indexes(out, cv, &blkaddr,
					      4, logical_clusterbits, false,
					      &dummy_head, big_pcluster);
		compacted_4b_initial -= 2;
	}
	DBG_BUGON(compacted_4b_initial);

	/* generate compacted_2b */
	while (compacted_2b) {
		in = parse_legacy_indexes(cv, 16, in);
		out = write_compacted_indexes(out, cv, &blkaddr,
					      2, logical_clusterbits, false,
					      &dummy_head, big_pcluster);
		compacted_2b -= 16;
	}
	DBG_BUGON(compacted_2b);

	/* generate compacted_4b_end */
	while (compacted_4b_end > 1) {
		in = parse_legacy_indexes(cv, 2, in);
		out = write_compacted_indexes(out, cv, &blkaddr,
					      4, logical_clusterbits, false,
					      &dummy_head, big_pcluster);
		compacted_4b_end -= 2;
	}

	/* generate final compacted_4b_end if needed */
	if (compacted_4b_end) {
		memset(cv, 0, sizeof(cv));
		in = parse_legacy_indexes(cv, 1, in);
		out = write_compacted_indexes(out, cv, &blkaddr,
					      4, logical_clusterbits, true,
					      &dummy_head, big_pcluster);
	}
	inode->extent_isize = out - (u8 *)compressmeta;
	return 0;
}

static void z_erofs_write_mapheader(struct erofs_inode *inode,
				    void *compressmeta)
{
	struct erofs_sb_info *sbi = inode->sbi;
	struct z_erofs_map_header h = {
		.h_advise = cpu_to_le16(inode->z_advise),
		.h_algorithmtype = inode->z_algorithmtype[1] << 4 |
				   inode->z_algorithmtype[0],
		/* lclustersize */
		.h_clusterbits = inode->z_logical_clusterbits - sbi->blkszbits,
	};

	if (inode->z_advise & Z_EROFS_ADVISE_FRAGMENT_PCLUSTER)
		h.h_fragmentoff = cpu_to_le32(inode->fragmentoff);
	else
		h.h_idata_size = cpu_to_le16(inode->idata_size);

	memset(compressmeta, 0, Z_EROFS_LEGACY_MAP_HEADER_SIZE);
	/* write out map header */
	memcpy(compressmeta, &h, sizeof(struct z_erofs_map_header));
}

void z_erofs_drop_inline_pcluster(struct erofs_inode *inode)
{
	struct erofs_sb_info *sbi = inode->sbi;
	const unsigned int type = Z_EROFS_LCLUSTER_TYPE_PLAIN;
	struct z_erofs_map_header *h = inode->compressmeta;

	h->h_advise = cpu_to_le16(le16_to_cpu(h->h_advise) &
				  ~Z_EROFS_ADVISE_INLINE_PCLUSTER);
	h->h_idata_size = 0;
	if (!inode->eof_tailraw)
		return;
	DBG_BUGON(inode->compressed_idata != true);

	/* patch the EOF lcluster to uncompressed type first */
	if (inode->datalayout == EROFS_INODE_COMPRESSED_FULL) {
		struct z_erofs_lcluster_index *di =
			(inode->compressmeta + inode->extent_isize) -
			sizeof(struct z_erofs_lcluster_index);
		__le16 advise =
			cpu_to_le16(type << Z_EROFS_LI_LCLUSTER_TYPE_BIT);

		di->di_advise = advise;
	} else if (inode->datalayout == EROFS_INODE_COMPRESSED_COMPACT) {
		/* handle the last compacted 4B pack */
		unsigned int eofs, base, pos, v, lo;
		u8 *out;

		eofs = inode->extent_isize -
			(4 << (BLK_ROUND_UP(sbi, inode->i_size) & 1));
		base = round_down(eofs, 8);
		pos = 16 /* encodebits */ * ((eofs - base) / 4);
		out = inode->compressmeta + base;
		lo = erofs_blkoff(sbi, get_unaligned_le32(out + pos / 8));
		v = (type << sbi->blkszbits) | lo;
		out[pos / 8] = v & 0xff;
		out[pos / 8 + 1] = v >> 8;
	} else {
		DBG_BUGON(1);
		return;
	}
	free(inode->idata);
	/* replace idata with prepared uncompressed data */
	inode->idata = inode->eof_tailraw;
	inode->idata_size = inode->eof_tailrawsize;
	inode->compressed_idata = false;
	inode->eof_tailraw = NULL;
}

int erofs_write_compressed_file(struct erofs_inode *inode, int fd)
{
	struct erofs_buffer_head *bh;
	static struct z_erofs_vle_compress_ctx ctx;
	erofs_blk_t blkaddr, compressed_blocks;
	unsigned int legacymetasize;
	int ret;
	struct erofs_sb_info *sbi = inode->sbi;
	u8 *compressmeta = malloc(BLK_ROUND_UP(sbi, inode->i_size) *
				  sizeof(struct z_erofs_lcluster_index) +
				  Z_EROFS_LEGACY_MAP_HEADER_SIZE);

	if (!compressmeta)
		return -ENOMEM;

	/* allocate main data buffer */
	bh = erofs_balloc(DATA, 0, 0, 0);
	if (IS_ERR(bh)) {
		ret = PTR_ERR(bh);
		goto err_free_meta;
	}

	/* initialize per-file compression setting */
	inode->z_advise = 0;
	inode->z_logical_clusterbits = sbi->blkszbits;
	if (!cfg.c_legacy_compress && inode->z_logical_clusterbits <= 14) {
		if (inode->z_logical_clusterbits <= 12)
			inode->z_advise |= Z_EROFS_ADVISE_COMPACTED_2B;
		inode->datalayout = EROFS_INODE_COMPRESSED_COMPACT;
	} else {
		inode->datalayout = EROFS_INODE_COMPRESSED_FULL;
	}

	if (erofs_sb_has_big_pcluster(sbi)) {
		inode->z_advise |= Z_EROFS_ADVISE_BIG_PCLUSTER_1;
		if (inode->datalayout == EROFS_INODE_COMPRESSED_COMPACT)
			inode->z_advise |= Z_EROFS_ADVISE_BIG_PCLUSTER_2;
	}
	if (cfg.c_fragments && !cfg.c_dedupe)
		inode->z_advise |= Z_EROFS_ADVISE_INTERLACED_PCLUSTER;

#ifndef NDEBUG
	if (cfg.c_random_algorithms) {
		while (1) {
			inode->z_algorithmtype[0] =
				rand() % EROFS_MAX_COMPR_CFGS;
			if (erofs_ccfg[inode->z_algorithmtype[0]].enable)
				break;
		}
	}
#endif
	ctx.ccfg = &erofs_ccfg[inode->z_algorithmtype[0]];
	inode->z_algorithmtype[0] = ctx.ccfg[0].algorithmtype;
	inode->z_algorithmtype[1] = 0;

	inode->idata_size = 0;
	inode->fragment_size = 0;

	/*
	 * Handle tails in advance to avoid writing duplicated
	 * parts into the packed inode.
	 */
	if (cfg.c_fragments && !erofs_is_packed_inode(inode)) {
		ret = z_erofs_fragments_dedupe(inode, fd, &ctx.tof_chksum);
		if (ret < 0)
			goto err_bdrop;
	}

	blkaddr = erofs_mapbh(bh->block);	/* start_blkaddr */
	ctx.inode = inode;
	ctx.pclustersize = z_erofs_get_max_pclustersize(inode);
	ctx.blkaddr = blkaddr;
	ctx.metacur = compressmeta + Z_EROFS_LEGACY_MAP_HEADER_SIZE;
	ctx.head = ctx.tail = 0;
	ctx.clusterofs = 0;
	ctx.remaining = inode->i_size - inode->fragment_size;
	ctx.fix_dedupedfrag = false;
	ctx.fragemitted = false;
	init_list_head(&ctx.elist);
	if (cfg.c_all_fragments && !erofs_is_packed_inode(inode) &&
	    !inode->fragment_size) {
		ret = z_erofs_pack_file_from_fd(inode, fd, ctx.tof_chksum);
		if (ret)
			goto err_free_idata;
	} else {
		while (ctx.remaining) {
			const u64 rx = min_t(u64, ctx.remaining,
					     sizeof(ctx.queue) - ctx.tail);

			ret = read(fd, ctx.queue + ctx.tail, rx);
			if (ret != rx) {
				ret = -errno;
				goto err_bdrop;
			}
			ctx.remaining -= rx;
			ctx.tail += rx;

			ret = vle_compress_one(&ctx);
			if (ret)
				goto err_free_idata;
		}
	}
	DBG_BUGON(ctx.head != ctx.tail);

	/* fall back to no compression mode */
	compressed_blocks = ctx.blkaddr - blkaddr;
	DBG_BUGON(compressed_blocks < !!inode->idata_size);
	compressed_blocks -= !!inode->idata_size;

	/* generate an extent for the deduplicated fragment */
	if (inode->fragment_size && !ctx.fragemitted) {
		struct z_erofs_inmem_extent *e = malloc(sizeof(*e));
		if (!e) {
			ret = -ENOMEM;
			goto err_free_idata;
		}
		z_erofs_update_clusterofs(&ctx);
		e->length = inode->fragment_size;
		e->compressedblks = 0;
		e->raw = false;
		e->partial = false;
		e->blkaddr = ctx.blkaddr;
		list_add_tail(&e->list, &ctx.elist);
	}
	z_erofs_fragments_commit(inode);

	z_erofs_write_indexes(&ctx);
	legacymetasize = ctx.metacur - compressmeta;
	/* estimate if data compression saves space or not */
	if (!inode->fragment_size &&
	    compressed_blocks * erofs_blksiz(sbi) + inode->idata_size +
	    legacymetasize >= inode->i_size) {
		z_erofs_dedupe_commit(true);
		ret = -ENOSPC;
		goto err_free_idata;
	}
	z_erofs_dedupe_commit(false);
	z_erofs_write_mapheader(inode, compressmeta);

	if (!ctx.fragemitted)
		sbi->saved_by_deduplication += inode->fragment_size;

	/* if the entire file is a fragment, a simplified form is used. */
	if (inode->i_size <= inode->fragment_size) {
		DBG_BUGON(inode->i_size < inode->fragment_size);
		DBG_BUGON(inode->fragmentoff >> 63);
		*(__le64 *)compressmeta =
			cpu_to_le64(inode->fragmentoff | 1ULL << 63);
		inode->datalayout = EROFS_INODE_COMPRESSED_FULL;
		legacymetasize = Z_EROFS_LEGACY_MAP_HEADER_SIZE;
	}

	if (compressed_blocks) {
		ret = erofs_bh_balloon(bh, erofs_pos(sbi, compressed_blocks));
		DBG_BUGON(ret != erofs_blksiz(sbi));
	} else {
		if (!cfg.c_fragments && !cfg.c_dedupe)
			DBG_BUGON(!inode->idata_size);
	}

	erofs_info("compressed %s (%llu bytes) into %u blocks",
		   inode->i_srcpath, (unsigned long long)inode->i_size,
		   compressed_blocks);

	if (inode->idata_size) {
		bh->op = &erofs_skip_write_bhops;
		inode->bh_data = bh;
	} else {
		erofs_bdrop(bh, false);
	}

	inode->u.i_blocks = compressed_blocks;

	if (inode->datalayout == EROFS_INODE_COMPRESSED_FULL) {
		inode->extent_isize = legacymetasize;
	} else {
		ret = z_erofs_convert_to_compacted_format(inode, blkaddr,
							  legacymetasize,
							  compressmeta);
		DBG_BUGON(ret);
	}
	inode->compressmeta = compressmeta;
	if (!erofs_is_packed_inode(inode))
		erofs_droid_blocklist_write(inode, blkaddr, compressed_blocks);
	return 0;

err_free_idata:
	if (inode->idata) {
		free(inode->idata);
		inode->idata = NULL;
	}
err_bdrop:
	erofs_bdrop(bh, true);	/* revoke buffer */
err_free_meta:
	free(compressmeta);
	inode->compressmeta = NULL;
	return ret;
}

static int z_erofs_build_compr_cfgs(struct erofs_sb_info *sbi,
				    struct erofs_buffer_head *sb_bh)
{
	struct erofs_buffer_head *bh = sb_bh;
	int ret = 0;

	if (sbi->available_compr_algs & (1 << Z_EROFS_COMPRESSION_LZ4)) {
		struct {
			__le16 size;
			struct z_erofs_lz4_cfgs lz4;
		} __packed lz4alg = {
			.size = cpu_to_le16(sizeof(struct z_erofs_lz4_cfgs)),
			.lz4 = {
				.max_distance =
					cpu_to_le16(sbi->lz4_max_distance),
				.max_pclusterblks = cfg.c_pclusterblks_max,
			}
		};

		bh = erofs_battach(bh, META, sizeof(lz4alg));
		if (IS_ERR(bh)) {
			DBG_BUGON(1);
			return PTR_ERR(bh);
		}
		erofs_mapbh(bh->block);
		ret = dev_write(sbi, &lz4alg, erofs_btell(bh, false),
				sizeof(lz4alg));
		bh->op = &erofs_drop_directly_bhops;
	}
#ifdef HAVE_LIBLZMA
	if (sbi->available_compr_algs & (1 << Z_EROFS_COMPRESSION_LZMA)) {
		struct {
			__le16 size;
			struct z_erofs_lzma_cfgs lzma;
		} __packed lzmaalg = {
			.size = cpu_to_le16(sizeof(struct z_erofs_lzma_cfgs)),
			.lzma = {
				.dict_size = cpu_to_le32(cfg.c_dict_size),
			}
		};

		bh = erofs_battach(bh, META, sizeof(lzmaalg));
		if (IS_ERR(bh)) {
			DBG_BUGON(1);
			return PTR_ERR(bh);
		}
		erofs_mapbh(bh->block);
		ret = dev_write(sbi, &lzmaalg, erofs_btell(bh, false),
				sizeof(lzmaalg));
		bh->op = &erofs_drop_directly_bhops;
	}
#endif
	if (sbi->available_compr_algs & (1 << Z_EROFS_COMPRESSION_DEFLATE)) {
		struct {
			__le16 size;
			struct z_erofs_deflate_cfgs z;
		} __packed zalg = {
			.size = cpu_to_le16(sizeof(struct z_erofs_deflate_cfgs)),
			.z = {
				.windowbits =
					cpu_to_le32(ilog2(cfg.c_dict_size)),
			}
		};

		bh = erofs_battach(bh, META, sizeof(zalg));
		if (IS_ERR(bh)) {
			DBG_BUGON(1);
			return PTR_ERR(bh);
		}
		erofs_mapbh(bh->block);
		ret = dev_write(sbi, &zalg, erofs_btell(bh, false),
				sizeof(zalg));
		bh->op = &erofs_drop_directly_bhops;
	}
	return ret;
}

int z_erofs_compress_init(struct erofs_sb_info *sbi, struct erofs_buffer_head *sb_bh)
{
	int i, ret;

	for (i = 0; cfg.c_compr_alg[i]; ++i) {
		struct erofs_compress *c = &erofs_ccfg[i].handle;

		ret = erofs_compressor_init(sbi, c, cfg.c_compr_alg[i]);
		if (ret)
			return ret;

		ret = erofs_compressor_setlevel(c, cfg.c_compr_level[i]);
		if (ret)
			return ret;

		erofs_ccfg[i].algorithmtype =
			z_erofs_get_compress_algorithm_id(c);
		erofs_ccfg[i].enable = true;
		sbi->available_compr_algs |= 1 << erofs_ccfg[i].algorithmtype;
		if (erofs_ccfg[i].algorithmtype != Z_EROFS_COMPRESSION_LZ4)
			erofs_sb_set_compr_cfgs(sbi);
	}

	/*
	 * if primary algorithm is empty (e.g. compression off),
	 * clear 0PADDING feature for old kernel compatibility.
	 */
	if (!cfg.c_compr_alg[0] ||
	    (cfg.c_legacy_compress && !strncmp(cfg.c_compr_alg[0], "lz4", 3)))
		erofs_sb_clear_lz4_0padding(sbi);

	if (!cfg.c_compr_alg[0])
		return 0;

	/*
	 * if big pcluster is enabled, an extra CBLKCNT lcluster index needs
	 * to be loaded in order to get those compressed block counts.
	 */
	if (cfg.c_pclusterblks_max > 1) {
		if (cfg.c_pclusterblks_max >
		    Z_EROFS_PCLUSTER_MAX_SIZE / erofs_blksiz(sbi)) {
			erofs_err("unsupported clusterblks %u (too large)",
				  cfg.c_pclusterblks_max);
			return -EINVAL;
		}
		erofs_sb_set_big_pcluster(sbi);
	}
	if (cfg.c_pclusterblks_packed > cfg.c_pclusterblks_max) {
		erofs_err("invalid physical cluster size for the packed file");
		return -EINVAL;
	}

	if (erofs_sb_has_compr_cfgs(sbi))
		return z_erofs_build_compr_cfgs(sbi, sb_bh);
	return 0;
}

int z_erofs_compress_exit(void)
{
	int i, ret;

	for (i = 0; cfg.c_compr_alg[i]; ++i) {
		ret = erofs_compressor_exit(&erofs_ccfg[i].handle);
		if (ret)
			return ret;
	}
	return 0;
}
