// SPDX-License-Identifier: GPL-2.0+ OR Apache-2.0
/*
 * Copyright (C) 2021 Gao Xiang <xiang@kernel.org>
 */
#include <stdlib.h>
#include "config.h"
#ifdef HAVE_LIBLZMA
#include <lzma.h>
#include "erofs/config.h"
#include "erofs/print.h"
#include "erofs/internal.h"
#include "compressor.h"

struct erofs_liblzma_context {
	lzma_options_lzma opt;
	lzma_stream strm;
};

static int erofs_liblzma_compress_destsize(const struct erofs_compress *c,
					   const void *src, unsigned int *srcsize,
					   void *dst, unsigned int dstsize)
{
	struct erofs_liblzma_context *ctx = c->private_data;
	lzma_stream *strm = &ctx->strm;

	lzma_ret ret = lzma_microlzma_encoder(strm, &ctx->opt);
	if (ret != LZMA_OK)
		return -EFAULT;

	strm->next_in = src;
	strm->avail_in = *srcsize;
	strm->next_out = dst;
	strm->avail_out = dstsize;

	ret = lzma_code(strm, LZMA_FINISH);
	if (ret != LZMA_STREAM_END)
		return -EBADMSG;

	*srcsize = strm->total_in;
	return strm->total_out;
}

static int erofs_compressor_liblzma_exit(struct erofs_compress *c)
{
	struct erofs_liblzma_context *ctx = c->private_data;

	if (!ctx)
		return -EINVAL;

	lzma_end(&ctx->strm);
	free(ctx);
	return 0;
}

static int erofs_compressor_liblzma_setlevel(struct erofs_compress *c,
					     int compression_level)
{
	struct erofs_liblzma_context *ctx = c->private_data;
	u32 preset;

	if (compression_level < 0)
		preset = LZMA_PRESET_DEFAULT;
	else if (compression_level >= 100)
		preset = (compression_level - 100) | LZMA_PRESET_EXTREME;
	else
		preset = compression_level;

	if (lzma_lzma_preset(&ctx->opt, preset))
		return -EINVAL;

	/* XXX: temporary hack */
	if (cfg.c_dict_size) {
		if (cfg.c_dict_size > Z_EROFS_LZMA_MAX_DICT_SIZE) {
			erofs_err("dict size %u is too large", cfg.c_dict_size);
			return -EINVAL;
		}
		ctx->opt.dict_size = cfg.c_dict_size;
	} else {
		if (ctx->opt.dict_size > Z_EROFS_LZMA_MAX_DICT_SIZE)
			ctx->opt.dict_size = Z_EROFS_LZMA_MAX_DICT_SIZE;
		cfg.c_dict_size = ctx->opt.dict_size;
	}
	c->compression_level = compression_level;
	return 0;
}

static int erofs_compressor_liblzma_init(struct erofs_compress *c,
					 bool print_warning)
{
	struct erofs_liblzma_context *ctx;

	ctx = malloc(sizeof(*ctx));
	if (!ctx)
		return -ENOMEM;
	ctx->strm = (lzma_stream)LZMA_STREAM_INIT;
	c->private_data = ctx;
	if (print_warning) {
		erofs_warn("EXPERIMENTAL MicroLZMA feature in use. Use at your own risk!");
	}
	return 0;
}

const struct erofs_compressor erofs_compressor_lzma = {
	.default_level = LZMA_PRESET_DEFAULT,
	.best_level = 109,
	.init = erofs_compressor_liblzma_init,
	.exit = erofs_compressor_liblzma_exit,
	.setlevel = erofs_compressor_liblzma_setlevel,
	.compress_destsize = erofs_liblzma_compress_destsize,
};
#endif
