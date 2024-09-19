#line 1 "af_alg.c"
#define _POSIX_C_SOURCE		200809L
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#if __has_include(<linux/if_alg.h>)
#	include <if_alg.h>
#else
struct sockaddr_alg {
	uint16_t	salg_family;
	uint8_t		salg_type[14];
	uint32_t	salg_feat;
	uint32_t	salg_mask;
	uint8_t		salg_name[];
};
struct af_alg_iv {
	uint32_t	ivlen;
	uint8_t		iv[0];
};
#	define ALG_SET_KEY				1
#	define ALG_SET_IV				2
#	define ALG_SET_OP				3
#	define ALG_SET_AEAD_ASSOCLEN	4
#	define ALG_SET_AEAD_AUTHSIZE	5
#	define ALG_SET_DRBG_ENTROPY		6
#	define ALG_OP_DECRYPT			0
#	define ALG_OP_ENCRYPT			1
#endif
#include <sys/socket.h>
#include <errno.h>

// Just for debugging
#include <stdio.h>
#include <inttypes.h>

#ifndef AF_ALG
#define AF_ALG 38
#endif
#ifndef SOL_ALG
#define SOL_ALG 279
#endif

static Tcl_HashTable	g_params;

#define CRYPTO_TYPES \
	X(_unknown) \
	X(aead) \
	X(hash) \
	X(akcipher) \
	X(cipher) \
	X(compression) \
	X(kpp) \
	X(rng) \
	X(scomp) \
	X(skcipher)
enum info_types {
	#define X(name) crypto_type_##name,
	CRYPTO_TYPES
	#undef X
	crypto_type__size
};
static const char*	info_type_strs[crypto_type__size] = {
	#define X(name) #name,
	CRYPTO_TYPES
	#undef X
};

struct af_alg_params {
	Tcl_Obj*		name;
	enum info_types	type;
	int				priority;
	size_t			digestsize;
	int				sockfd;
	int				fd;
};

static struct sockaddr_storage	_sa_hmac_sha256;
static struct sockaddr_alg*		g_hmac_sha256;		// Pre-prepare a hmac(sha256) sockaddr

static void free_params(struct af_alg_params** p) //<<<
{
	if (*p) {
		replace_tclobj(&(*p)->name, NULL);
		if ((*p)->sockfd) close((*p)->sockfd);
		if ((*p)->fd) close((*p)->fd);
		ckfree(*p);
		*p = NULL;
	}
}

//>>>
static int get_params(Tcl_Interp* interp, enum info_types type, Tcl_Obj* name, const struct af_alg_params** out) //<<<
{
	int			code = TCL_OK;
	Tcl_Obj*	key = NULL;

	replace_tclobj(&key, Tcl_ObjPrintf("%s/%s", info_type_strs[type], Tcl_GetString(name)));
	Tcl_HashEntry*	he = Tcl_FindHashEntry(&g_params, Tcl_GetString(key));
	if (!he) THROW_PRINTF_LABEL(finally, code, "No info for \"%s\"", Tcl_GetString(key));
	*out = Tcl_GetHashValue(he);
	if (!*out) THROW_ERROR_LABEL(finally, code, "Corrupt g_params: entry exists but is NULL");

finally:
	replace_tclobj(&key, NULL);
	return code;
}

//>>>
static int init_g_params(Tcl_Interp* interp) //<<<
{
	int					code = TCL_OK;
	Tcl_Obj*			cmd = NULL;
	Tcl_Obj*			crypto_info = NULL;
	Tcl_Obj*			tmp = NULL;
	struct af_alg_params*	section = NULL;

	Tcl_InitHashTable(&g_params, TCL_STRING_KEYS);

	replace_tclobj(&cmd, Tcl_NewStringObj("apply {{} {set h [open /proc/crypto]; try {read $h} finally {close $h}}}", -1));
	code = Tcl_EvalObjEx(interp, cmd, 0);
	if (code != TCL_OK) goto finally;
	replace_tclobj(&crypto_info, Tcl_GetObjResult(interp));
	Tcl_ResetResult(interp);
	const char*	str = Tcl_GetString(crypto_info);
	const char*	cur = str;
	const char* YYMARKER;

	// Section state
	section = ckalloc(sizeof *section);
	*section = (struct af_alg_params){0};

	for (;;) {
		const char	*v1, *v2, *t_aead, *t_ahash, *t_akcipher, *t_cipher, *t_compression, *t_kpp, *t_rng, *t_scomp, *t_shash, *t_skcipher, *t_unknown;
		/*!stags:re2c:main format = "const char* @@;"; */
		/*!local:re2c:main
			re2c:define:YYCTYPE		= char;
			re2c:define:YYCURSOR	= cur;
			re2c:yyfill:enable		= 0;
			re2c:tags				= 1;

			end		= [\x00];
			eol		= [\n];
			ws		= [ \t];
			valchar	= [^:] \ eol \ end;
			val		= valchar+;
			tokchar	= valchar \ [()];
			tok		= tokchar+;
			digit	= [0-9];
			integer	= digit+;
			sep		= ws* ":" ws*;

			typename
				= "aead"		@t_aead 
				| "ahash"		@t_ahash
				| "akcipher"	@t_akcipher
				| "cipher"		@t_cipher
				| "compression"	@t_compression
				| "kpp"			@t_kpp
				| "rng"			@t_rng
				| "scomp"		@t_scomp
				| "shash"		@t_shash
				| "skcipher"	@t_skcipher
				| tok			@t_unknown;

			end			{ break; }
			eol			{ goto section_end; }
			*			{ THROW_PRINTF_LABEL(finally, code, "Failed to parse /proc/crypto, offset: %"PRIdPTR": (%.*s...)", cur-str-1, 50, cur-1); }

			"name" sep @v1 tok @v2 eol	{
				replace_tclobj(&section->name, Tcl_NewStringObj(v1, (int)(v2-v1)));
				continue;
			}
			"name" sep tok "(" val ")" eol	{ goto ignore_section; }

			"type" sep typename eol		{
				if (t_aead)				section->type = crypto_type_aead;
				else if (t_ahash)		section->type = crypto_type_hash;
				else if (t_akcipher)	goto ignore_section;
				else if (t_cipher)		section->type = crypto_type_cipher;
				else if (t_compression)	section->type = crypto_type_compression;
				else if (t_kpp)			goto ignore_section;
				else if (t_rng)			section->type = crypto_type_rng;
				else if (t_scomp)		section->type = crypto_type_scomp;
				else if (t_shash)		section->type = crypto_type_hash;
				else if (t_skcipher)	section->type = crypto_type_skcipher;
				else if (t_unknown)		goto ignore_section;
				continue;
			}

			"priority" sep @v1 integer @v2 eol	{
				char*	e = NULL;
				section->priority = strtol(v1, &e, 10);
				if (e != v2) THROW_PRINTF_LABEL(finally, code, "Could not parse priority \"%.*s\"", (int)(v2-v1), v1);
				continue;
			}

			"selftest" sep "passed" eol	{ continue; }
			"selftest" sep tok eol		{ goto ignore_section; }
			"internal" sep "yes" eol	{ goto ignore_section; }

			"digestsize" sep @v1 integer @v2 eol	{
				char*	e = NULL;
				section->digestsize = strtol(v1, &e, 10);
				if (e != v2) THROW_PRINTF_LABEL(finally, code, "Could not parse digestsize \"%.*s\"", (int)(v2-v1), v1);
				continue;
			}

			@v1 tok sep tok @v2 eol	{ continue; }
		*/

	section_end:
		{
			//fprintf(stderr, "section_end\n");
			if (!section->name) THROW_ERROR_LABEL(finally, code, "No name found in section");
			replace_tclobj(&tmp, Tcl_ObjPrintf("%s/%s", info_type_strs[section->type], Tcl_GetString(section->name)));
			int isnew;
			Tcl_HashEntry*	he = Tcl_CreateHashEntry(&g_params, Tcl_GetString(tmp), &isnew);
			//fprintf(stderr, "Checked for existing (%s): %d\n", Tcl_GetString(tmp), isnew);
			if (!isnew) {
				struct af_alg_params*	existing = Tcl_GetHashValue(he);
				if (section->priority <= existing->priority) {
					//fprintf(stderr, "Existing priority: %d is higher than this: %d\n", existing->priority, section->priority);
					free_params(&section);
					goto reset_section_state;
				}
				free_params(&existing);
			}
			#if 0
			fprintf(stderr, "Parsed section:\n%20s: %s\n%20s: %s\n%20s: %d\n%20s: %d\n",
				"name",			Tcl_GetString(section->name),
				"type",			info_type_strs[section->type],
				"priority",		section->priority,
				"digestsize",	section->digestsize
			);
			#endif
			Tcl_SetHashValue(he, section);
			section = NULL;		// Ownership handed to hash table entry
		}
		goto reset_section_state;

	ignore_section:
		for (;;) {
			/*!local:re2c:ignore_section
				re2c:define:YYCTYPE		= char;
				re2c:define:YYCURSOR	= cur;
				re2c:yyfill:enable		= 0;

				end		= [\x00];
				eol		= [\n];

				eol eol	{ goto reset_section_state; }
				end		{ THROW_ERROR_LABEL(finally, code, "Unexpected end while ignoring section"); }
				*		{ continue; }
			*/
		}
		goto reset_section_state;

	reset_section_state:
		if (section) free_params(&section);
		section = ckalloc(sizeof *section);
		*section = (struct af_alg_params){0};
		continue;
	}

finally:
	replace_tclobj(&cmd,			NULL);
	replace_tclobj(&crypto_info,	NULL);
	replace_tclobj(&tmp,			NULL);
	if (section) free_params(&section);
	return code;
}

//>>>

// Tcl_ObjType to cache params lookups <<<
static Tcl_HashTable	g_intreps;
static void register_intrep(Tcl_Obj* obj) //<<<
{
	int				isnew;
	Tcl_HashEntry*	he = Tcl_CreateHashEntry(&g_intreps, obj, &isnew);
	if (!isnew) Tcl_Panic("obj intrep already registered");
}

//>>>
static void forget_intrep(Tcl_Obj* obj) //<<<
{
	Tcl_HashEntry*	he = Tcl_FindHashEntry(&g_intreps, obj);
	if (!he) Tcl_Panic("obj intrep not registered");
	Tcl_DeleteHashEntry(he);
}

//>>>

static void free_af_alg_params_intrep(Tcl_Obj* obj);
static void dup_af_alg_params_intrep(Tcl_Obj* src, Tcl_Obj* dest);
static Tcl_ObjType objtype_af_alg_params = {
	.name			= "af_alg_params",
	.freeIntRepProc	= free_af_alg_params_intrep,
	.dupIntRepProc	= dup_af_alg_params_intrep,
};

static void free_af_alg_params_intrep(Tcl_Obj* obj) //<<<
{
	forget_intrep(obj);
}

//>>>
static void dup_af_alg_params_intrep(Tcl_Obj* src, Tcl_Obj* dest) //<<<
{
	Tcl_ObjInternalRep*	src_ir = Tcl_FetchInternalRep(src, &objtype_af_alg_params);
	Tcl_StoreInternalRep(dest, &objtype_af_alg_params, src_ir);
	register_intrep(dest);
}

//>>>
static int get_af_alg_params_from_obj(Tcl_Interp* interp, enum info_types type, Tcl_Obj* obj, struct af_alg_params** params) //<<<
{
	int		code = TCL_OK;

	Tcl_ObjInternalRep*	ir = Tcl_FetchInternalRep(obj, &objtype_af_alg_params);
	if (ir) {
		*params = ir->twoPtrValue.ptr1;
	} else {
		TEST_OK_LABEL(finally, code, get_params(interp, type, obj, params));
		Tcl_StoreInternalRep(obj, &objtype_af_alg_params, &(Tcl_ObjInternalRep){.twoPtrValue.ptr1 = *params});
		register_intrep(obj);
	}

finally:
	return code;
}

//>>>
//>>>

INIT { //<<<
	int		code = TCL_OK;
	Tcl_InitHashTable(&g_intreps, TCL_ONE_WORD_KEYS);
	TEST_OK_LABEL(finally, code, init_g_params(interp));

	g_hmac_sha256 = (struct sockaddr_alg*)&_sa_hmac_sha256;
	*g_hmac_sha256 = (struct sockaddr_alg){
		.salg_family	= AF_ALG,
		.salg_type		= "hash",
	};
	memcpy(g_hmac_sha256->salg_name, "hmac(sha256)", 13);

finally:
	return code;
}

//>>>
RELEASE { //<<<
	Tcl_HashSearch	search;
	Tcl_HashEntry*	he = NULL;

	while ((he = Tcl_FirstHashEntry(&g_intreps, &search))) {
		Tcl_Obj*	obj = Tcl_GetHashKey(&g_intreps, he);
		Tcl_GetString(obj);
		Tcl_FreeInternalRep(obj);
	}

	for (he = Tcl_FirstHashEntry(&g_params, &search); he; he = Tcl_NextHashEntry(&search)) {
		struct af_alg_params* params = Tcl_GetHashValue(he);
		if (params) free_params(&params);
		Tcl_DeleteHashEntry(he);
	}
	Tcl_DeleteHashTable(&g_params);
}

//>>>
OBJCMD(hash) { //<<<
	int						code = TCL_OK;
	struct sockaddr_alg*	salg = NULL;
	int						sockfd = 0;
	int						fd = 0;

	enum {A_cmd, A_HASH, A_BYTES, A_objc};
	CHECK_ARGS_LABEL(finally, code, "alg bytes");

	struct af_alg_params*	params = NULL;
	TEST_OK_LABEL(finally, code, get_af_alg_params_from_obj(interp, crypto_type_hash, objv[A_HASH], &params));

	if (!params->fd) {
		int			alglen;
		const char*	alg = Tcl_GetStringFromObj(objv[A_HASH], &alglen);

		sockfd = socket(AF_ALG, SOCK_SEQPACKET, 0);
		if (sockfd == -1) THROW_POSIX_LABEL(finally, code, "socket");

		const size_t	sa_len = sizeof(struct sockaddr_alg) + alglen + 1;
		salg = ckalloc(sa_len);
		*salg = (struct sockaddr_alg){
			.salg_family	= AF_ALG,
			.salg_type		= "hash"
		};
		memcpy(salg->salg_name, alg, alglen+1);

		if (-1 == bind(sockfd, (struct sockaddr*)salg, sa_len))
			THROW_POSIX_LABEL(finally, code, "bind");

		fd = accept(sockfd, NULL, 0);
		if (fd == -1) THROW_POSIX_LABEL(finally, code, "accept");

		params->fd = fd;
		params->sockfd = sockfd;
		fd = 0;		// Hand ownership to the hash entry
		sockfd = 0;	// Hand ownership to the hash entry
	}

	{
		int			len;
		const char*	bytes = Tcl_GetByteArrayFromObj(objv[A_BYTES], &len);
		size_t		remain = len;
		while (remain) {
			const ssize_t wrote = write(params->fd, bytes, remain);
			if (wrote == -1) {
				if (errno == EINTR) continue;
				THROW_POSIX_LABEL(finally, code, "write");
			}
			remain -= wrote;
			bytes  += wrote;
		}
	}

	{
		uint8_t		digest[params->digestsize];
		uint8_t*	p = digest;
		size_t		remain = params->digestsize;
		while (remain) {
			ssize_t	got = read(params->fd, p, remain);
			if (got == -1) {
				if (errno == EINTR) continue;
				THROW_POSIX_LABEL(finally, code, "read");
			}
			remain -= got;
			p      += got;
		}

		Tcl_SetObjResult(interp, Tcl_NewByteArrayObj(digest, params->digestsize));
	}

finally:
	if (salg) {
		ckfree(salg);
		salg = NULL;
	}
	if (sockfd) close(sockfd);
	if (fd)     close(fd);
	return code;
}

//>>>
OBJCMD(hmac) { //<<<
	int						code = TCL_OK;
	int						sockfd = 0;
	int						fd = 0;
	Tcl_Obj*				hmac_name = NULL;
	Tcl_DString				ds;

	Tcl_DStringInit(&ds);

	enum {A_cmd, A_HASH, A_KEY, A_BYTES, A_objc};
	CHECK_ARGS_LABEL(finally, code, "hash key bytes");

	struct af_alg_params*	params = NULL;
	TEST_OK_LABEL(finally, code, get_af_alg_params_from_obj(interp, crypto_type_hash, objv[A_HASH], &params));
	const size_t	digestsize = params->digestsize;

	int				hashlen;
	const char*		hashname = Tcl_GetStringFromObj(objv[A_HASH], &hashlen);
	Tcl_DStringAppend(&ds, "hmac(", 5);
	Tcl_DStringAppend(&ds, hashname, hashlen);
	Tcl_DStringAppend(&ds, ")", 1);

	int				alglen = Tcl_DStringLength(&ds);
	const char*		alg    = Tcl_DStringValue(&ds);		// hmac($hash)

	// Seems like we can't re-use the fds for hmac
	sockfd = socket(AF_ALG, SOCK_SEQPACKET, 0);
	if (sockfd == -1) THROW_POSIX_LABEL(finally, code, "socket");
	//fprintf(stderr, "sockaddr_storage leaves %lld bytes for name\n", sizeof(struct sockaddr_storage) - sizeof(struct sockaddr_alg));
	struct sockaddr_storage	_sa;
	struct sockaddr_alg*	salg = (struct sockaddr_alg*)&_sa;
	*salg = (struct sockaddr_alg){
		.salg_family	= AF_ALG,
		.salg_type		= "hash",
	};
	memcpy(salg->salg_name, alg, alglen+1);

	if (-1 == bind(sockfd, (struct sockaddr*)salg, sizeof(struct sockaddr_alg)+alglen+1))
		THROW_POSIX_LABEL(finally, code, "bind");

	int				keylen;
	const uint8_t*	key = Tcl_GetByteArrayFromObj(objv[A_KEY], &keylen);
	if (-1 == setsockopt(sockfd, SOL_ALG, ALG_SET_KEY, key, keylen))
		THROW_POSIX_LABEL(finally, code, "setsockopt");

	fd = accept(sockfd, NULL, 0);
	if (fd == -1) THROW_POSIX_LABEL(finally, code, "accept");

	{
		int			len;
		const char*	bytes = Tcl_GetByteArrayFromObj(objv[A_BYTES], &len);
		size_t		remain = len;
		while (remain) {
			const ssize_t wrote = write(fd, bytes, remain);
			if (wrote == -1) {
				if (errno == EINTR) continue;
				THROW_POSIX_LABEL(finally, code, "write");
			}
			remain -= wrote;
			bytes  += wrote;
		}
	}

	{
		uint8_t		digest[digestsize];
		uint8_t*	p = digest;
		size_t		remain = digestsize;
		while (remain) {
			ssize_t	got = read(fd, p, remain);
			if (got == -1) {
				if (errno == EINTR) continue;
				THROW_POSIX_LABEL(finally, code, "read");
			}
			remain -= got;
			p      += got;
		}

		Tcl_SetObjResult(interp, Tcl_NewByteArrayObj(digest, digestsize));
	}

finally:
	if (sockfd) close(sockfd);
	if (fd)     close(fd);
	replace_tclobj(&hmac_name, NULL);
	Tcl_DStringFree(&ds);
	return code;
}

//>>>
static int apply_hmac_sha256(Tcl_Interp* interp, const uint8_t* k_bytes, size_t k_len, const uint8_t* m, size_t m_len, uint8_t* hash) //<<<
{
	int		code = TCL_OK;
	int		sockfd = 0;
	int		fd = 0;

	sockfd = socket(AF_ALG, SOCK_SEQPACKET, 0);
	if (sockfd == -1) THROW_POSIX_LABEL(finally, code, "socket");

	if (-1 == bind(sockfd, (struct sockaddr*)g_hmac_sha256, sizeof(struct sockaddr_alg)+13))
		THROW_POSIX_LABEL(finally, code, "bind");

	if (-1 == setsockopt(sockfd, SOL_ALG, ALG_SET_KEY, k_bytes, k_len))
		THROW_POSIX_LABEL(finally, code, "setsockopt");

	fd = accept(sockfd, NULL, 0);
	if (fd == -1) THROW_POSIX_LABEL(finally, code, "accept");

	if (-1 == write(fd, m, m_len)) THROW_POSIX_LABEL(finally, code, "write");
	if (-1 == read(fd, hash, 32) ) THROW_POSIX_LABEL(finally, code, "read");

finally:
	if (fd) close(fd);
	if (sockfd) close(sockfd);

	return code;
}

//>>>
OBJCMD(sasl_hi) { //<<<
	int			code = TCL_OK;

	enum {A_cmd, A_STR, A_SALT, A_IT, A_objc};
	CHECK_ARGS_LABEL(finally, code, "str salt it");

	int		str_len;
	int		salt_len;
	#ifdef Tcl_GetBytesFromObj
	const uint8_t*const	str_bytes  = Tcl_GetBytesFromObj(interp, objv[A_STR],  &str_len);
	if (!str_bytes) {code = TCL_ERROR; goto finally;}
	const uint8_t*const	salt_bytes = Tcl_GetBytesFromObj(interp, objv[A_SALT], &salt_len);
	if (!salt_bytes) {code = TCL_ERROR; goto finally;}
	#else
	const uint8_t*const	str_bytes  = Tcl_GetByteArrayFromObj(objv[A_STR],  &str_len);
	const uint8_t*const	salt_bytes = Tcl_GetByteArrayFromObj(objv[A_SALT], &salt_len);
	#endif
	int		it;
	TEST_OK_LABEL(finally, code, Tcl_GetIntFromObj(interp, objv[A_IT], &it));

	#define HLEN	32
	uint8_t		res[HLEN];
	uint8_t		next[HLEN];

	{
		uint8_t		salti[salt_len+4];
		memcpy(salti, salt_bytes, salt_len);
		salti[salt_len+0] = 0;
		salti[salt_len+1] = 0;
		salti[salt_len+2] = 0;
		salti[salt_len+3] = 1;

		TEST_OK_LABEL(finally, code, apply_hmac_sha256(interp, str_bytes, str_len, salti, salt_len+4, res));
	}
	memcpy(next, res, sizeof(next));

	size_t	c = it-1;
	while (c--) {
		uint8_t		tmp1[HLEN];
		TEST_OK_LABEL(finally, code, apply_hmac_sha256(interp, str_bytes, str_len, next, sizeof(next), tmp1));
		memcpy(next, tmp1, sizeof(next));
		{
			uint64_t*restrict	r = (uint64_t*)res;
			uint64_t*restrict	n = (uint64_t*)next;
			r[0] ^= n[0];
			r[1] ^= n[1];
			r[2] ^= n[2];
			r[3] ^= n[3];
		}
	}

	Tcl_SetObjResult(interp, Tcl_NewByteArrayObj(res, sizeof(res)));

finally:
	return code;
	#undef HLEN
}

//>>>

// vim: foldmethod=marker foldmarker=<<<,>>> ts=4 shiftwidth=4
