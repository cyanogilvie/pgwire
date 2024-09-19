#line 1 "tomcrypt_jitc.c"
#include <stdint.h>
#include <string.h>
#include <stdlib.h>

struct sha256_state {
	uint64_t		length;
	uint32_t		state[8];
	uint32_t		curlen;
	unsigned char	buf[64];
};

static Tcl_LoadHandle	libtc;

static const char* tc_syms[] = {
	"sha256_init",
	"sha256_process",
	"sha256_done",
	NULL
};
static void* procs[sizeof(tc_syms) / sizeof(tc_syms[0]) - 1] = {0};

static void (*tc_sha256_init)(struct sha256_state* md);
static void (*tc_sha256_process)(struct sha256_state* md, const unsigned char* in, unsigned long inlen);
static void (*tc_sha256_done)(struct sha256_state* md, unsigned char* hash);

INIT { //<<<
	int			code = TCL_OK;
	Tcl_Obj*	libpath = NULL;

	replace_tclobj(&libpath, Tcl_NewStringObj(LIBTC, -1));
	TEST_OK_LABEL(finally, code, Tcl_LoadFile(interp, libpath, tc_syms, TCL_LOAD_LAZY, procs, &libtc));
	*(void**)(&tc_sha256_init)		= procs[0];
	*(void**)(&tc_sha256_process)	= procs[1];
	*(void**)(&tc_sha256_done)		= procs[2];

finally:
	replace_tclobj(&libpath, NULL);
	return code;
}

//>>>
RELEASE { //<<<
	Tcl_FSUnloadFile(interp, libtc);
}

//>>>

static void do_hmac_sha256(const uint8_t* k_bytes, size_t k_len, const uint8_t* m, size_t m_len, uint8_t* hash) //<<<
{
	struct sha256_state	md;
	#define BLEN	64
	#define HLEN	32
	uint8_t	k_prime[BLEN];
	if (k_len > BLEN) {
		tc_sha256_init(&md);
		tc_sha256_process(&md, k_bytes, k_len);
		tc_sha256_done(&md, hash);
		memcpy(k_prime, hash, HLEN);
		memset(k_prime+HLEN, 0, BLEN-HLEN);
	} else {
		memcpy(k_prime, k_bytes, k_len);
		memset(k_prime+k_len, 0, BLEN-k_len);
	}

	#define STATICSIZE	1024
	uint8_t			sbuf[STATICSIZE];
	const size_t	buflen = BLEN + HLEN + BLEN + m_len;
	uint8_t*		dbuf = (buflen <= STATICSIZE) ? sbuf : malloc(buflen);
	uint8_t*		h_i = NULL;
	uint8_t*		tail = NULL;
	{
		const uint64_t*restrict	k = (const uint64_t*)k_prime;
		uint64_t*restrict		p = (uint64_t*)dbuf;

		p[0] = k[0] ^ 0x5c5c5c5c5c5c5c5cULL;
		p[1] = k[1] ^ 0x5c5c5c5c5c5c5c5cULL;
		p[2] = k[2] ^ 0x5c5c5c5c5c5c5c5cULL;
		p[3] = k[3] ^ 0x5c5c5c5c5c5c5c5cULL;
		p[4] = k[4] ^ 0x5c5c5c5c5c5c5c5cULL;
		p[5] = k[5] ^ 0x5c5c5c5c5c5c5c5cULL;
		p[6] = k[6] ^ 0x5c5c5c5c5c5c5c5cULL;
		p[7] = k[7] ^ 0x5c5c5c5c5c5c5c5cULL;
		h_i = (uint8_t*)(p+8);

		p = (uint64_t*)(dbuf + BLEN + HLEN);
		p[0] = k[0] ^ 0x3636363636363636ULL;
		p[1] = k[1] ^ 0x3636363636363636ULL;
		p[2] = k[2] ^ 0x3636363636363636ULL;
		p[3] = k[3] ^ 0x3636363636363636ULL;
		p[4] = k[4] ^ 0x3636363636363636ULL;
		p[5] = k[5] ^ 0x3636363636363636ULL;
		p[6] = k[6] ^ 0x3636363636363636ULL;
		p[7] = k[7] ^ 0x3636363636363636ULL;
		tail = (uint8_t*)(dbuf + BLEN + HLEN + BLEN);
	}
	memcpy(tail, m, m_len);

	tc_sha256_init(&md);
	tc_sha256_process(&md, h_i+HLEN, BLEN+m_len);
	tc_sha256_done(&md, h_i);

	tc_sha256_init(&md);
	tc_sha256_process(&md, dbuf, BLEN+HLEN);
	tc_sha256_done(&md, hash);

	if (dbuf && dbuf != sbuf) {
		free(dbuf);
		dbuf = NULL;
	}
	#undef BLEN
	#undef HLEN
	#undef STATICSIZE
}

//>>>

OBJCMD(sha256) { //<<<
	int					code = TCL_OK;

	enum {A_cmd, A_BYTES, A_objc};
	CHECK_ARGS_LABEL(finally, code, "bytes");

	int					len;
	#ifdef Tcl_GetBytesFromObj
	const uint8_t*const	bytes = Tcl_GetBytesFromObj(interp, objv[A_BYTES], &len);
	if (!bytes) {code = TCL_ERROR; goto finally;}
	#else
	const uint8_t*const	bytes = Tcl_GetByteArrayFromObj(objv[A_BYTES], &len);
	#endif
	struct sha256_state	md;
	uint8_t				hash[256/8];

	tc_sha256_init(&md);
	tc_sha256_process(&md, bytes, len);
	tc_sha256_done(&md, hash);

	Tcl_SetObjResult(interp, Tcl_NewByteArrayObj(hash, sizeof(hash)));

finally:
	return code;
}

//>>>
OBJCMD(hmac_sha256) { //<<<
	int					code = TCL_OK;
	struct sha256_state	md;
	uint8_t				hash[256/8];

	enum {A_cmd, A_K, A_M, A_objc};
	CHECK_ARGS_LABEL(finally, code, "K m");

	int					k_len, m_len;
	#ifdef Tcl_GetBytesFromObj
	const uint8_t*const	k_bytes = Tcl_GetBytesFromObj(interp, objv[A_K], &k_len);
	if (!k_bytes) {code = TCL_ERROR; goto finally;}
	const uint8_t*const	m_bytes = Tcl_GetBytesFromObj(interp, objv[A_M], &m_len);
	if (!m_bytes) {code = TCL_ERROR; goto finally;}
	#else
	const uint8_t*const	k_bytes = Tcl_GetByteArrayFromObj(objv[A_K], &k_len);
	const uint8_t*const	m_bytes = Tcl_GetByteArrayFromObj(objv[A_M], &m_len);
	#endif

	do_hmac_sha256(k_bytes, k_len, m_bytes, m_len, hash);

	Tcl_SetObjResult(interp, Tcl_NewByteArrayObj(hash, sizeof(hash)));

finally:
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

		do_hmac_sha256(str_bytes, str_len, salti, salt_len+4, res);
	}
	memcpy(next, res, sizeof(next));

	size_t	c = it-1;
	while (c--) {
		uint8_t		tmp1[HLEN];
		do_hmac_sha256(str_bytes, str_len, next, sizeof(next), tmp1);
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
