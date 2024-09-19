#line 1 "jitc_hash.c"
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* Derived from: LibTomCrypt, modular cryptographic library -- Tom St Denis */
/* SPDX-License-Identifier: Unlicense */

#define RORc(x, y) ( ((((uint32_t)(x)&0xFFFFFFFFUL)>>(uint32_t)((y)&31)) | ((uint32_t)(x)<<(uint32_t)((32-((y)&31))&31))) & 0xFFFFFFFFUL)
#define ROLc(x, y) ( (((uint32_t)(x)<<(uint32_t)((y)&31)) | (((uint32_t)(x)&0xFFFFFFFFUL)>>(uint32_t)((32-((y)&31))&31))) & 0xFFFFFFFFUL)

#define LOAD32L(x, y)                         \
	do {                                      \
		x = ((uint32_t)((y)[3] & 0xFF)<<24) | \
			((uint32_t)((y)[2] & 0xFF)<<16) | \
			((uint32_t)((y)[1] & 0xFF)<< 8) | \
			((uint32_t)((y)[0] & 0xFF)    );  \
	} while(0)

#define LOAD32H(x, y)                         \
	do {                                      \
		x = ((uint32_t)((y)[0] & 0xFF)<<24) | \
			((uint32_t)((y)[1] & 0xFF)<<16) | \
			((uint32_t)((y)[2] & 0xFF)<< 8) | \
			((uint32_t)((y)[3] & 0xFF)    );  \
	} while(0)

#define STORE32L(x, y)                        \
	do {                                      \
		(y)[3] = (uint8_t)(((x)>>24) & 0xFF); \
		(y)[2] = (uint8_t)(((x)>>16) & 0xFF); \
		(y)[1] = (uint8_t)(((x)>> 8) & 0xFF); \
		(y)[0] = (uint8_t)( (x)      & 0xFF); \
	} while(0)

#define STORE32H(x, y)                        \
	do {                                      \
		(y)[0] = (uint8_t)(((x)>>24) & 0xFF); \
		(y)[1] = (uint8_t)(((x)>>16) & 0xFF); \
		(y)[2] = (uint8_t)(((x)>> 8) & 0xFF); \
		(y)[3] = (uint8_t)( (x)      & 0xFF); \
	} while(0)

#define STORE64L(x, y)                        \
	do {                                      \
		(y)[7] = (uint8_t)(((x)>>56) & 0xFF); \
		(y)[6] = (uint8_t)(((x)>>48) & 0xFF); \
		(y)[5] = (uint8_t)(((x)>>40) & 0xFF); \
		(y)[4] = (uint8_t)(((x)>>32) & 0xFF); \
		(y)[3] = (uint8_t)(((x)>>24) & 0xFF); \
		(y)[2] = (uint8_t)(((x)>>16) & 0xFF); \
		(y)[1] = (uint8_t)(((x)>> 8) & 0xFF); \
		(y)[0] = (uint8_t)( (x)      & 0xFF); \
	} while(0)

#define STORE64H(x, y)                        \
	do {                                      \
		(y)[0] = (uint8_t)(((x)>>56) & 0xFF); \
		(y)[1] = (uint8_t)(((x)>>48) & 0xFF); \
		(y)[2] = (uint8_t)(((x)>>40) & 0xFF); \
		(y)[3] = (uint8_t)(((x)>>32) & 0xFF); \
		(y)[4] = (uint8_t)(((x)>>24) & 0xFF); \
		(y)[5] = (uint8_t)(((x)>>16) & 0xFF); \
		(y)[6] = (uint8_t)(((x)>> 8) & 0xFF); \
		(y)[7] = (uint8_t)( (x)      & 0xFF); \
	} while(0)

#if defined(__x86_64__)
// TODO: 
#elif defined(__aarch64__)
#endif

#define Ch(x,y,z)       (z ^ (x & (y ^ z)))
#define Maj(x,y,z)      (((x | y) & z) | (x & y))
#define S(x, n)         RORc((x),(n))
#define R(x, n)         (((x)&0xFFFFFFFFUL)>>(n))
#define Sigma0(x)       (S(x,  2) ^ S(x, 13) ^ S(x, 22))
#define Sigma1(x)       (S(x,  6) ^ S(x, 11) ^ S(x, 25))
#define Gamma0(x)       (S(x,  7) ^ S(x, 18) ^ R(x,  3))
#define Gamma1(x)       (S(x, 17) ^ S(x, 19) ^ R(x, 10))

// SHA256 <<<
#define BLOCKSIZE	64
#define HLEN		32

struct sha256_state {
	uint64_t		length;
	uint32_t		state[8];
	uint32_t		curlen;
	uint8_t			buf[BLOCKSIZE];
};

static void sha256_compress(struct sha256_state* md, const uint8_t* buf) // compress 512-bits: <<<
{
	uint32_t	S[8], W[64], t0, t1;
	int			i;

	/* copy state into S */
	for (i=0; i<8; i++)
		S[i] = md->state[i];

	/* copy the state into 512-bits into W[0..15] */
	for (i=0; i<16; i++)
		LOAD32H(W[i], buf + (4*i));

	/* fill W[16..63] */
	for (i=16; i<64; i++)
		W[i] = Gamma1(W[i - 2]) + W[i - 7] + Gamma0(W[i - 15]) + W[i - 16];

	/* Compress */
	#define RND(a,b,c,d,e,f,g,h,i,ki)                   \
		t0 = h + Sigma1(e) + Ch(e, f, g) + ki + W[i];   \
		t1 = Sigma0(a) + Maj(a, b, c);                  \
		d += t0;                                        \
		h  = t0 + t1;

	RND(S[0],S[1],S[2],S[3],S[4],S[5],S[6],S[7],0,0x428a2f98)
	RND(S[7],S[0],S[1],S[2],S[3],S[4],S[5],S[6],1,0x71374491)
	RND(S[6],S[7],S[0],S[1],S[2],S[3],S[4],S[5],2,0xb5c0fbcf)
	RND(S[5],S[6],S[7],S[0],S[1],S[2],S[3],S[4],3,0xe9b5dba5)
	RND(S[4],S[5],S[6],S[7],S[0],S[1],S[2],S[3],4,0x3956c25b)
	RND(S[3],S[4],S[5],S[6],S[7],S[0],S[1],S[2],5,0x59f111f1)
	RND(S[2],S[3],S[4],S[5],S[6],S[7],S[0],S[1],6,0x923f82a4)
	RND(S[1],S[2],S[3],S[4],S[5],S[6],S[7],S[0],7,0xab1c5ed5)
	RND(S[0],S[1],S[2],S[3],S[4],S[5],S[6],S[7],8,0xd807aa98)
	RND(S[7],S[0],S[1],S[2],S[3],S[4],S[5],S[6],9,0x12835b01)
	RND(S[6],S[7],S[0],S[1],S[2],S[3],S[4],S[5],10,0x243185be)
	RND(S[5],S[6],S[7],S[0],S[1],S[2],S[3],S[4],11,0x550c7dc3)
	RND(S[4],S[5],S[6],S[7],S[0],S[1],S[2],S[3],12,0x72be5d74)
	RND(S[3],S[4],S[5],S[6],S[7],S[0],S[1],S[2],13,0x80deb1fe)
	RND(S[2],S[3],S[4],S[5],S[6],S[7],S[0],S[1],14,0x9bdc06a7)
	RND(S[1],S[2],S[3],S[4],S[5],S[6],S[7],S[0],15,0xc19bf174)
	RND(S[0],S[1],S[2],S[3],S[4],S[5],S[6],S[7],16,0xe49b69c1)
	RND(S[7],S[0],S[1],S[2],S[3],S[4],S[5],S[6],17,0xefbe4786)
	RND(S[6],S[7],S[0],S[1],S[2],S[3],S[4],S[5],18,0x0fc19dc6)
	RND(S[5],S[6],S[7],S[0],S[1],S[2],S[3],S[4],19,0x240ca1cc)
	RND(S[4],S[5],S[6],S[7],S[0],S[1],S[2],S[3],20,0x2de92c6f)
	RND(S[3],S[4],S[5],S[6],S[7],S[0],S[1],S[2],21,0x4a7484aa)
	RND(S[2],S[3],S[4],S[5],S[6],S[7],S[0],S[1],22,0x5cb0a9dc)
	RND(S[1],S[2],S[3],S[4],S[5],S[6],S[7],S[0],23,0x76f988da)
	RND(S[0],S[1],S[2],S[3],S[4],S[5],S[6],S[7],24,0x983e5152)
	RND(S[7],S[0],S[1],S[2],S[3],S[4],S[5],S[6],25,0xa831c66d)
	RND(S[6],S[7],S[0],S[1],S[2],S[3],S[4],S[5],26,0xb00327c8)
	RND(S[5],S[6],S[7],S[0],S[1],S[2],S[3],S[4],27,0xbf597fc7)
	RND(S[4],S[5],S[6],S[7],S[0],S[1],S[2],S[3],28,0xc6e00bf3)
	RND(S[3],S[4],S[5],S[6],S[7],S[0],S[1],S[2],29,0xd5a79147)
	RND(S[2],S[3],S[4],S[5],S[6],S[7],S[0],S[1],30,0x06ca6351)
	RND(S[1],S[2],S[3],S[4],S[5],S[6],S[7],S[0],31,0x14292967)
	RND(S[0],S[1],S[2],S[3],S[4],S[5],S[6],S[7],32,0x27b70a85)
	RND(S[7],S[0],S[1],S[2],S[3],S[4],S[5],S[6],33,0x2e1b2138)
	RND(S[6],S[7],S[0],S[1],S[2],S[3],S[4],S[5],34,0x4d2c6dfc)
	RND(S[5],S[6],S[7],S[0],S[1],S[2],S[3],S[4],35,0x53380d13)
	RND(S[4],S[5],S[6],S[7],S[0],S[1],S[2],S[3],36,0x650a7354)
	RND(S[3],S[4],S[5],S[6],S[7],S[0],S[1],S[2],37,0x766a0abb)
	RND(S[2],S[3],S[4],S[5],S[6],S[7],S[0],S[1],38,0x81c2c92e)
	RND(S[1],S[2],S[3],S[4],S[5],S[6],S[7],S[0],39,0x92722c85)
	RND(S[0],S[1],S[2],S[3],S[4],S[5],S[6],S[7],40,0xa2bfe8a1)
	RND(S[7],S[0],S[1],S[2],S[3],S[4],S[5],S[6],41,0xa81a664b)
	RND(S[6],S[7],S[0],S[1],S[2],S[3],S[4],S[5],42,0xc24b8b70)
	RND(S[5],S[6],S[7],S[0],S[1],S[2],S[3],S[4],43,0xc76c51a3)
	RND(S[4],S[5],S[6],S[7],S[0],S[1],S[2],S[3],44,0xd192e819)
	RND(S[3],S[4],S[5],S[6],S[7],S[0],S[1],S[2],45,0xd6990624)
	RND(S[2],S[3],S[4],S[5],S[6],S[7],S[0],S[1],46,0xf40e3585)
	RND(S[1],S[2],S[3],S[4],S[5],S[6],S[7],S[0],47,0x106aa070)
	RND(S[0],S[1],S[2],S[3],S[4],S[5],S[6],S[7],48,0x19a4c116)
	RND(S[7],S[0],S[1],S[2],S[3],S[4],S[5],S[6],49,0x1e376c08)
	RND(S[6],S[7],S[0],S[1],S[2],S[3],S[4],S[5],50,0x2748774c)
	RND(S[5],S[6],S[7],S[0],S[1],S[2],S[3],S[4],51,0x34b0bcb5)
	RND(S[4],S[5],S[6],S[7],S[0],S[1],S[2],S[3],52,0x391c0cb3)
	RND(S[3],S[4],S[5],S[6],S[7],S[0],S[1],S[2],53,0x4ed8aa4a)
	RND(S[2],S[3],S[4],S[5],S[6],S[7],S[0],S[1],54,0x5b9cca4f)
	RND(S[1],S[2],S[3],S[4],S[5],S[6],S[7],S[0],55,0x682e6ff3)
	RND(S[0],S[1],S[2],S[3],S[4],S[5],S[6],S[7],56,0x748f82ee)
	RND(S[7],S[0],S[1],S[2],S[3],S[4],S[5],S[6],57,0x78a5636f)
	RND(S[6],S[7],S[0],S[1],S[2],S[3],S[4],S[5],58,0x84c87814)
	RND(S[5],S[6],S[7],S[0],S[1],S[2],S[3],S[4],59,0x8cc70208)
	RND(S[4],S[5],S[6],S[7],S[0],S[1],S[2],S[3],60,0x90befffa)
	RND(S[3],S[4],S[5],S[6],S[7],S[0],S[1],S[2],61,0xa4506ceb)
	RND(S[2],S[3],S[4],S[5],S[6],S[7],S[0],S[1],62,0xbef9a3f7)
	RND(S[1],S[2],S[3],S[4],S[5],S[6],S[7],S[0],63,0xc67178f2)
	#undef RND

	for (i = 0; i < 8; i++)
		md->state[i] = md->state[i] + S[i];
}

//>>>
static inline void sha256_init(struct sha256_state* md) //<<<
{
	*md = (struct sha256_state){
		.state = {
			0x6A09E667UL,
			0xBB67AE85UL,
			0x3C6EF372UL,
			0xA54FF53AUL,
			0x510E527FUL,
			0x9B05688CUL,
			0x1F83D9ABUL,
			0x5BE0CD19UL,
		}
	};
}

//>>>
static void sha256_process(struct sha256_state* md, const uint8_t* in, size_t inlen) //<<<
{
	while (inlen) {
		if (md->curlen == 0 && inlen >= BLOCKSIZE) {
			sha256_compress(md, in);
			md->length += BLOCKSIZE * 8;
			in         += BLOCKSIZE;
			inlen      -= BLOCKSIZE;
		} else {
			const size_t	bufrem = BLOCKSIZE - md->curlen;
			const size_t	n = inlen < bufrem ? inlen : bufrem;
			memcpy(md->buf, in, n);
			md->curlen += n;
			in         += n;
			inlen      -= n;
			if (md->curlen == BLOCKSIZE) {
				sha256_compress(md, md->buf);
				md->length += BLOCKSIZE * 8;
				md->curlen = 0;
			}
		}
	}
}

//>>>
static void sha256_done(struct sha256_state* md, uint8_t* out) //<<<
{
	int i;

	/* increase the length of the message */
	md->length += md->curlen * 8;

	/* append the '1' bit */
	md->buf[md->curlen++] = 0x80;

	/* if the length is currently above 56 bytes we append zeros
	* then compress.  Then we can fall back to padding zeros and length
	* encoding like normal.
	*/
	if (md->curlen > BLOCKSIZE-sizeof(uint64_t)) {
		while (md->curlen < BLOCKSIZE) md->buf[md->curlen++] = 0;
		sha256_compress(md, md->buf);
		md->curlen = 0;
	}

	/* pad upto 56 bytes of zeroes */
	while (md->curlen < BLOCKSIZE-sizeof(uint64_t)) md->buf[md->curlen++] = 0;

	/* store length */
	STORE64H(md->length, md->buf+BLOCKSIZE-sizeof(uint64_t));
	sha256_compress(md, md->buf);

	/* copy output */
	for (i=0; i<HLEN/sizeof(uint32_t); i++) STORE32H(md->state[i], out+(sizeof(uint32_t)*i));
}

//>>>
OBJCMD(sha256) { //<<<
	int			code = TCL_OK;

	enum {A_cmd, A_BYTES, A_objc};
	CHECK_ARGS_LABEL(finally, code, "bytes");

	struct sha256_state		md;
	uint8_t					hash[HLEN];

	int		len;
	#ifdef Tcl_GetBytesFromObj
	const uint8_t*	bytes = Tcl_GetBytesFromObj(interp, objv[A_BYTES], &len);
	#else
	const uint8_t*	bytes = Tcl_GetByteArrayFromObj(objv[A_BYTES], &len);
	#endif

	sha256_init(&md);
	sha256_process(&md, bytes, len);
	sha256_done(&md, hash);

	Tcl_SetObjResult(interp, Tcl_NewByteArrayObj(hash, HLEN));

finally:
	return code;
}

//>>>
static void do_hmac_sha256(const uint8_t* k_bytes, size_t k_len, const uint8_t* m, size_t m_len, uint8_t* hash) //<<<
{
	struct sha256_state	md;
	uint8_t				k_prime[BLOCKSIZE];
	if (k_len > BLOCKSIZE) {
		sha256_init(&md);
		sha256_process(&md, k_bytes, k_len);
		sha256_done(&md, hash);
		memcpy(k_prime, hash, HLEN);
		memset(k_prime+HLEN, 0, BLOCKSIZE-HLEN);
	} else {
		memcpy(k_prime, k_bytes, k_len);
		memset(k_prime+k_len, 0, BLOCKSIZE-k_len);
	}

	#define STATICSIZE	1024
	uint8_t			sbuf[STATICSIZE];
	const size_t	buflen = BLOCKSIZE + HLEN + BLOCKSIZE + m_len;
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

		p = (uint64_t*)(dbuf + BLOCKSIZE + HLEN);
		p[0] = k[0] ^ 0x3636363636363636ULL;
		p[1] = k[1] ^ 0x3636363636363636ULL;
		p[2] = k[2] ^ 0x3636363636363636ULL;
		p[3] = k[3] ^ 0x3636363636363636ULL;
		p[4] = k[4] ^ 0x3636363636363636ULL;
		p[5] = k[5] ^ 0x3636363636363636ULL;
		p[6] = k[6] ^ 0x3636363636363636ULL;
		p[7] = k[7] ^ 0x3636363636363636ULL;
		tail = (uint8_t*)(dbuf + BLOCKSIZE + HLEN + BLOCKSIZE);
	}
	memcpy(tail, m, m_len);

	sha256_init(&md);
	sha256_process(&md, h_i+HLEN, BLOCKSIZE+m_len);
	sha256_done(&md, h_i);

	sha256_init(&md);
	sha256_process(&md, dbuf, BLOCKSIZE+HLEN);
	sha256_done(&md, hash);

	if (dbuf && dbuf != sbuf) {
		free(dbuf);
		dbuf = NULL;
	}
	#undef STATICSIZE
}

//>>>
OBJCMD(hmac_sha256) { //<<<
	int					code = TCL_OK;
	struct sha256_state	md;
	uint8_t				hash[HLEN];

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
}

//>>>

#undef BLOCKSIZE
#undef HLEN
// SHA256 >>>

// MD5 <<<
#define BLOCKSIZE	64
#define HLEN		16

struct md5_state {
	uint64_t	length;
	uint32_t	state[4];
	uint32_t	curlen;
	uint8_t		buf[BLOCKSIZE];
};

#define F(x,y,z)  (z ^ (x & (y ^ z)))
#define G(x,y,z)  (y ^ (z & (y ^ x)))
#define H(x,y,z)  (x^y^z)
#define I(x,y,z)  (y^(x|(~z)))


#define FF(a,b,c,d,M,s,t) \
    a = (a + F(b,c,d) + M + t); a = ROLc(a, s) + b;

#define GG(a,b,c,d,M,s,t) \
    a = (a + G(b,c,d) + M + t); a = ROLc(a, s) + b;

#define HH(a,b,c,d,M,s,t) \
    a = (a + H(b,c,d) + M + t); a = ROLc(a, s) + b;

#define II(a,b,c,d,M,s,t) \
    a = (a + I(b,c,d) + M + t); a = ROLc(a, s) + b;



static void md5_compress(struct md5_state* md, const uint8_t* buf) //<<<
{
	uint32_t	i, W[16], a, b, c, d;

	/* copy the state into 512-bits into W[0..15] */
	for (i=0; i<16; i++) LOAD32L(W[i], buf + (4*i));

	/* copy state */
	a = md->state[0];
	b = md->state[1];
	c = md->state[2];
	d = md->state[3];

	FF(a,b,c,d,W[0],7,0xd76aa478UL)
	FF(d,a,b,c,W[1],12,0xe8c7b756UL)
	FF(c,d,a,b,W[2],17,0x242070dbUL)
	FF(b,c,d,a,W[3],22,0xc1bdceeeUL)
	FF(a,b,c,d,W[4],7,0xf57c0fafUL)
	FF(d,a,b,c,W[5],12,0x4787c62aUL)
	FF(c,d,a,b,W[6],17,0xa8304613UL)
	FF(b,c,d,a,W[7],22,0xfd469501UL)
	FF(a,b,c,d,W[8],7,0x698098d8UL)
	FF(d,a,b,c,W[9],12,0x8b44f7afUL)
	FF(c,d,a,b,W[10],17,0xffff5bb1UL)
	FF(b,c,d,a,W[11],22,0x895cd7beUL)
	FF(a,b,c,d,W[12],7,0x6b901122UL)
	FF(d,a,b,c,W[13],12,0xfd987193UL)
	FF(c,d,a,b,W[14],17,0xa679438eUL)
	FF(b,c,d,a,W[15],22,0x49b40821UL)
	GG(a,b,c,d,W[1],5,0xf61e2562UL)
	GG(d,a,b,c,W[6],9,0xc040b340UL)
	GG(c,d,a,b,W[11],14,0x265e5a51UL)
	GG(b,c,d,a,W[0],20,0xe9b6c7aaUL)
	GG(a,b,c,d,W[5],5,0xd62f105dUL)
	GG(d,a,b,c,W[10],9,0x02441453UL)
	GG(c,d,a,b,W[15],14,0xd8a1e681UL)
	GG(b,c,d,a,W[4],20,0xe7d3fbc8UL)
	GG(a,b,c,d,W[9],5,0x21e1cde6UL)
	GG(d,a,b,c,W[14],9,0xc33707d6UL)
	GG(c,d,a,b,W[3],14,0xf4d50d87UL)
	GG(b,c,d,a,W[8],20,0x455a14edUL)
	GG(a,b,c,d,W[13],5,0xa9e3e905UL)
	GG(d,a,b,c,W[2],9,0xfcefa3f8UL)
	GG(c,d,a,b,W[7],14,0x676f02d9UL)
	GG(b,c,d,a,W[12],20,0x8d2a4c8aUL)
	HH(a,b,c,d,W[5],4,0xfffa3942UL)
	HH(d,a,b,c,W[8],11,0x8771f681UL)
	HH(c,d,a,b,W[11],16,0x6d9d6122UL)
	HH(b,c,d,a,W[14],23,0xfde5380cUL)
	HH(a,b,c,d,W[1],4,0xa4beea44UL)
	HH(d,a,b,c,W[4],11,0x4bdecfa9UL)
	HH(c,d,a,b,W[7],16,0xf6bb4b60UL)
	HH(b,c,d,a,W[10],23,0xbebfbc70UL)
	HH(a,b,c,d,W[13],4,0x289b7ec6UL)
	HH(d,a,b,c,W[0],11,0xeaa127faUL)
	HH(c,d,a,b,W[3],16,0xd4ef3085UL)
	HH(b,c,d,a,W[6],23,0x04881d05UL)
	HH(a,b,c,d,W[9],4,0xd9d4d039UL)
	HH(d,a,b,c,W[12],11,0xe6db99e5UL)
	HH(c,d,a,b,W[15],16,0x1fa27cf8UL)
	HH(b,c,d,a,W[2],23,0xc4ac5665UL)
	II(a,b,c,d,W[0],6,0xf4292244UL)
	II(d,a,b,c,W[7],10,0x432aff97UL)
	II(c,d,a,b,W[14],15,0xab9423a7UL)
	II(b,c,d,a,W[5],21,0xfc93a039UL)
	II(a,b,c,d,W[12],6,0x655b59c3UL)
	II(d,a,b,c,W[3],10,0x8f0ccc92UL)
	II(c,d,a,b,W[10],15,0xffeff47dUL)
	II(b,c,d,a,W[1],21,0x85845dd1UL)
	II(a,b,c,d,W[8],6,0x6fa87e4fUL)
	II(d,a,b,c,W[15],10,0xfe2ce6e0UL)
	II(c,d,a,b,W[6],15,0xa3014314UL)
	II(b,c,d,a,W[13],21,0x4e0811a1UL)
	II(a,b,c,d,W[4],6,0xf7537e82UL)
	II(d,a,b,c,W[11],10,0xbd3af235UL)
	II(c,d,a,b,W[2],15,0x2ad7d2bbUL)
	II(b,c,d,a,W[9],21,0xeb86d391UL)

	md->state[0] = md->state[0] + a;
	md->state[1] = md->state[1] + b;
	md->state[2] = md->state[2] + c;
	md->state[3] = md->state[3] + d;
}

//>>>
static inline void md5_init(struct md5_state* md) //<<<
{
	*md = (struct md5_state){
		.state = {
			0x67452301UL,
			0xefcdab89UL,
			0x98badcfeUL,
			0x10325476UL,
		}
	};
}

//>>>
static void md5_process(struct md5_state* md, const uint8_t* in, size_t inlen) //<<<
{
	while (inlen) {
		if (md->curlen == 0 && inlen >= BLOCKSIZE) {
			md5_compress(md, in);
			md->length += BLOCKSIZE * 8;
			in         += BLOCKSIZE;
			inlen      -= BLOCKSIZE;
		} else {
			const size_t	bufrem = BLOCKSIZE - md->curlen;
			const size_t	n = inlen < bufrem ? inlen : bufrem;
			memcpy(md->buf, in, n);
			md->curlen += n;
			in         += n;
			inlen      -= n;
			if (md->curlen == BLOCKSIZE) {
				md5_compress(md, md->buf);
				md->length += BLOCKSIZE * 8;
				md->curlen = 0;
			}
		}
	}
}

//>>>
static void md5_done(struct md5_state* md, uint8_t* out) //<<<
{
    /* increase the length of the message */
    md->length += md->curlen * 8;

    /* append the '1' bit */
    md->buf[md->curlen++] = 0x80;

    /* if the length is currently above 56 bytes we append zeros
     * then compress.  Then we can fall back to padding zeros and length
     * encoding like normal.
     */
    if (md->curlen > BLOCKSIZE-sizeof(uint64_t)) {
        while (md->curlen < BLOCKSIZE) md->buf[md->curlen++] = 0;
        md5_compress(md, md->buf);
        md->curlen = 0;
    }

    /* pad upto 56 bytes of zeroes */
    while (md->curlen < BLOCKSIZE-sizeof(uint64_t)) md->buf[md->curlen++] = 0;

    /* store length */
    STORE64L(md->length, md->buf+BLOCKSIZE-sizeof(uint64_t));
    md5_compress(md, md->buf);

    /* copy output */
    for (int i=0; i<HLEN/sizeof(uint32_t); i++) STORE32L(md->state[i], out+(sizeof(uint32_t)*i));
}

//>>>
OBJCMD(md5) { //<<<
	int			code = TCL_OK;

	enum {A_cmd, A_BYTES, A_objc};
	CHECK_ARGS_LABEL(finally, code, "bytes");

	struct md5_state		md;
	uint8_t					hash[HLEN];

	int		len;
	#ifdef Tcl_GetBytesFromObj
	const uint8_t*	bytes = Tcl_GetBytesFromObj(interp, objv[A_BYTES], &len);
	#else
	const uint8_t*	bytes = Tcl_GetByteArrayFromObj(objv[A_BYTES], &len);
	#endif

	md5_init(&md);
	md5_process(&md, bytes, len);
	md5_done(&md, hash);

	Tcl_SetObjResult(interp, Tcl_NewByteArrayObj(hash, HLEN));
finally:
	return code;
}

//>>>
#undef BLOCKSIZE
#undef HLEN
// MD5 >>>

// vim: foldmethod=marker foldmarker=<<<,>>> ts=4 shiftwidth=4
