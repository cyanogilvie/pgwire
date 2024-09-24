#line 1 "accel.c"
#include <byteswap.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>

#define PGWIRE_LITS \
	X( BLANK,	"" ) \
	X( ONE,		"1" ) \
	X( ZERO,	"0" ) \
	X( TRUE,	"1" ) \
	X( FALSE,	"0" )
enum {
	#define X(sym, str) PGWIRE_LIT_##sym,
	PGWIRE_LITS
	#undef X
	PGWIRE_LIT_SIZE
};
const char* lit_str[PGWIRE_LIT_SIZE] = {
	#define X(sym, str) str,
	PGWIRE_LITS
	#undef X
};
static Tcl_Obj*	lit[PGWIRE_LIT_SIZE] = {0};

INIT {
	for (size_t i=0; i<PGWIRE_LIT_SIZE; i++) replace_tclobj(&lit[i], Tcl_NewStringObj(lit_str[i], -1));
	//Tcl_Eval(interp,
	//	"puts \"Init pgwire cdef, tid: [file tail [file readlink /proc/thread-self]], [thread::id], name: [if {[info exists ::ns_shim::interp_name]} {set ::ns_shim::interp_name}][if {[info exists ::ns_shim::interp_name_suffix]} {string cat / $::ns_shim::interp_name_suffix}]\"");
	return TCL_OK;
}

RELEASE {
	for (size_t i=0; i<PGWIRE_LIT_SIZE; i++) replace_tclobj(&lit[i], NULL);
}

struct column_cx {
	Tcl_Encoding	encoding;
	int				rs;
	Tcl_Obj**		cols;
};

%accel_ops%
#line 45 "accel.c"

struct foreach_state {
	struct column_cx	col;
	int					r;
	int					datarowc;
	Tcl_Obj**			datarowv;
	int					colcount;
	Tcl_Obj**			rowv;
	Tcl_Obj*			row;
	Tcl_Obj*			rowvar;
	Tcl_Obj*			script;
	col_op**			ops;
	int					delimc;
	Tcl_Obj**			delimv;
};

OBJCMD(c_foreach_batch_nr_setup);
static int c_foreach_batch_nr_loop_top(Tcl_Interp* interp, struct foreach_state* s);
static int c_foreach_batch_nr_loop_bot(ClientData cdata[], Tcl_Interp* interp, int result);

static void free_foreach_state(struct foreach_state* s) //<<<
{
	//fprintf(stderr, "freeing foreach_state %p\n", s);
	if (s) {
		if (s->col.cols) {
			for (int i=0; i < s->colcount; i++)
				replace_tclobj(&s->col.cols[i], NULL);
			ckfree(s->col.cols); s->col.cols = NULL;
		}

		if (s->datarowv) {
			for (int i=0; i < s->datarowc; i++)
				replace_tclobj(&s->datarowv[i], NULL);
			ckfree(s->datarowv); s->datarowv = NULL;
		}

		if (s->rowv) {
			for (int i = 0; i < s->colcount * 2; i++)
				replace_tclobj(&s->rowv[i], NULL);
			ckfree(s->rowv); s->rowv = NULL;
		}

		replace_tclobj(&s->row,    NULL);
		replace_tclobj(&s->rowvar, NULL);
		replace_tclobj(&s->script, NULL);

		if (s->ops) {
			ckfree(s->ops); s->ops = NULL;
		}

		ckfree(s); s = NULL;
	}
}

//>>>
OBJCMD(c_foreach_batch_nr) //<<<
{
	//fprintf(stderr, "c_foreach_batch_nr\n");
	return Tcl_NRCallObjProc(interp, c_foreach_batch_nr_setup, cdata, objc, objv);
}

//>>>
OBJCMD(c_foreach_batch_nr_setup) //<<<
{
	int						code = TCL_OK;
	struct foreach_state*	s = NULL;
	Tcl_Obj**				datarowv = NULL;
	int						datarowc;
	Tcl_Obj**				colv = NULL;
	int						colc;
	Tcl_Encoding			encoding;
	int						i;
	Tcl_Obj**				delimv = NULL;
	int						delimc;

	enum {A_cmd, A_ROWVAR, A_OPS, A_COLS, A_ENCODING, A_DATAROWS, A_SCRIPT, A_DELIMS, A_objc};
	CHECK_ARGS_LABEL(err, code, "rowvar ops columns tcl_encoding datarows script delims");

	TEST_OK_LABEL(err, code, (Tcl_ListObjGetElements(interp, objv[A_DATAROWS], &datarowc, &datarowv)));
	if (datarowc == 0) goto err;	// Not really an error, just nothing to do.  code is still TCL_OK

	TEST_OK_LABEL(err, code, (Tcl_ListObjGetElements(interp, objv[A_COLS], &colc, &colv)));
	TEST_OK_LABEL(err, code, (Tcl_GetEncodingFromObj(interp, objv[A_ENCODING], &encoding)));
	TEST_OK_LABEL(err, code, Tcl_ListObjGetElements(interp, objv[A_DELIMS], &delimc, &delimv));

	s = ckalloc(sizeof(*s));
	memset(s, 0, sizeof(*s));

	s->col.encoding = encoding;
	//s->col.rs = 0;
	s->col.cols = ckalloc(colc * sizeof(Tcl_Obj*));
	memset(s->col.cols, 0, colc * sizeof(Tcl_Obj*));
	for (i=0; i<colc; i++)
		replace_tclobj(&s->col.cols[i], colv[i]);
	//s->r = 0;
	s->datarowc = datarowc;
	s->datarowv = ckalloc(datarowc * sizeof(Tcl_Obj*));
	memset(s->datarowv, 0, datarowc * sizeof(Tcl_Obj*));
	for (i=0; i<datarowc; i++)
		replace_tclobj(&s->datarowv[i], datarowv[i]);
	s->colcount = colc;
	s->rowv = ckalloc(colc*2 * sizeof(Tcl_Obj*));
	memset(s->rowv, 0, colc*2 * sizeof(Tcl_Obj*));
	//s->row = NULL;
	replace_tclobj(&s->rowvar, objv[A_ROWVAR]);
	replace_tclobj(&s->script, objv[A_SCRIPT]);
	s->ops = ckalloc(colc * sizeof(col_op*));
	s->delimc = delimc;
	s->delimv = delimv;

	TEST_OK_LABEL(err, code, compile_ops(interp, objv[A_OPS], s->ops, colc));

	return c_foreach_batch_nr_loop_top(interp, s);

err:
	if (s) {
		free_foreach_state(s);
		s = NULL;
	}

	return code;
}

//>>>
static int c_foreach_batch_nr_loop_top(Tcl_Interp* interp, struct foreach_state* s) //<<<
{
	int				code = TCL_OK;
	int				data_len, c;
	unsigned char*	data = Tcl_GetByteArrayFromObj(s->datarowv[s->r], &data_len);
	unsigned char*	p = data+2;

	s->col.rs = 0;

	/*
	// per datarow:

	if (data_len < 4) {
		Tcl_SetObjResult(interp, Tcl_ObjPrintf("data is too short: %d", data_len));
		code = TCL_ERROR;
		goto finally;
	}

	colcount = bswap_16(*(int16_t*)p); p+=2;
	if (colcount != c_types_len/3) {
		Tcl_SetObjResult(interp, Tcl_ObjPrintf("data claims %d columns, but c_types describes only %d", colcount, c_types_len/3));
		code = TCL_ERROR;
		goto finally;
	}
	*/

	//fprintf(stderr, "data_len: %d\n", data_len);
	for (c=0; c < s->colcount; c++) {
		const int	collen = bswap_32(*(int32_t*)p);
		//const int	old_rs = s->col.rs;

		//fprintf(stderr, "-> c: %d, rs: %d, p-data: %d, collen: %d, rowv[%d]: %p, rowv[%d]: %p\n", c, s->col.rs, p-data, collen, s->col.rs, s->rowv[s->col.rs], s->col.rs+1, s->rowv[s->col.rs+1]);
		p += 4;
		TEST_OK_LABEL(finally, code, s->ops[c](interp, c, collen, &p, &s->col, s->rowv, s->delimv[c]));
		//fprintf(stderr, "<- c: %d, rs: %d, p-data: %d, collen: %d, rowv[%d]: %p, rowv[%d]: %p\n", c, s->col.rs, p-data, collen, old_rs, s->rowv[old_rs], old_rs+1, s->rowv[old_rs+1]);
	}

	if (s->col.rs >= 0) {
		// The "vars" op handler sets cols.rs to -1 to signal that we don't have a rowvar to set
		replace_tclobj(&s->row, Tcl_NewListObj(s->col.rs, s->rowv));

		if (NULL == Tcl_ObjSetVar2(interp, s->rowvar, NULL, s->row, TCL_LEAVE_ERR_MSG)) {
			code = TCL_ERROR;
			goto finally;
		}

		//fprintf(stderr, "running script for row %s:\n%s\n", Tcl_GetString(s->row), Tcl_GetString(s->script));
	}

	Tcl_NRAddCallback(interp, c_foreach_batch_nr_loop_bot, s, NULL, NULL, NULL);
	return Tcl_NREvalObj(interp, s->script, 0);

finally:
	if (s) {
		free_foreach_state(s);
		s = NULL;
	}

	return code;
}

//>>>
static int c_foreach_batch_nr_loop_bot(ClientData cdata[], Tcl_Interp* interp, int result) //<<<
{
	struct foreach_state*	s = cdata[0];
	int						code = TCL_OK;

	switch (result) {
		case TCL_OK:
		case TCL_CONTINUE:
			goto checkloop;
		default:
			code = result;
			goto done;
	}

checkloop:
	s->r++;
	if (s->r < s->datarowc) {
		//fprintf(stderr, "checkloop, looping: r: %d, datarowc: %d\n", s->r, s->datarowc);
		return c_foreach_batch_nr_loop_top(interp, s);
	}

done:
	//fprintf(stderr, "done\n");
	if (s) {
		free_foreach_state(s);
		s = NULL;
	}
	//fprintf(stderr, "returning %d\n", code);
	return code;
}

//>>>
OBJCMD(c_allrows_batch) //<<<
{
	int					code = TCL_OK;
	Tcl_Obj**			datarowv = NULL;
	int					datarowc;
	Tcl_Obj**			colv = NULL;
	int					colc;
	Tcl_Encoding		encoding;
	Tcl_Obj*			rows = NULL;	// Loaned ref
	Tcl_Obj*			lrows = NULL;
	col_op**			ops = NULL;
	Tcl_Obj**			rowv = NULL;
	Tcl_Obj**			delimv = NULL;
	int					delimc;

	enum {A_cmd, A_ROWSVAR, A_OPS, A_COLS, A_ENCODING, A_DATAROWS, A_DELIMS, A_objc};
	CHECK_ARGS_LABEL(finally, code, "rowsvar ops columns tcl_encoding datarows delims");

	TEST_OK_LABEL(finally, code, Tcl_ListObjGetElements(interp, objv[A_COLS], &colc, &colv));
	TEST_OK_LABEL(finally, code, Tcl_GetEncodingFromObj(interp, objv[A_ENCODING], &encoding));
	TEST_OK_LABEL(finally, code, Tcl_ListObjGetElements(interp, objv[A_DATAROWS], &datarowc, &datarowv));
	if (datarowc == 0) return TCL_OK;
	TEST_OK_LABEL(finally, code, Tcl_ListObjGetElements(interp, objv[A_DELIMS], &delimc, &delimv));

	/* Retrieve the existing value from $rowsvar and ensure it's unshared */
	rows = Tcl_ObjGetVar2(interp, objv[A_ROWSVAR], NULL, 0);
	if (rows == NULL) {
		replace_tclobj(&lrows, Tcl_NewListObj(0, NULL));
		rows = lrows;
	} else if (Tcl_IsShared(rows)) {
		replace_tclobj(&lrows, Tcl_DuplicateObj(rows));
		rows = lrows;
	}

	ops  =  (col_op**)malloc(sizeof(col_op*) * colc);
	rowv = (Tcl_Obj**)malloc(sizeof(Tcl_Obj*) * colc * 2);
	memset(rowv, 0, sizeof(Tcl_Obj*)*colc*2);
	{
		const int			colcount = colc;
		int					r;
		struct column_cx	col;

		TEST_OK_LABEL(finally, code, compile_ops(interp, objv[A_OPS], ops, colcount));

		col.encoding = encoding;
		col.cols = colv;

		for (r=0; r<datarowc; r++) {
			int				data_len, c;
			unsigned char*	data = Tcl_GetByteArrayFromObj(datarowv[r], &data_len);
			unsigned char*	p = data+2;

			col.rs = 0;

			for (c=0; c<colcount; c++) {
				const int	collen = bswap_32(*(int32_t*)p);

				p += 4;
				TEST_OK_LABEL(finally, code, ops[c](interp, c, collen, &p, &col, rowv, delimv[c]));
			}

			if (col.rs >= 0)
				TEST_OK_LABEL(finally, code, Tcl_ListObjAppendElement(interp, rows, Tcl_NewListObj(col.rs, rowv)));
		}
	}


	/*
	// per datarow:

	if (data_len < 4) {
		Tcl_SetObjResult(interp, Tcl_ObjPrintf("data is too short: %d", data_len));
		code = TCL_ERROR;
		goto finally;
	}

	colcount = bswap_16(*(int16_t*)p); p+=2;
	if (colcount != c_types_len/3) {
		Tcl_SetObjResult(interp, Tcl_ObjPrintf("data claims %d columns, but c_types describes only %d", colcount, c_types_len/3));
		code = TCL_ERROR;
		goto finally;
	}
	*/

	Tcl_SetObjResult(interp, rows);

	if (NULL == Tcl_ObjSetVar2(interp, objv[A_ROWSVAR], NULL, rows, TCL_LEAVE_ERR_MSG)) {
		code = TCL_ERROR;
		goto finally;
	}

finally:
	rows = NULL;
	replace_tclobj(&lrows, NULL);
	if (ops) { free(ops); ops = NULL; }
	if (rowv) {
		int i;
		for (i=0; i<colc*2; i++)
			replace_tclobj(&rowv[i], NULL);
		free(rowv);
		rowv = NULL;
	}
	return code;
}

//>>>
OBJCMD(c_makerow2) //<<<
{
	int					code = TCL_OK;
	Tcl_Obj**			colv = NULL;
	int					colc;
	Tcl_Encoding		encoding;
	unsigned char*		data = NULL;
	int					data_len;
	col_op**			ops = NULL;
	Tcl_Obj**			rowv = NULL;
	Tcl_Obj**			delimv = NULL;
	int					delimc;

	enum {A_cmd, A_OPS, A_COLS, A_ENCODING, A_DATAROW, A_DELIMS, A_objc};
	CHECK_ARGS_LABEL(finally, code, "ops columns tcl_encoding datarow delims");

	TEST_OK_LABEL(finally, code, Tcl_ListObjGetElements(interp, objv[A_COLS], &colc, &colv));
	TEST_OK_LABEL(finally, code, Tcl_GetEncodingFromObj(interp, objv[A_ENCODING], &encoding));
	data = Tcl_GetByteArrayFromObj(objv[A_DATAROW], &data_len);
	TEST_OK_LABEL(finally, code, Tcl_ListObjGetElements(interp, objv[A_DELIMS], &delimc, &delimv));

	ops = (col_op**)malloc(sizeof(col_op*) * colc);
	rowv = (Tcl_Obj**)malloc(sizeof(Tcl_Obj*) * colc * 2);
	memset(rowv, 0, sizeof(Tcl_Obj*)*colc*2);
	{
		const int			colcount = colc;
		struct column_cx	col;
		int					c;
		unsigned char*		p = data+2;

		TEST_OK_LABEL(finally, code, compile_ops(interp, objv[A_OPS], ops, colcount));

		col.encoding = encoding;
		col.cols = colv;
		col.rs = 0;

		//fprintf(stderr, "data_len: %d\n", data_len);
		for (c=0; c<colcount; c++) {
			const int	collen = bswap_32(*(int32_t*)p);

			//fprintf(stderr, "c: %d, rs: %d, p-data: %d, collen: %d\n", c, col.rs, p-data, collen);
			p += 4;
			TEST_OK_LABEL(finally, code, ops[c](interp, c, collen, &p, &col, rowv, delimv[c]));
		}

		// The "vars" op handler sets cols.rs to -1 to signal that we don't have a rowvar to set
		if (col.rs >= 0)
			Tcl_SetObjResult(interp, Tcl_NewListObj(col.rs, rowv));
	}

finally:
	if (ops) { free(ops); ops = NULL; }
	if (rowv) {
		int i;
		for (i=0; i<colc*2; i++) replace_tclobj(&rowv[i], NULL);
		free(rowv);
		rowv = NULL;
	}
	return code;
}

//>>>
OBJCMD(xor) //<<<
{
	int			code = TCL_OK;
	Tcl_Obj*	res = NULL;

	enum {A_cmd, A_A, A_B, A_objc};
	CHECK_ARGS_LABEL(finally, code, "a b");

	int		a_len, b_len;
	#ifdef Tcl_GetBytesFromObj
	const uint8_t*	a = (const uint8_t*)Tcl_GetBytesFromObj(interp, objv[A_A], &a_len);
	if (!a) {code = TCL_ERROR; goto finally;}
	const uint8_t*	b = (const uint8_t*)Tcl_GetBytesFromObj(interp, objv[A_B], &b_len);
	if (!b) {code = TCL_ERROR; goto finally;}
	#else
	const uint8_t*	a = Tcl_GetByteArrayFromObj(objv[A_A], &a_len);
	const uint8_t*	b = Tcl_GetByteArrayFromObj(objv[A_B], &b_len);
	#endif

	if (a_len != b_len) THROW_ERROR_LABEL(finally, code, "a and b must be the same length");

	replace_tclobj(&res, Tcl_NewByteArrayObj(NULL, a_len));
	#ifdef Tcl_GetBytesFromObj
	uint8_t*restrict	r = (uint8_t*)Tcl_GetBytesFromObj(interp, res, NULL);
	#else
	uint8_t*restrict	r = (uint8_t*)Tcl_GetByteArrayFromObj(res, NULL);
	#endif

	size_t	c = a_len;
	while (c--) *r++ = *a++ ^ *b++;

	Tcl_InvalidateStringRep(res);
	Tcl_SetObjResult(interp, res);

finally:
	replace_tclobj(&res, NULL);
	return code;
}

//>>>

// vim: foldmethod=marker foldmarker=<<<,>>> ts=4 shiftwidth=4
