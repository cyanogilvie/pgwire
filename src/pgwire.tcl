package require Thread

apply {{} {
	# Preserve tuneables from previous loads when re-sourcing this, or set before we are loaded <<<
	set preserved	{}
	foreach v {
		block_accelerators
		default_batchsize
		include_path
	} {
		if {[info exists ::pgwire::$v]} {
			lappend preserved $v [set ::pgwire::$v]
		}
	}
	#>>>

	if {[info object isa object ::pgwire]} {
		::pgwire destroy
	}

	if {[namespace exists ::pgwire]} {
		namespace delete ::pgwire
	}
	namespace eval ::pgwire {}
	foreach {v val} $preserved {
		set ::pgwire::$v	$val
	}
}}
namespace eval ::pgwire { #<<<
	if {[info commands ::ns_log] ne ""} {
		proc log {lvl msg} {
			ns_log $lvl $msg
		}
	} else {
		proc log {lvl msg} {
			puts $msg
		}
	}

	if {![info exists default_batchsize]} {
		variable default_batchsize	1000
	}
	if {![info exists include_path]} {
		variable include_path	/usr/include
	}

	namespace eval tapchan {
		namespace export *
		namespace ensemble create -prefixes no

		# msgtypes lookup <<<
		variable msgtypes {
			frontend {
				d	CopyData
				c	CopyDone
				B	Bind
				C	Close
				f	CopyFail
				D	Describe
				E	Execute
				H	Flush
				F	FunctionCall
				P	Parse
				p	PasswordMessage
				Q	Query
				S	Sync
				X	Terminate
			}
			backend {
				R	Authentication*
				R0	AuthenticationOk
				R2	AuthenticationKerberosV5
				R3	AuthenticationCleartextPassword
				R5	AuthenticationMD5Password
				R6	AuthenticationSCMCredential
				R7	AuthenticationGSS
				R8	AuthenticationGSSContinue
				R9	AuthenticationSSPI
				K	BackendKeyData
				2	BindComplete
				3	CloseComplete
				C	CommandComplete
				G	CopyInResponse
				H	CopyOutResponse
				D	DataRow
				I	EmptyQueryResponse
				E	ErrorResponse
				V	FunctionCallResponse
				n	NoData
				N	NoticeResponse
				A	NotificationResponse
				t	ParameterDescription
				S	ParameterStatus
				1	ParseComplete
				s	PortalSuspended
				Z	ReadyForQuery
				T	RowDescription
				d	CopyData
				c	CopyDone
			}
		}
		# msgtypes lookup >>>

		proc initialize {chan mode} { #<<<
			::pgwire::log notice "pgwire tapchan $chan initialize $mode"
			return {initialize finalize read write flush drain clean}
		}

		#>>>
		proc clear chan { ::pgwire::log notice "pgwire tapchan $chan clear" }
		proc finalize chan { ::pgwire::log notice "pgwire tapchan $chan finalize" }
		proc read {chan bytes} { #<<<
			::pgwire::log notice "pgwire tapchan $chan read [binary encode base64 $bytes]"
			decode backend $bytes
			::pgwire::log notice "pgwire tapchan $chan read passing on bytes"
			set bytes
		}

		#>>>
		proc write {chan bytes} { #<<<
			::pgwire::log notice "pgwire tapchan $chan write [binary encode base64 $bytes]"
			decode frontend $bytes
			::pgwire::log notice "pgwire tapchan $chan write passing on bytes"
			set bytes
		}

		#>>>
		proc flush chan { #<<<
			::pgwire::log notice "pgwire tapchan $chan flush"
		}

		#>>>
		proc drain chan { #<<<
			::pgwire::log notice "pgwire tapchan $chan drain"
		}

		#>>>
		proc decode {from bytes} { #<<<
			variable msgtypes

			set i	0
			while {$i < [string length $bytes]} {
				binary scan $bytes @${i}aI msgtype len
				if {$msgtype eq "\u0"} {
					# Probably the startup message
					binary scan $bytes @${i}I len
					incr i 4
					incr len -4		;# len includes itself
					binary scan $bytes @${i}a${len} payload
					binary scan $payload Ia* ver fields
					incr i $len
					::pgwire::log notice "hello message, version: [expr {$ver >> 16}].[expr {$ver & 0xff}], fields:\n[join [lmap {k v} [split [string trimright $fields \u0] \u0] {format {	%30s: %s} $k $v}] \n]"
					continue
				} else {
					incr i	5
					incr len -4		;# len includes itself
					binary scan $bytes @${i}a${len} data
					incr i $len
				}
				try {
					dict get $msgtypes $from $msgtype
				} trap {TCL LOOKUP DICT} {errmsg options} {
					::pgwire::log error "Invalid msgtype: \"$msgtype\", [binary encode hex $msgtype], probably a sync issue"
				} on ok messagename {
					::pgwire::log notice "pgwire tapchan from $from ($msgtype) $messagename"
				}
			}
		}

		#>>>
	}

	# accel_ops: generate c code for each combination of type, format and null handling, and a function "compile_ops" that takes a Tcl list of ops and populates an array of addresses to those op functions <<<
	proc build_ops {format c_types} { #<<<
		lmap {column type} $c_types {
			return -level 0 op_${type}_${format}
		}
	}

	#>>>
	set accel_ops	[apply {{} {
		set res	{}

		set opargs	{const int colnum, const int collen, unsigned char** data, struct column_cx* c, Tcl_Obj* rowv[], struct interp_cx* l}
		append res "typedef void (col_op)($opargs);\n"

		set opnames	{}
		foreach {format add_column_key handle_null} {
			lists {
			} {
				val = l->lit[PGWIRE_LIT_BLANK];
			}

			dicts {
				rowv[c->rs++] = c->cols[colnum];
			} {
				return;
			}

			dicts_no_nulls {
				rowv[c->rs++] = c->cols[colnum];
			} {
				val = l->lit[PGWIRE_LIT_BLANK];
			}
		} { 
			foreach {type makeval} {
				bool	{
					// TODO: check that this is in fact 1 byte
					val = l->lit[(*(unsigned char*)(*data) == 0) ? PGWIRE_LIT_FALSE : PGWIRE_LIT_TRUE];
				}
				int1	{val = Tcl_NewIntObj(*(char*)(*data));}
				int2	{val = Tcl_NewIntObj(bswap_16(*(int16_t*)(*data)));}
				int4	{val = Tcl_NewIntObj(bswap_32(*(int32_t*)(*data)));}
				int8	{val = Tcl_NewIntObj(bswap_64(*(int64_t*)(*data)));}
				bytea	{val = Tcl_NewByteArrayObj(*data, collen);}
				float4	{
					{
						char buf[4];

						*(uint32_t*)buf = bswap_32(*(uint32_t*)(*data));
						val = Tcl_NewDoubleObj(*(float*)buf);
					}
				}
				float8 {
					{
						char buf[8];

						*(uint64_t*)buf = bswap_64(*(uint64_t*)(*data));
						val = Tcl_NewDoubleObj(*(double*)buf);
					}
				}
				text {
					{
						Tcl_DString		utf8;

						if (collen == 0) {
							val = l->lit[PGWIRE_LIT_BLANK];
						} else {
							Tcl_DStringInit(&utf8);
							Tcl_ExternalToUtfDString(c->encoding, *data, collen, &utf8);
							val = Tcl_NewStringObj(Tcl_DStringValue(&utf8), Tcl_DStringLength(&utf8));
							Tcl_DStringFree(&utf8);
						}
					}
				}
			} {
				set opname	op_${type}_${format}
				lappend opnames	$opname
				append res [string map [list \
					%format%			$format \
					%handle_null%		$handle_null \
					%add_column_key%	$add_column_key \
					%makeval%			$makeval \
					%opname%			$opname \
					%opargs%			$opargs \
				] {
					static void %opname%(%opargs%) //<<<
					{
						Tcl_Obj*	val;

						if (collen != -1) {
							%makeval%
							*data += collen;
						} else {
							%handle_null%
						}

						%add_column_key%
						rowv[c->rs++] = val;
					}

					//>>>
				}]
			}
		}

		set opname_strings	[join [lmap opname $opnames {
			format {"%s"} $opname
		}] {,
	  				}]
		set opname_addrs	[join [lmap opname $opnames {format %s $opname}] {,
					}]
		append res [string map [list \
			%opname_strings%	$opname_strings \
			%opname_addrs%		$opname_addrs \
		] {
			int compile_ops(Tcl_Interp* interp, Tcl_Obj* ops, col_op* opv[], int expecting)
			{
				int			retcode = TCL_OK;
				Tcl_Obj**	ov = NULL;
				int			oc;
				static const char* opnames[] = {
					%opname_strings%,
					(char*)NULL
				};
				col_op*	opaddr[] = {
					%opname_addrs%
				};
				int opidx, i, opi = 0;

				if (TCL_OK != (retcode = Tcl_ListObjGetElements(interp, ops, &oc, &ov)))
					goto done;

				if (oc != expecting) {
					Tcl_SetObjResult(interp, Tcl_ObjPrintf("Expecting %d ops, got %d", expecting, oc));
					retcode = TCL_ERROR;
					goto done;
				}

				for (i=0; i<oc; i++) {
					if (TCL_OK != (retcode = Tcl_GetIndexFromObj(interp, ov[i], opnames, "op", TCL_EXACT, &opidx)))
						goto done;

					opv[opi++] = opaddr[opidx];
				}

done:
				return retcode;
			}

			/* Testing only */
			int compile_ops_cmd(ClientData cdata, Tcl_Interp* interp, int objc, Tcl_Obj *const objv[])
			{
				int			retcode = TCL_OK;
				int			expecting;

				if (objc != 3) {
					Tcl_WrongNumArgs(interp, 1, objv, "ops expecting");
					retcode = TCL_ERROR;
					goto done;
				}

				if (TCL_OK != (retcode = Tcl_GetIntFromObj(interp, objv[2], &expecting)))
					goto done;

				{
					col_op*	ops[expecting];

					if (TCL_OK != (retcode = compile_ops(interp, objv[1], ops, expecting)))
						goto done;
				}

			done:
				return retcode;
			}
		}]

		set res
	}}]
	#>>>
	# C code <<<
	set c_code	[string map [list \
		%accel_ops%	$::pgwire::accel_ops \
	] {
		#include <byteswap.h>
		#include <stdint.h>
		#include <string.h>

		struct column_cx {
			Tcl_Encoding	encoding;
			int				rs;
			Tcl_Obj**		cols;
		};

		enum {
			PGWIRE_LIT_BLANK,
			PGWIRE_LIT_ONE,
			PGWIRE_LIT_ZERO,
			PGWIRE_LIT_TRUE,
			PGWIRE_LIT_FALSE,

			PGWIRE_LIT_SIZE
		};
		const char* lit_vals[] = {
			"",
			"1",
			"0",
			"1",
			"0"
		};

		struct interp_cx {
			Tcl_Obj*	lit[PGWIRE_LIT_SIZE];
		};

		%accel_ops%

		struct foreach_state {
			struct column_cx	col;
			int					r;
			int					datarowc;
			Tcl_Obj**			datarowv;
			int					colcount;
			Tcl_Obj**			rowv;
			struct interp_cx*	l;
			Tcl_Obj*			row;
			Tcl_Obj*			rowvar;
			Tcl_Obj*			script;
			col_op**			ops;
		};

		int c_makerow(ClientData cdata, Tcl_Interp* interp, int objc, Tcl_Obj *const objv[]) //<<<
		{
			const char* restrict		data = NULL;
			int							data_len;
			Tcl_Obj**					c_types = NULL;
			int							c_types_len;
			Tcl_Encoding				encoding;
			int							i;
			const char* restrict		p = NULL;
			const char* restrict		e = NULL;
			uint16_t					colcount;
			Tcl_Obj*					row = NULL;
			int							retcode = TCL_OK;
			static const char* types[] = {
				"bool",
				"int1",
				"smallint",
				"int2",
				"int4",
				"integer",
				"bigint",
				"int8",
				"bytea",
				"float4",
				"float8",
				"varchar",
				"text",
				(char*)NULL
			};
			int type_idx;
			enum {
				TYPE_BOOL,
				TYPE_INT1,
				TYPE_SMALLINT,
				TYPE_INT2,
				TYPE_INT4,
				TYPE_INTEGER,
				TYPE_BIGINT,
				TYPE_INT8,
				TYPE_BYTEA,
				TYPE_FLOAT4,
				TYPE_FLOAT8,
				TYPE_VARCHAR,
				TYPE_TEXT
			};
			static const char* formats[] = {
				"dicts",
				"lists",
				"dicts_no_nulls",
				(char*)NULL
			};
			int format;
			enum {
				FORMAT_DICTS,
				FORMAT_LISTS,
				FORMAT_DICTS_NO_NULLS
			};

			if (objc != 5) {
				Tcl_WrongNumArgs(interp, 1, objv, "data c_types tcl_encoding format");
				return TCL_ERROR;
			}

			p = data = Tcl_GetByteArrayFromObj(objv[1], &data_len);
			e = p + data_len;
			if (Tcl_ListObjGetElements(interp, objv[2], &c_types_len, &c_types) != TCL_OK)
				return TCL_ERROR;
			if (Tcl_GetEncodingFromObj(interp, objv[3], &encoding) != TCL_OK)
				return TCL_ERROR;
			if (Tcl_GetIndexFromObj(interp, objv[4], formats, "format", TCL_EXACT, &format) != TCL_OK)
				return TCL_ERROR;

			if (c_types_len % 2 != 0) {
				Tcl_SetObjResult(interp, Tcl_ObjPrintf("c_types must be an even number of elements"));
				return TCL_ERROR;
			}

			if (data_len < 4) {
				Tcl_SetObjResult(interp, Tcl_ObjPrintf("data is too short: %d", data_len));
				return TCL_ERROR;
			}

			colcount = bswap_16(*(uint16_t*)p); p+=2;
			if (colcount != c_types_len/2) {
				Tcl_SetObjResult(interp, Tcl_ObjPrintf("data claims %d columns, but c_types describes only %d", colcount, c_types_len/2));
				return TCL_ERROR;
			}

			{
				const int	rslots = colcount * (format == FORMAT_LISTS ? 1 : 2);
				Tcl_Obj**	rowv = NULL;
				int			c;		/* col num */
				int			rs;		/* rowv slot */
				Tcl_Obj*	nullobj = NULL;

				rowv = ckalloc(sizeof(Tcl_Obj*)*rslots);

				Tcl_IncrRefCount(nullobj = Tcl_NewObj());	/* TODO: store a static null obj in an interp AssocData */

				/* Zero the pointers, so that we don't leak partial rows of Tcl_Objs on error */
				memset(rowv, 0, sizeof(Tcl_Obj*) * rslots);

				for (i=0, c=1, rs=0; i<c_types_len; i+=2, c++) {
					const int collen = bswap_32(*(uint32_t*)p);

					p += 4;

					if (collen == -1) {
						/* NULL */
						switch (format) {
							case FORMAT_DICTS_NO_NULLS:
								Tcl_IncrRefCount(rowv[rs++] = c_types[i]);
								// Falls through
							case FORMAT_LISTS:
								Tcl_IncrRefCount(rowv[rs++] = nullobj);
						}
						continue;
					}

					if (e-p < collen) {
						Tcl_SetObjResult(interp, Tcl_ObjPrintf("Insufficient data for column %d: need %d but %ld remain", i/2, collen, e-p));
						retcode = TCL_ERROR;
						goto done;
					}

					if (format == FORMAT_DICTS || format == FORMAT_DICTS_NO_NULLS) {
						Tcl_IncrRefCount(rowv[rs++] = c_types[i]);
					}

					if (TCL_OK != (retcode = Tcl_GetIndexFromObj(interp, c_types[i+1], types, "type", TCL_EXACT, &type_idx)))
						goto done;

					switch (type_idx) {
						case TYPE_BOOL:
							// TODO: check that this is in fact 1 byte
							// TODO: instead of Tcl_NewBooleanObj, ref a static true or false obj?
							Tcl_IncrRefCount(rowv[rs++] = Tcl_NewBooleanObj(*p != 0));
							break;
						case TYPE_INT1:
							Tcl_IncrRefCount(rowv[rs++] = Tcl_NewIntObj(*p));	// Signed?
							break;
						case TYPE_SMALLINT:
						case TYPE_INT2:
							Tcl_IncrRefCount(rowv[rs++] = Tcl_NewIntObj(bswap_16(*(int16_t*)p)));	// Signed?
							break;
						case TYPE_INT4:
						case TYPE_INTEGER:
							Tcl_IncrRefCount(rowv[rs++] = Tcl_NewIntObj(bswap_32(*(int32_t*)p)));	// Signed?
							break;
						case TYPE_BIGINT:
						case TYPE_INT8:
							Tcl_IncrRefCount(rowv[rs++] = Tcl_NewIntObj(bswap_64(*(int64_t*)p)));	// Signed?
							break;
						case TYPE_BYTEA:
							Tcl_IncrRefCount(rowv[rs++] = Tcl_NewByteArrayObj(p, collen));
							break;
						case TYPE_FLOAT4:
							{
								char buf[4];
								*(int32_t*)buf = bswap_32(*(int32_t*)p);
								Tcl_IncrRefCount(rowv[rs++] = Tcl_NewDoubleObj(*(float*)buf));
							}
							break;
						case TYPE_FLOAT8:
							{
								char buf[8];
								*(int64_t*)buf = bswap_64(*(int64_t*)p);
								Tcl_IncrRefCount(rowv[rs++] = Tcl_NewDoubleObj(*(double*)buf));
							}
							break;
						case TYPE_VARCHAR:
						case TYPE_TEXT:
							{
								Tcl_DString		utf8;

								Tcl_DStringInit(&utf8);

								Tcl_ExternalToUtfDString(encoding, p, collen, &utf8);
								Tcl_IncrRefCount(rowv[rs++] = Tcl_NewStringObj(Tcl_DStringValue(&utf8), Tcl_DStringLength(&utf8)));
								Tcl_DStringFree(&utf8);
							}
							break;
						default:
							Tcl_SetObjResult(interp, Tcl_ObjPrintf("Unrecognised type \"%s\" for column \"%s\"", Tcl_GetString(c_types[i+1]), Tcl_GetString(c_types[i])));
							retcode = TCL_ERROR;
							goto done;
					}
					p += collen;
				}

				Tcl_SetObjResult(interp, Tcl_NewListObj(rs, rowv));

done:
				while (--rs >= 0) {
					if (rowv[rs] != NULL) {
						Tcl_DecrRefCount(rowv[rs]);
						rowv[rs] = NULL;
					}
				}
				if (nullobj) {
					Tcl_DecrRefCount(nullobj); nullobj = NULL;
				}
				if (rowv) {
					ckfree(rowv); rowv = NULL;
				}
				return retcode;
			}
		}
		//>>>
		void free_interp_cx(ClientData cdata, Tcl_Interp* interp) //<<<
		{
			struct interp_cx*	l = (struct interp_cx*)cdata;
			int					i;

			if (l) {
				for (i=0; i<PGWIRE_LIT_SIZE; i++) {
					if (l->lit[i]) {
						Tcl_DecrRefCount(l->lit[i]);
						l->lit[i] = NULL;
					}
				}

				ckfree(l); l = NULL;
			}
		}

		//>>>
		struct interp_cx* get_interp_cx(Tcl_Interp* interp) //<<<
		{
			struct interp_cx*	l = Tcl_GetAssocData(interp, "pgwire", NULL);
			int					i;

			if (l == NULL) {
				l = ckalloc(sizeof *l);
				for (i=0; i<PGWIRE_LIT_SIZE; i++)
					Tcl_IncrRefCount(l->lit[i] = Tcl_NewStringObj(lit_vals[i], -1));
				Tcl_SetAssocData(interp, "pgwire", free_interp_cx, l);
			}

			return l;
		}

		//>>>
		static void free_foreach_state(struct foreach_state* s) //<<<
		{
			int		i;

			//fprintf(stderr, "freeing foreach_state %p\n", s);
			if (s) {
				if (s->col.cols) {
					for (i=0; i < s->colcount; i++) {
						if (s->col.cols[i]) {
							Tcl_DecrRefCount(s->col.cols[i]); s->col.cols[i] = NULL;
						}
					}
					ckfree(s->col.cols); s->col.cols = NULL;
				}

				if (s->datarowv) {
					for (i=0; i < s->datarowc; i++) {
						Tcl_DecrRefCount(s->datarowv[i]); s->datarowv[i] = NULL;
					}
					ckfree(s->datarowv); s->datarowv = NULL;
				}

				if (s->rowv) {
					// TODO: somehow know which entries in rowv we should decref?
					ckfree(s->rowv); s->rowv = NULL;
				}

				s->l = NULL;

				if (s->row) {
					Tcl_DecrRefCount(s->row); s->row = NULL;
				}

				if (s->rowvar) {
					Tcl_DecrRefCount(s->rowvar); s->rowvar = NULL;
				}

				if (s->script) {
					Tcl_DecrRefCount(s->script); s->script = NULL;
				}

				if (s->ops) {
					ckfree(s->ops); s->ops = NULL;
				}

				ckfree(s); s = NULL;
			}
		}

		//>>>
		int c_foreach_batch_nr_setup(ClientData cdata, Tcl_Interp* interp, int objc, Tcl_Obj *const objv[]);
		int c_foreach_batch_nr_loop_top(Tcl_Interp* interp, struct foreach_state* s);
		int c_foreach_batch_nr_loop_bot(ClientData cdata[], Tcl_Interp* interp, int result);
		int c_foreach_batch_nr(ClientData cdata, Tcl_Interp* interp, int objc, Tcl_Obj *const objv[]) //<<<
		{
			//fprintf(stderr, "c_foreach_batch_nr\n");
			return Tcl_NRCallObjProc(interp, c_foreach_batch_nr_setup, cdata, objc, objv);
		}

		//>>>
		int c_foreach_batch_nr_setup(ClientData cdata, Tcl_Interp* interp, int objc, Tcl_Obj *const objv[]) //<<<
		{
			struct foreach_state*	s = NULL;
			int					retcode = TCL_OK;
			Tcl_Obj**			datarowv = NULL;
			int					datarowc;
			Tcl_Obj*			script = NULL;
			Tcl_Obj**			colv = NULL;
			int					colc;
			struct interp_cx*	l = get_interp_cx(interp);
			Tcl_Obj*			rowvar = NULL;
			Tcl_Encoding		encoding;
			int					i;
			int					ops_len;

			//fprintf(stderr, "c_foreach_batch_nr_setup\n");
			if (objc != 7) {
				Tcl_WrongNumArgs(interp, 1, objv, "rowvar ops columns tcl_encoding datarows script");
				return TCL_ERROR;
			}

			rowvar = objv[1];
			if (Tcl_ListObjGetElements(interp, objv[3], &colc, &colv) != TCL_OK)
				return TCL_ERROR;
			if (Tcl_GetEncodingFromObj(interp, objv[4], &encoding) != TCL_OK)
				return TCL_ERROR;
			if (Tcl_ListObjGetElements(interp, objv[5], &datarowc, &datarowv) != TCL_OK)
				return TCL_ERROR;

			if (datarowc == 0)
				return TCL_OK;

			Tcl_IncrRefCount(script = objv[6]);

			s = ckalloc(sizeof(*s));
			memset(s, 0, sizeof(*s));

			s->col.encoding = encoding;
			//s->col.rs = 0;
			s->col.cols = ckalloc(colc * sizeof(Tcl_Obj*));
			for (i=0; i<colc; i++)
				Tcl_IncrRefCount(s->col.cols[i] = colv[i]);
			//s->r = 0;
			s->datarowc = datarowc;
			s->datarowv = ckalloc(datarowc * sizeof(Tcl_Obj*));
			for (i=0; i<datarowc; i++)
				Tcl_IncrRefCount(s->datarowv[i] = datarowv[i]);
			s->colcount = colc;
			s->rowv = ckalloc(colc*2 * sizeof(Tcl_Obj*));
			memset(s->rowv, 0, colc*2 * sizeof(Tcl_Obj*));
			s->l = l;
			//s->row = NULL;
			Tcl_IncrRefCount(s->rowvar = rowvar);
			Tcl_IncrRefCount(s->script = script);
			s->ops = ckalloc(colc * sizeof(col_op*));
			if (TCL_OK != (retcode = compile_ops(interp, objv[2], s->ops, colc)))
				goto err;

			return c_foreach_batch_nr_loop_top(interp, s);

		err:
			if (s) {
				free_foreach_state(s); s = NULL;
			}

			return retcode;
		}

		//>>>
		int c_foreach_batch_nr_loop_top(Tcl_Interp* interp, struct foreach_state* s) //<<<
		{
			int				retcode = TCL_OK;
			int				data_len, c;
			unsigned char*	data = Tcl_GetByteArrayFromObj(s->datarowv[s->r], &data_len);
			unsigned char*	p = data+2;

			s->col.rs = 0;

			/*
			// per datarow:

			if (data_len < 4) {
				Tcl_SetObjResult(interp, Tcl_ObjPrintf("data is too short: %d", data_len));
				retcode = TCL_ERROR;
				goto done;
			}

			colcount = bswap_16(*(int16_t*)p); p+=2;
			if (colcount != c_types_len/2) {
				Tcl_SetObjResult(interp, Tcl_ObjPrintf("data claims %d columns, but c_types describes only %d", colcount, c_types_len/2));
				retcode = TCL_ERROR;
				goto done;
			}
			*/

			//fprintf(stderr, "data_len: %d\n", data_len);
			for (c=0; c < s->colcount; c++) {
				const int	collen = bswap_32(*(int32_t*)p);

				//fprintf(stderr, "c: %d, rs: %d, p-data: %d, collen: %d\n", c, s->col.rs, p-data, collen);
				p += 4;
				s->ops[c](c, collen, &p, &s->col, s->rowv, s->l);
			}

			if (s->row) Tcl_DecrRefCount(s->row);
			Tcl_IncrRefCount(s->row = Tcl_NewListObj(s->col.rs, s->rowv));

			if (NULL == Tcl_ObjSetVar2(interp, s->rowvar, NULL, s->row, TCL_LEAVE_ERR_MSG)) {
				retcode = TCL_ERROR;
				goto done;
			}

			//fprintf(stderr, "running script for row %s:\n%s\n", Tcl_GetString(s->row), Tcl_GetString(s->script));
			Tcl_NRAddCallback(interp, c_foreach_batch_nr_loop_bot, s, NULL, NULL, NULL);
			return Tcl_NREvalObj(interp, s->script, 0);

		done:
			if (s) {
				free_foreach_state(s); s = NULL;
			}

			return retcode;
		}

		//>>>
		int c_foreach_batch_nr_loop_bot(ClientData cdata[], Tcl_Interp* interp, int result) //<<<
		{
			struct foreach_state*	s = cdata[0];
			int						retcode = TCL_OK;

			switch (result) {
				case TCL_OK:
				case TCL_CONTINUE:
					goto checkloop;
				default:
					retcode = result;
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
				free_foreach_state(s); s = NULL;
			}
			//fprintf(stderr, "returning %d\n", retcode);
			return retcode;
		}

		//>>>
		int c_foreach_batch(ClientData cdata, Tcl_Interp* interp, int objc, Tcl_Obj *const objv[]) //<<<
		{
			int					retcode = TCL_OK;
			Tcl_Obj**			datarowv = NULL;
			int					datarowc;
			Tcl_Obj*			script = NULL;
			Tcl_Obj**			colv = NULL;
			int					colc;
			Tcl_Obj*			row = NULL;
			struct interp_cx*	l = get_interp_cx(interp);
			Tcl_Obj*			rowvar = NULL;
			Tcl_Encoding		encoding;

			if (objc != 7) {
				Tcl_WrongNumArgs(interp, 1, objv, "rowvar ops columns tcl_encoding datarows script");
				return TCL_ERROR;
			}

			rowvar = objv[1];

			if (Tcl_ListObjGetElements(interp, objv[3], &colc, &colv) != TCL_OK)
				return TCL_ERROR;
			if (Tcl_GetEncodingFromObj(interp, objv[4], &encoding) != TCL_OK)
				return TCL_ERROR;
			if (Tcl_ListObjGetElements(interp, objv[5], &datarowc, &datarowv) != TCL_OK)
				return TCL_ERROR;
			Tcl_IncrRefCount(script = objv[6]);

			{
				const int			colcount = colc;
				col_op*				ops[colcount];		// Is the concern about stack consumption here for a very large number of columns valid?
				Tcl_Obj*			rowv[colcount*2];	// *2: dict format can use up to 2 slots per col
				int					r;
				struct column_cx	col;

				if (TCL_OK != (retcode = compile_ops(interp, objv[2], ops, colcount)))
					goto done;

				col.encoding = encoding;
				col.cols = colv;

				for (r=0; r<datarowc; r++) {
					int				data_len, c;
					unsigned char*	data = Tcl_GetByteArrayFromObj(datarowv[r], &data_len);
					unsigned char*	p = data+2;

					col.rs = 0;

					//fprintf(stderr, "data_len: %d\n", data_len);
					for (c=0; c<colcount; c++) {
						const int	collen = bswap_32(*(int32_t*)p);

						//fprintf(stderr, "c: %d, rs: %d, p-data: %d, collen: %d\n", c, col.rs, p-data, collen);
						p += 4;
						ops[c](c, collen, &p, &col, rowv, l);
					}

					if (row) Tcl_DecrRefCount(row);
					Tcl_IncrRefCount(row = Tcl_NewListObj(col.rs, rowv));

					if (NULL == Tcl_ObjSetVar2(interp, rowvar, NULL, row, TCL_LEAVE_ERR_MSG)) {
						retcode = TCL_ERROR;
						goto done;
					}

					//fprintf(stderr, "running script for row %s:\n%s\n", Tcl_GetString(row), Tcl_GetString(script));
					retcode = Tcl_EvalObjEx(interp, script, 0);
					switch (retcode) {
						case TCL_OK:
							break;
						case TCL_CONTINUE:
							retcode = TCL_OK;
							continue;
						default:
							goto done;
					}
				}
			}


			/*
			// per datarow:

			if (data_len < 4) {
				Tcl_SetObjResult(interp, Tcl_ObjPrintf("data is too short: %d", data_len));
				retcode = TCL_ERROR;
				goto done;
			}

			colcount = bswap_16(*(int16_t*)p); p+=2;
			if (colcount != c_types_len/2) {
				Tcl_SetObjResult(interp, Tcl_ObjPrintf("data claims %d columns, but c_types describes only %d", colcount, c_types_len/2));
				retcode = TCL_ERROR;
				goto done;
			}
			*/

done:
			if (script) {
				Tcl_DecrRefCount(script); script = NULL;
			}
			if (row) {
				Tcl_DecrRefCount(row); row = NULL;
			}
			return retcode;
		}

		//>>>
		int c_allrows_batch(ClientData cdata, Tcl_Interp* interp, int objc, Tcl_Obj *const objv[]) //<<<
		{
			int					retcode = TCL_OK;
			Tcl_Obj**			datarowv = NULL;
			int					datarowc;
			Tcl_Obj**			colv = NULL;
			int					colc;
			struct interp_cx*	l = get_interp_cx(interp);
			Tcl_Encoding		encoding;
			Tcl_Obj*			rows = NULL;

			if (objc != 6) {
				Tcl_WrongNumArgs(interp, 1, objv, "rowsvar ops columns tcl_encoding datarows");
				return TCL_ERROR;
			}

			if (Tcl_ListObjGetElements(interp, objv[3], &colc, &colv) != TCL_OK)
				return TCL_ERROR;
			if (Tcl_GetEncodingFromObj(interp, objv[4], &encoding) != TCL_OK)
				return TCL_ERROR;
			if (Tcl_ListObjGetElements(interp, objv[5], &datarowc, &datarowv) != TCL_OK)
				return TCL_ERROR;
			if (datarowc == 0)
				return TCL_OK;

			/* Retrieve the existing value from $rowsvar and ensure it's unshared */
			rows = Tcl_ObjGetVar2(interp, objv[1], NULL, 0);
			if (rows == NULL) {
				rows = Tcl_NewListObj(0, NULL);
			} else if (Tcl_IsShared(rows)) {
				rows = Tcl_DuplicateObj(rows);
			}

			{
				const int			colcount = colc;
				col_op*				ops[colcount];		// Is the concern about stack consumption here for a very large number of columns valid?
				Tcl_Obj*			rowv[colcount*2];	// *2: dict format can use up to 2 slots per col
				int					r;
				struct column_cx	col;

				if (TCL_OK != (retcode = compile_ops(interp, objv[2], ops, colcount)))
					goto done;

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
						ops[c](c, collen, &p, &col, rowv, l);
					}

					if (TCL_OK != (retcode = Tcl_ListObjAppendElement(interp, rows, Tcl_NewListObj(col.rs, rowv))))
						goto done;
				}
			}


			/*
			// per datarow:

			if (data_len < 4) {
				Tcl_SetObjResult(interp, Tcl_ObjPrintf("data is too short: %d", data_len));
				retcode = TCL_ERROR;
				goto done;
			}

			colcount = bswap_16(*(int16_t*)p); p+=2;
			if (colcount != c_types_len/2) {
				Tcl_SetObjResult(interp, Tcl_ObjPrintf("data claims %d columns, but c_types describes only %d", colcount, c_types_len/2));
				retcode = TCL_ERROR;
				goto done;
			}
			*/

			Tcl_SetObjResult(interp, rows);

			if (NULL == Tcl_ObjSetVar2(interp, objv[1], NULL, rows, TCL_LEAVE_ERR_MSG)) {
				retcode = TCL_ERROR;
				goto done;
			}

done:
			if (rows) {
				/* rows may have a 0 refcount, if we created a fresh obj, or duplicated one, and didn't make it to the end.  In that case we need to free it to avoid a leak */
				Tcl_IncrRefCount(rows);
				Tcl_DecrRefCount(rows); rows = NULL;
			}
			return retcode;
		}

		//>>>
		int c_makerow2(ClientData cdata, Tcl_Interp* interp, int objc, Tcl_Obj *const objv[]) //<<<
		{
			int					retcode = TCL_OK;
			Tcl_Obj**			colv = NULL;
			int					colc;
			struct interp_cx*	l = get_interp_cx(interp);
			Tcl_Encoding		encoding;
			unsigned char*		data = NULL;
			int					data_len;

			if (objc != 5) {
				Tcl_WrongNumArgs(interp, 1, objv, "ops columns tcl_encoding datarow");
				return TCL_ERROR;
			}

			if (Tcl_ListObjGetElements(interp, objv[2], &colc, &colv) != TCL_OK)
				return TCL_ERROR;
			if (Tcl_GetEncodingFromObj(interp, objv[3], &encoding) != TCL_OK)
				return TCL_ERROR;
			data = Tcl_GetByteArrayFromObj(objv[4], &data_len);

			{
				const int			colcount = colc;
				col_op*				ops[colcount];		// Is the concern about stack consumption here for a very large number of columns valid?
				Tcl_Obj*			rowv[colcount*2];	// *2: dict format can use up to 2 slots per col
				int					r;
				struct column_cx	col;
				int					c;
				unsigned char*		p = data+2;

				if (TCL_OK != (retcode = compile_ops(interp, objv[1], ops, colcount)))
					goto done;

				col.encoding = encoding;
				col.cols = colv;
				col.rs = 0;

				//fprintf(stderr, "data_len: %d\n", data_len);
				for (c=0; c<colcount; c++) {
					const int	collen = bswap_32(*(int32_t*)p);

					//fprintf(stderr, "c: %d, rs: %d, p-data: %d, collen: %d\n", c, col.rs, p-data, collen);
					p += 4;
					ops[c](c, collen, &p, &col, rowv, l);
				}

				Tcl_SetObjResult(interp, Tcl_NewListObj(col.rs, rowv));
			}

done:
			return retcode;
		}

		//>>>
	}]
	# C code >>>
	set lineno	0
	#::pgwire::log notice "c code:\n[join [lmap line [split $c_code \n] {format {%3d: %s} [incr lineno] $line}] \n]"
}
#>>>


set ::pgwire::accelerators	0
if {![info exists ::pgwire::block_accelerators]} {
	if {[llength [lsearch -all -inline -exact -not [package versions critcl] 0]] > 0} {
		try {
			rename ::interp ::pgwire::_interp
			try {
				proc ::interp {op args} {
					# Prevent critcl turning on interp frame debug (about 10% slowdown)
					switch -exact -- $op {
						debug return
					}
					tailcall ::pgwire::_interp $op {*}$args
				}

				package require critcl 3
			} finally {
				rename ::interp {}
				rename ::pgwire::_interp ::interp
			}
		} on error {} {
		} on ok ver {
			#::pgwire::log notice "Have critcl $ver"
			# With a little help from my friends (c) <<<
			if {[critcl::compiling]} {
				#::pgwire::log notice "critcl::compiling: [critcl::compiling]"
				foreach path $::pgwire::include_path {
					critcl::cheaders -I$path
				}
				#critcl::tcl [info tclversion]
				critcl::tcl 8.6
				#critcl::debug memory
				critcl::debug symbols
				critcl::cflags -O2 -g
				#critcl::cflags -O3 -march=native
				critcl::ccode $::pgwire::c_code;	set baseline	[dict get [info frame 0] line]; set lineno	[expr {$baseline-1}]
				#::pgwire::log notice "c code:\n[join [lmap line [split $::pgwire::c_code \n] {format {%3d: %s} [incr lineno] $line}] \n]"
				#critcl::ccommand ::pgwire::c_makerow           c_makerow
				critcl::ccommand ::pgwire::c_makerow2           c_makerow2
				critcl::ccommand ::pgwire::c_foreach_batch      c_foreach_batch
				critcl::ccommand ::pgwire::c_allrows_batch      c_allrows_batch
				critcl::ccommand ::pgwire::compile_ops          compile_ops_cmd
				critcl::cinit {
					Tcl_NRCreateCommand(interp, "::pgwire::c_foreach_batch_nr", c_foreach_batch_nr, c_foreach_batch_nr_setup, NULL, NULL);
				} {}

				# Force compile and load.  Not necessary, without it commands will lazy compile and load when called
				critcl::load
			}
			# With a little help from my friends (c) >>>
			set ::pgwire::accelerators	1
		}
	}
	if {!$::pgwire::accelerators} {
		try {
			package require Thread	;# Thread: tcc appears to have concurrency issues, so we need to ensure that we're only running in a single thread at at time
			package require tcc4tcl
		} on error {} {
		} on ok ver {
			tsv::lock pgwire {
				set start	[clock microseconds]
				#::pgwire::log notice "Have tcc4tcl $ver"
				# With a little help from my friends (c) <<<
				tcc4tcl $::tcc4tcl::dir memory tcc
				foreach path $::pgwire::include_path {
					tcc add_include_path $path
				}
				tcc compile "#include <tcl.h>\n$::pgwire::c_code"
				tcc command ::pgwire::c_makerow2             c_makerow2
				tcc command ::pgwire::c_foreach_batch        c_foreach_batch
				tcc nrcommand ::pgwire::c_foreach_batch_nr   c_foreach_batch_nr c_foreach_batch_nr_setup
				tcc command ::pgwire::c_allrows_batch        c_allrows_batch
				tcc command ::pgwire::compile_ops            compile_ops_cmd
				rename tcc {}
				# With a little help from my friends (c) >>>
				set ::pgwire::accelerators	1
				::pgwire::log notice "build pgwire accelerators with tcc: [format {%.3f ms} [expr {([clock microseconds] - $start)/1e6}]]"
			}
		}
	}
} else {
	puts stderr "Not using accelerators, block: [info exists ::pgwire::block_accelerators], critcl versions: ([package versions critcl]), tcc4tcl versions: ([package versions tcc4tcl])"
}

oo::class create ::pgwire {
	variable {*}{
		socket
		transaction_status
		type_oids
		type_oids_rev
		tcl_encoding
		server_params
		name_seq
		backend_key_data
		prepared
		ready_for_query
		age_count

		msgtypes
		encodings_map
		errorfields

		busy_sql
		sync_outstanding
		pending_rowbuffer
		preserved
	}

	constructor args { #<<<
		#::pgwire::log notice "pgwire::constructor args: ($args)"
		if {[self next] ne ""} next

		if {"::tcl::mathop" ni [namespace path]} {
			namespace path [list {*}[namespace path] ::tcl::mathop]
		}

		# Const lookups <<<
		set msgtypes {
			frontend {
				CopyData		d
				CopyDone		c
				Bind			B
				Close			C
				CopyFail		f
				Describe		D
				Execute			E
				Flush			H
				FunctionCall	F
				Parse			P
				PasswordMessage	p
				Query			Q
				Sync			S
				Terminate		X
			}
			backend {
				R	Authentication*
				R0	AuthenticationOk
				R2	AuthenticationKerberosV5
				R3	AuthenticationCleartextPassword
				R5	AuthenticationMD5Password
				R6	AuthenticationSCMCredential
				R7	AuthenticationGSS
				R8	AuthenticationGSSContinue
				R9	AuthenticationSSPI
				K	BackendKeyData
				2	BindComplete
				3	CloseComplete
				C	CommandComplete
				G	CopyInResponse
				H	CopyOutResponse
				D	DataRow
				I	EmptyQueryResponse
				E	ErrorResponse
				V	FunctionCallResponse
				n	NoData
				N	NoticeResponse
				A	NotificationResponse
				t	ParameterDescription
				S	ParameterStatus
				1	ParseComplete
				s	PortalSuspended
				Z	ReadyForQuery
				T	RowDescription
				d	CopyData
				c	CopyDone
			}
		}
		set encodings_map {
			SQL_ASCII		ascii
			WIN950			ascii
			Windows950		ascii
			BIG5			big5
			EUC_CN			euc-cn
			EUC_JP			euc-jp
			EUC_KR			euc-kr
			LATIN1			iso8859-1
			ISO88591		iso8859-1
			LATIN2			iso8859-2
			ISO88592		iso8859-2
			LATIN3			iso8859-3
			ISO88593		iso8859-3
			LATIN4			iso8859-4
			ISO88594		iso8859-4
			ISO_8859_5		iso8859-5
			ISO_8859_6		iso8859-6
			ISO_8859_7		iso8859-7
			ISO_8859_8		iso8859-8
			LATIN5			iso8859-9
			ISO88599		iso8859-9
			LATIN6			iso8859-10
			ISO885910		iso8859-10
			LATIN7			iso8859-13
			ISO885913		iso8859-13
			LATIN8			iso8859-14
			ISO885914		iso8859-14
			LATIN9			iso8859-15
			ISO885915		iso8859-15
			LATIN10			iso8859-16
			ISO885916		iso8859-16
			KOI8R			koi8-r
			KOI8			koi8-r
			KOI8U			koi8-u
			SJIS			shiftjis
			Mskanji			shiftjis
			ShiftJIS		shiftjis
			WIN932			shiftjis
			Windows932		shiftjis
			UTF8			utf-8
			Unicode			utf-8
		}
		set errorfields {
			S	Severity
			V	SeverityNL
			C	Code
			M	Message
			D	Detail
			H	Hint
			P	Position
			p	InternalPosition
			q	InternalQuery
			W	Where
			s	Schema
			t	Table
			c	Column
			d	DataType
			n	Constraint
			F	File
			L	Line
			R	Routine
		}
		# Const lookups >>>

		my variable makerow_cache
		set makerow_cache	{}
		set preserved		{}

		switch -exact [llength $args] {
			0 {
			}

			2 {
				if {[lindex $args 0] eq "-attach"} {
					my attach [lindex $args 1]
				} else {
					error "Wrong number of arguments, must be {chan db user password} or -attach \$handle"
				}
			}

			4 - 5 {
				my connect {*}$args
			}

			default {
				error "Wrong number of arguments, must be {chan db user password} or -attach \$handle"
			}
		}
	}

	#>>>
	destructor { #<<<
		if {[info exists socket] && $socket in [chan names]} {
			my Terminate
			close $socket
		}
		unset -nocomplain socket
		if {[self next] ne ""} next
	}

	#>>>
	method connect {chan db user password {params {}}} { #<<<
		set socket				$chan
		set name_seq			0
		set prepared			{}
		set ready_for_query		0
		set age_count			0
		set sync_outstanding	0

		# Initial values <<<
		set transaction_status	I
		set server_params		{}
		set backend_key_data	{}
		set type_oids_rev	{
			bytea		17
			int8		20
			int2		21
			int4		23
			text		25
			float4		700
			float8		701
		}
		set type_oids	{
			17		bytea
			20		int8
			21		int2
			23		int4
			25		text
			700		float4
			701		float8
		}
		set tcl_encoding	utf-8
		# Initial values >>>

		chan configure $socket \
			-blocking		1 \
			-translation	binary \
			-encoding		binary \
			-buffering		full

		my _startup $db $user $password $params
	}

	#>>>
	method detach_raw {} { # Detach, without resetting transaction state or command sync <<<
		if {![info exists socket] || $socket ni [chan names]} {
			error "Not connected"
		}

		#package require Thread
		thread::detach $socket
		try {
			list \
				socket				$socket \
				transaction_status	$transaction_status \
				type_oids			$type_oids \
				type_oids_rev		$type_oids_rev \
				tcl_encoding		$tcl_encoding \
				server_params		$server_params \
				name_seq			$name_seq \
				backend_key_data	$backend_key_data \
				prepared			$prepared \
				ready_for_query		$ready_for_query \
				age_count			$age_count \
				sync_outstanding	$sync_outstanding
		} finally {
			unset socket
			my destroy
		}
	}

	#>>>
	method detach {} { #<<<
		if {![info exists socket] || $socket ni [chan names]} {
			error "Not connected"
		}

		# Ensure that the handle we're detaching is ready for a query and not in an open transaction <<<
		if {!$ready_for_query} {
			puts -nonewline $socket S\u0\u0\u0\u4; set sync_outstanding 1
			flush $socket
			while 1 {
				if {[binary scan [read $socket 5] aI msgtype len] != 2} {my connection_lost}
				incr len -4	;# len includes itself

				switch -exact -- $msgtype {
					Z { # ReadyForQuery <<<
						set data	[read $socket $len]
						if {[eof $socket]} {my connection_lost}
						binary scan $data a transaction_status
						set ready_for_query		1
						set sync_outstanding	0
						break
						#>>>
					}

					R - E - S - K - C - D - T - t - 1 - 2 - n - s {
						# Eat these while waiting for the Sync response
						if {$len > 0} {
							read $socket $len
							if {[eof $socket]} {my connection_lost}
						}
					}

					default {my default_message_handler $msgtype $len}
				}
			}
		}

		if {$transaction_status ne "I"} {
			my rollback
		}
		# Ensure that the handle we're detaching is ready for a query and not in an open transaction >>>

		tailcall my detach_raw
	}

	#>>>
	method attach handle { #<<<
		#package require Thread
		dict with handle {}
		thread::attach $socket
	}

	#>>>
	method _tx {start end bytes} { #<<<
	}

	#>>>
	method _rx {start end bytes} { #<<<
	}

	#>>>

	# Try to find a suitable md5 command <<<
	if {![catch {package require hash}]} {
		method _md5_hex bytes {
			package require hash
			binary encode hex [hash::md5 $bytes]
		}
	} elseif {![catch {md5 foo} r]} {
		switch -exact -- [binary encode hex $r] {
			acbd18db4cc2f85cedef654fccc4a4d8 { # md5 command returns binary
				method _md5_hex bytes {
					binary encode hex [md5 $bytes]
				}
			}
			6163626431386462346363326638356365646566363534666363633461346438 { # md5 command returns hex
				method _md5_hex bytes {
					md5 $bytes
				}
			}
			724c305932307a432b467a74373256507a4d536b32413d3d { # md5 command returns base64
				method _md5_hex bytes {
					binary encode hex [binary decode base64 [md5 $bytes]]
				}
			}
			default {
				error "md5 command return format not recognised"
			}
		}
	} else {
		# TODO: provide a pure Tcl implementation?
		error "No md5 command available"
	}
	# Try to find a suitable md5 command >>>
	method _startup {db user password params} { #<<<
		set payload	[binary format I [expr {
			3 << 16 | 0
		}]]	;# Version 3.0

		foreach {k v} [list \
			user						$user \
			database					$db \
			client_encoding				UTF8 \
			standard_conforming_strings	on \
			{*}$params
		] {
			append payload $k \0 $v \0
		}
		append payload \0

		puts -nonewline $socket [binary format Ia* [expr {4+[string length $payload]}] $payload]
		flush $socket

		my on_response {
			AuthenticationOk {} {}

			AuthenticationMD5Password salt {
				my PasswordMessage md5[my _md5_hex [my _md5_hex [encoding convertto $tcl_encoding $password$user]]$salt]
				flush $socket
			}
			AuthenticationCleartextPassword	{} {
				my PasswordMessage [encoding convertto $tcl_encoding $password]
				flush $socket
			}

			AuthenticationKerberosV5		args {my _error "Authentication method not supported"}
			AuthenticationSCMCredential		args {my _error "Authentication method not supported"}
			AuthenticationGSS				args {my _error "Authentication method not supported"}
			AuthenticationGSSContinue		args {my _error "Authentication method not supported"}
			AuthenticationSSPI				args {my _error "Authentication method not supported"}
			ReadyForQuery transaction_status break
		}

		my simple_query_dict row {
			select
				oid,
				typname
			from
				pg_type
			where
				oid < 10000 and
				oid is not null and
				typname is not null
		} {
			dict set type_oids		[dict get $row oid] [dict get $row typname]
			dict set type_oids_rev	[dict get $row typname] [dict get $row oid]
		}
		#::pgwire::log notice "server_params: $server_params"
	}

	#>>>
	method _compile_actions actions { #<<<
		set compiled_actions	{}
		foreach {messagename msgargs body} $actions {
			set wbody	""
			if {[llength $msgargs] > 0} {
				set linkvarcmds	{}
				foreach arg $msgargs {
					lappend linkvarcmds	[list upvar 2 $arg $arg]
				}
				append wbody	[join $linkvarcmds \n] \n
				append wbody	[format {lassign $args %s} $msgargs] \n
				append wbody	[format {catch {uplevel 2 %s} r o} [list $body]] \n
			} else {
				append wbody	[format {catch {uplevel 2 %s} r o} [list $body]] \n
			}
			append wbody {list $r $o} \n
			dict set compiled_actions $messagename [list args $wbody]
		}
		set valid_messagenames	[dict values [dict get $msgtypes backend]]
		foreach key [dict keys $compiled_actions] {
			if {$key ni $valid_messagenames} {
				error "Error compiling action handlers: \"$key\" is not a valid message type, must be one of: [join $valid_messagenames {, }]"
			}
		}
		dict set compiled_actions ErrorResponse \
			[list fields	[format {%s $fields} [namespace code {my ErrorResponse}]]]
		dict set compiled_actions NoticeResponse \
			[list fields	[format {%s $fields} [namespace code {my NoticeResponse}]]]

	}

	#>>>
	method ErrorResponse fields { #<<<
		if {[dict exists $fields SeverityNL]} {
			set severity	[dict get $fields SeverityNL]
		} else {
			set severity	[dict get $fields Severity]
		}
		if {[dict exists $fields Code]} {
			set code		[dict get $fields Code]
		} else {
			set code		unknown
		}
		return -level 2 -code error -errorcode [list PGWIRE ErrorResponse $severity $code $fields] "Postgres error: [dict get $fields Message]"
	}

	#>>>
	method NoticeResponse fields { #<<<
		# forward to get notices
		::pgwire::log warning "Postgres NoticeResponse:\n\t[join [lmap {k v} $fields {format {%20s: %s} $k $v}] \n\t]"
	}

	#>>>
	method NotificationResponse {pid channel payload} { #<<<
		# forward this to get notification pushes
	}

	#>>>
	method _error msg { #<<<
		my destroy
		throw {PGWIRE FATAL} $msg
	}

	#>>>
	method connection_lost {} { #<<<
		close $socket
		unset socket
		my destroy
		throw {PG CONNECTION LOST} "Server closed connection"
	}

	#>>>
	method write data { #<<<
		puts -nonewline $socket $data
	}

	#>>>
	method flush {} { #<<<
		flush $socket
	}

	#>>>
	if {[catch {binary scan foo\u0bar C* _str}] == 0 && $_str eq "foo"} {
		method _get_string {data ofs_var} { # Use TIP586's binary scan c-string support <<<
			upvar 1 $ofs_var i
			if {[binary scan C* $data string] == 0} {
				#::pgwire::log notice "No c-string found in [regexp -all -inline .. [binary encode hex $data]]"
				throw unterminated_string "No null terminator found"
			}
			set i	[expr {$i + [string length $string] + 1}]
			set string
		}

		#>>>
	} else {
		method _get_string {data ofs_var} { #<<<
			upvar 1 $ofs_var i
			set idx	[string first \u0 $data $i]
			if {$idx == -1} {
				#::pgwire::log notice "No c-string found in [regexp -all -inline .. [binary encode hex $data]]"
				throw unterminated_string "No null terminator found"
			}
			set string	[string range $data $i [expr {$idx - 1}]]
			set i		[expr {$idx + 1}]
			set string
		}

		#>>>
	}
	method read_one_message {} { #<<<
		while 1 {
			if {[binary scan [read $socket 5] aI msgtype len] != 2} {my connection_lost}
			#::pgwire::log notice "got msgtype ($msgtype)"
			incr len -4		;# len includes itself
			set data	[read $socket $len]
			if {[string length $data] != $len} {
				if {[eof $socket]} {my connection_lost}
			}
			#::pgwire::log notice "read $len bytes of data for ($msgtype)"
			try {
				dict get $msgtypes backend $msgtype
			} trap {TCL LOOKUP DICT} {errmsg options} {
				my _error "Invalid msgtype: \"$msgtype\", [binary encode hex $msgtype], probably a sync issue, abandoning connection"
			} on ok messagename {}

			# Parse message <<<
			#::pgwire::log notice "parsing $messagename"
			set messageparams	{}
			switch -exact -- $messagename {
				Authentication* { #<<<
					binary scan $data I subtype
					set msgtype	R$subtype
					try {
						set messagename	[dict get $msgtypes backend $msgtype]
					} trap {TCL LOOKUP DICT} {errmsg options} {
						set messagename	_invalid_$msgtype
					}
					switch -exact -- $messagename {
						AuthenticationMD5Password { # AuthenticationMD5Password <<<
							binary scan $data @4a4 salt
							lappend messageparams $salt
							#>>>
						}
						AuthenticationOk { # AuthenticationOk <<<
							#>>>
						}
						AuthenticationKerberosV5 { # AuthenticationKerberosV5 <<<
							#>>>
						}
						AuthenticationSCMCredential { # AuthenticationSCMCredential <<<
							#>>>
						}
						AuthenticationGSS { # AuthenticationGSS <<<
							#>>>
						}
						AuthenticationGSSContinue { # AuthenticationGSSContinue <<<
							#>>>
						}
						AuthenticationSSPI { # AuthenticationSSPI <<<
							#>>>
						}

						default { #<<<
							::pgwire::log warning "Parsing of msgtype \"$msgtype\" not implemented yet"
							#>>>
						}
					}
					#>>>
				}
				ErrorResponse { # ErrorResponse <<<
					set i	0
					set fields	{}
					while {$i < [string length $data]} {
						set field_type	[string index $data $i]
						if {$field_type eq "\0"} break
						incr i
						set string		[my _get_string $data i]
						lappend fields	[dict get $errorfields $field_type] $string
					}
					lappend messageparams	$fields
					#>>>
				}
				ParameterStatus { # ParameterStatus <<<
					set i	0
					set param	[my _get_string $data i]
					set value	[my _get_string $data i]
					#?? {::pgwire::log debug "Server ParameterStatus \"$param\" -> \"$value\""}
					dict set server_params $param $value
					if {$param eq "client_encoding"} {
						if {[dict exists $encodings_map $value]} {
							set tcl_encoding	[dict get $encodings_map $value]
						} else {
							::pgwire::log warning "No tcl encoding equivalent defined for \"$value\""
							set tcl_encoding	ascii
						}
					}
					continue
					#>>>
				}
				ReadyForQuery { # ReadyForQuery <<<
					binary scan $data a transaction_status
					set ready_for_query		1
					set sync_outstanding	0
					lappend messageparams $transaction_status
					#>>>
				}
				NoticeResponse { # NoticeResponse <<<
					set i	0
					set fields	{}
					while {$i < [string length $data]} {
						set field_type	[string index $data $i]
						if {$field_type eq "\0"} break
						incr i
						set string	[my _get_string $data i]
						lappend fields	[dict get $errorfields $field_type] $string
					}
					lappend messageparams $fields
					#>>>
				}
				BackendKeyData { # BackendKeyData <<<
					binary scan $data I backend_key_data
					#::pgwire::log notice "BackendKeyData: $backend_key_data"
					continue
					#>>>
				}
				CommandComplete { # CommandComplete <<<
					set i	0
					lappend messageparams	[my _get_string $data i]
					#>>>
				}
				NotificationResponse { # NotificationResponse <<<
					binary scan $data I pid
					set i 4
					set channel	[my _get_string $data i]
					set payload	[my _get_string $data i]
					my NotificationResponse $pid $channel $payload
					continue
					#>>>
				}
				DataRow { # DataRow <<<
					binary scan $data S column_count
					#?? {::pgwire::log debug "column_count: $column_count"}
					set i	2
					set row	{}
					for {set c 0} {$c < $column_count} {incr c} {
						binary scan $data @${i}I len
						#?? {::pgwire::log debug "Reading column, len: $len"}
						incr i 4
						if {$len == -1} {
							# Column is NULL
							lappend row 1 {}
						} else {
							binary scan $data @${i}a${len} column_data
							lappend row 0 $column_data
							incr i $len
						}
					}
					#?? {::pgwire::log debug "Output row: ($row)"}
					lappend messageparams $row
					#>>>
				}
				RowDescription { # RowDescription <<<
					binary scan $data S fields_per_row
					set i	2
					set column_desc	{}
					for {set c 0} {$c < $fields_per_row} {incr c} {
						set field_name	[my _get_string $data i]
						binary scan $data @${i}ISISIS \
								table_oid \
								attrib_num \
								type_oid \
								data_size \
								type_modifier \
								format
						incr i 18
						if {[dict exists $type_oids $type_oid]} {
							set type_name	[dict get $type_oids $type_oid]
						} else {
							set type_name	__unknown($type_oid)
						}
						dict set column_desc $field_name [dict create \
								table_oid		$table_oid \
								attrib_num		$attrib_num	\
								type_oid		$type_oid \
								type_name		$type_name \
								data_size		$data_size \
								type_modifier	$type_modifier \
								format			$format \
						]
					}
					lappend messageparams $column_desc
					#>>>
				}
				ParameterDescription { # ParameterDescription <<<
					binary scan $data S count
					binary scan $data @2I${count} parameter_type_oids
					#lappend messageparams [lmap oid $parameter_type_oids {expr {[dict exists $type_oids $oid] ? [dict get $type_oids $oid] : $oid}}]

					lappend messageparams [lmap e $parameter_type_oids {dict get $type_oids $e}]
					#>>>
				}
				ParseComplete { # ParseComplete <<<
					#>>>
				}
				BindComplete { # BindComplete <<<
					#>>>
				}
				CloseComplete { # CloseComplete <<<
					#>>>
				}
				NoData { # NoData <<<
					#>>>
				}
				PortalSuspended { # PortalSuspended <<<
					#>>>
				}

				default { #<<<
					::pgwire::log warning "Parsing of msgtype \"$msgtype\" not implemented yet"
					#>>>
				}
			}
			# Parse message >>>

			#::pgwire::log notice "read_one_message returning [list $messagename $messageparams]"
			return [list $messagename $messageparams]
		}
	}

	#>>>
	method on_response actions { #<<<
		set compiled_actions	[my _compile_actions $actions]
		while 1 {
			set messages	[my read_one_message]
			set chainres	{}
			foreach {messagetype messageparams} $messages {
				#::pgwire::log notice "messagetype: ($messagetype), messageparams: ($messageparams)"
				try {
					lassign [apply [dict get $compiled_actions $messagetype] {*}$messageparams] \
						res options
					return -options $options $res
				} on break {res options} {
					set chainres	[list $res $options]
					break
				} trap {TCL LOOKUP DICT} {errmsg options} {
					::pgwire::log error "Unhandled message: $messagetype:\n$messageparams\n-----------\n[join $compiled_actions \n]\n[dict get $options -errorinfo]"
					set frames	{}
					for {set i [info frame]} {$i > 1} {incr i -1} {
						lappend frames	[info frame $i]
					}
					::pgwire::log error "frames:\n-*- [join $frames "\n-*- "]"
					my Terminate
					my destroy
					throw {PGWIRE UNSYNCED} "Unexpected message \"$messagetype\" in current context"
				} on error {errmsg options} {
					return -options $options "Action $messagetype handler error: $errmsg"
				}
			}
			if {[llength $chainres] > 0} {
				lassign $chainres res options
				return -options $options $res
			}
		}
	}

	#>>>
	method close_statement stmt_name { #<<<
		my Close S $stmt_name
	}

	#>>>
	method close_portal portal_name { #<<<
		my Close P $portal_name
	}

	#>>>
	method read_to_sync {} { #<<<
		while 1 {
			if {[binary scan [read $socket 5] aI msgtype len] != 2} {my connection_lost}
			#::pgwire::log notice "read_to_sync msgtype ($msgtype)"
			incr len -4
			if {$msgtype eq "Z"} { # ReadyForQuery
				if {[binary scan [read $socket $len] a transaction_status] != 1} {my connection_lost}
				set ready_for_query		1
				set sync_outstanding	0
				break
			} else {
				my default_message_handler $msgtype $len
			}
		}
		#::pgwire::log notice "read_to_sync returning transaction_status: ($transaction_status)"
		set transaction_status
	}

	#>>>
	method sync {} { #<<<
		puts -nonewline $socket S\u0\u0\u0\u4; set sync_outstanding 1
		flush $socket
		my read_to_sync
	}

	#>>>
	method skip_to_sync {} { #<<<
		if {[info exists socket] && $socket in [chan names]} {
			while 1 {
				if {[binary scan [read $socket 5] aI msgtype len] != 2} {my connection_lost}
				incr len -4	;# len includes itself

				switch -exact -- $msgtype {
					Z { # ReadyForQuery <<<
						set data	[read $socket $len]
						if {[eof $socket]} {my connection_lost}
						binary scan $data a transaction_status
						set ready_for_query		1
						set sync_outstanding	0
						break
						#>>>
					}

					R - E - S - K - C - D - T - t - 1 - 2 - n - s {
						# Eat these while waiting for the Sync response
						if {$len > 0} {read $socket $len}
						if {[eof $socket]} {my connection_lost}
					}

					default {my default_message_handler $msgtype $len}
				}
			}
		}
	}

	#>>>
	method sync_outstanding args { #<<<
		set sync_outstanding {*}$args
	}

	#>>>
	method tcl_encoding {} { set tcl_encoding }

	method Flush {} { #<<<
		puts -nonewline $socket H\u0\u0\u0\u4
		flush $socket
	}

	#>>>
	method Sync {} { #<<<
		puts -nonewline $socket S\u0\u0\u0\u4; set sync_outstanding 1
		flush $socket
	}

	#>>>
	method Terminate {} { #<<<
		#::pgwire::log notice "Sending Terminate"
		puts -nonewline $socket X\u0\u0\u0\u4
	}

	#>>>
	method PasswordMessage password { #<<<
		set payload	[encoding convertto $tcl_encoding $password]\u0
		puts -nonewline $socket [binary format aIa* p [+ 4 [string length $payload]] $payload]
	}

	#>>>
	method Query sql { #<<<
		set payload	[encoding convertto $tcl_encoding $sql]\u0
		puts -nonewline $socket [binary format aIa* Q [+ 4 [string length $payload]] $payload]
	}

	#>>>
	method Close {type name} { #<<<
		set payload	$type[encoding convertto $tcl_encoding $name]\u0
		puts -nonewline $socket [binary format aIa* C [+ 4 [string length $payload]] $payload]
	}

	#>>>
	method preserve sql { #<<<
		dict incr preserved $sql 1
	}

	#>>>
	method release sql { #<<<
		if {[dict exists $preserved $sql] && [dict get $preserved $sql] > 0} {
			dict incr preserved $sql -1
			if {[dict get $preserved $sql] == 0} {
				dict unset preserved $sql
			}
		}
	}

	#>>>

	method val str { # Quote a SQL value <<<
		if {
			[dict exists $server_params standard_conforming_strings] &&
			[dict get $server_params standard_conforming_strings] eq "on"
		} {
			return '[string map {' ''} $str]'
		} else {
			return '[string map {' '' \\ \\\\} $str]'
		}
	}

	#>>>
	method name str { # Quote a SQL identifier <<<
		return \"[string map {"\"" "\"\"" "\\" "\\\\"} $str]\"
	}

	#>>>
	method _basic_query_noresult sql { #<<<
		set payload [encoding convertto $tcl_encoding $sql]\u0
		puts -nonewline $socket [binary format aIa* Q [+ 4 [string length $payload]] $payload]
		flush $socket
		while 1 {
			if {[binary scan [read $socket 5] aI msgtype len] != 2} {my connection_lost}
			incr len -4
			switch -exact -- $msgtype {
				Z { # ReadyForQuery <<<
					if {![binary scan [read $socket $len] a transaction_status] == 1} {my connection_lost}
					set ready_for_query		1
					set sync_outstanding	0
					break
					#>>>
				}
				C - T - D { # CommandComplete, RowDescription, DataRow <<<
					set data	[read $socket $len]
					if {[eof $socket]} {my connection_lost}
					#>>>
				}
				default {my default_message_handler $msgtype $len}
			}
		}
	}

	#>>>
	method begintransaction {} { #<<<
		tailcall my _basic_query_noresult begin
	}

	#>>>
	method rollback {} { #<<<
		if {$transaction_status ne "I"} {
			tailcall my _basic_query_noresult rollback
		}
	}

	#>>>
	method commit {} { #<<<
		if {$transaction_status ne "I"} {
			tailcall my _basic_query_noresult commit
		}
	}

	#>>>
	method simple_query_dict {rowdict sql on_row} { #<<<
		if {!$ready_for_query} {
			error "Re-entrant query while another is processing: $sql while processing: $busy_sql"
		}
		set busy_sql	$sql
		upvar 1 $rowdict row
		package require tdbc
		set quoted	{}
		foreach tok [tdbc::tokenize $sql] {
			switch -glob -- $tok {
				::* {
					# Prevent PG casts from looking like params
					append compiled	$tok
				}

				:* - $* - @* {
					set name	[string range $tok 1 end]
					if {[regexp {^[0-9]+$} $name]} {
						# Avoid matching $1, $4, etc in the statement (which usually aren't intended
						# to be parameters from us, but refer to the argument of functions).  Not
						# watertight, but would require a SQL-dialect aware parser to do properly.
						append compiled $tok
						continue
					}
					set exists	[uplevel 1 [list info exists $name]]
					if {$exists} {
						set value	[uplevel 1 [list set $name]]
						append quoted	[my val $value]
					} else {
						append quoted	NULL
					}
				}

				; {
					append compiled $tok
				}

				default {
					append quoted $tok
				}
			}
		}

		my Query $quoted
		flush $socket
		my on_response {
			RowDescription columns {set result_columns $columns}
			DataRow column_values {
				set row {}
				foreach {isnull value} $column_values name [dict keys $result_columns] {
					if {$isnull} {
					} else {
						dict set row $name $value
					}
				}
				uplevel 1 $on_row
			}
			CommandComplete {} {}
			CloseComplete {} {}
			ReadyForQuery transaction_status break
		}
	}

	#>>>
	method simple_query_list {rowlist sql on_row} { #<<<
		if {!$ready_for_query} {
			error "Re-entrant query while another is processing: $sql while processing: $busy_sql"
		}
		set busy_sql	$sql
		upvar 1 $rowlist row
		package require tdbc
		set quoted	{}
		foreach tok [tdbc::tokenize $sql] {
			switch -glob -- $tok {
				::* {
					# Prevent PG casts from looking like params
					append compiled	$tok
				}

				:* - $* - @* {
					set name	[string range $tok 1 end]
					if {[regexp {^[0-9]+$} $name]} {
						# Avoid matching $1, $4, etc in the statement (which usually aren't intended
						# to be parameters from us, but refer to the argument of functions).  Not
						# watertight, but would require a SQL-dialect aware parser to do properly.
						append compiled $tok
						continue
					}
					set exists	[uplevel 1 [list info exists $name]]
					if {$exists} {
						set value	[uplevel 1 [list set $name]]
						append quoted	[my val $value]
					} else {
						append quoted	NULL
					}
				}

				; {
					append compiled $tok
				}

				default {
					append quoted $tok
				}
			}
		}

		my Query $quoted
		flush $socket
		my on_response {
			RowDescription columns {set result_columns $columns}
			DataRow column_values {
				set row	[lmap {isnull value} $column_values {set value}]
				uplevel 1 $on_row
			}
			CloseComplete {} {}
			CommandComplete {} {}
			ReadyForQuery transaction_status break
		}
	}

	#>>>
	method default_message_handler {msgtype len} { #<<<
		switch -exact -- $msgtype {
			E - N { # ErrorResponse / NoticeResponse <<<
				set data	[read $socket $len]
				#::pgwire::log notice "len: ($len), data: [string length $data]"
				if {[eof $socket]} {my connection_lost}

				set i	0
				set fields	{}
				while {$i < [string length $data]} {
					set field_type	[string index $data $i]
					if {$field_type eq "\u0"} break
					incr i

					set idx	[string first "\u0" $data $i]
					if {$idx == -1} {
						#::pgwire::log notice "No c-string found in [regexp -all -inline .. [binary encode hex $data]]"
						throw unterminated_string "No null terminator found"
					}
					set string	[string range $data $i [- $idx 1]]
					set i	[+ $idx 1]

					# SeverityNL:	Same as Severity, but not localized
					lappend fields	[dict get $errorfields $field_type] $string
				}

				if {$msgtype eq "E"} {
					my ErrorResponse $fields
				} else {
					my NoticeResponse $fields
				}
				#>>>
			}
			S { # ParameterStatus <<<
				set data	[read $socket $len]
				if {[eof $socket]} {my connection_lost}

				set i	0

				set idx	[string first "\u0" $data $i]
				if {$idx == -1} {
					#::pgwire::log notice "No c-string found in [regexp -all -inline .. [binary encode hex $data]]"
					throw unterminated_string "No null terminator found"
				}
				set param	[string range $data $i [- $idx 1]]
				set i	[+ $idx 1]

				set idx	[string first "\u0" $data $i]
				if {$idx == -1} {
					#::pgwire::log notice "No c-string found in [regexp -all -inline .. [binary encode hex $data]]"
					throw unterminated_string "No null terminator found"
				}
				set value	[string range $data $i [- $idx 1]]

				dict set server_params $param $value
				if {$param eq "client_encoding"} {
					if {[dict exists $encodings_map $value]} {
						set tcl_encoding	[dict get $encodings_map $value]
					} else {
						::pgwire::log warning "No tcl encoding equivalent defined for \"$value\""
						set tcl_encoding	ascii
					}
				}
				#>>>
			}
			A { # NotificationResponse <<<
				set data	[read $socket $len]
				if {[eof $socket]} {my connection_lost}
				binary scan $data I pid
				set i 4

				set idx	[string first "\u0" $data $i]
				if {$idx == -1} {
					#::pgwire::log notice "No c-string found in [regexp -all -inline .. [binary encode hex $data]]"
					throw unterminated_string "No null terminator found"
				}
				set channel	[string range $data $i [- $idx 1]]
				set i	[+ $idx 1]

				set idx	[string first "\u0" $data $i]
				if {$idx == -1} {
					#::pgwire::log notice "No c-string found in [regexp -all -inline .. [binary encode hex $data]]"
					throw unterminated_string "No null terminator found"
				}
				set payload	[string range $data $i [- $idx 1]]

				my NotificationResponse $pid $channel $payload
				#>>>
			}
			3 { # CloseComplete <<<
				# Accept these here so that we don't have to wait in a blocking loop for each statement closed
				#>>>
			}
			default { #<<<
				my _error "Invalid msgtype: \"$msgtype\", [binary encode hex $msgtype], probably a sync issue, abandoning the connection"
				#>>>
			}
		}
	}

	#>>>
	method tokenize sql { # Parse the bind variables from the SQL <<<
		set seq				0
		set params_assigned {}
		set compiled		{}

		foreach tok [tdbc::tokenize $sql] {
			switch -glob -- $tok {
				::* {
					# Prevent PG casts from looking like params
					append compiled $tok
				}

				:* - $* - @* {
					set name	[string range $tok 1 end]
					if {[regexp {^[0-9]+$} $name]} {
						# Avoid matching $1, $4, etc in the statement (which usually aren't intended
						# to be parameters from us, but refer to the argument of functions).  Not
						# watertight, but would require a SQL-dialect aware parser to do properly.
						append compiled $tok
						continue
					}

					if {![dict exists $params_assigned $name]} {
						set id	[incr seq]
						dict set params_assigned $name id		$id
					} else {
						set id	[dict get $params_assigned $name id]
					}

					append compiled \$$id
				}
				; {
					#error "Multiple statements are not supported"
					# These may be within a function definition or similar.  Build it here and let
					# the backend sort out whether it will accept it
					append compiled $tok
				}
				default {
					append compiled $tok
				}
			}
		}

		list $params_assigned $compiled
	}

	#>>>
	method prepare_extended_query sql { #<<<
		set busy_sql	$sql

		if {![dict exists $prepared $sql]} { # Prepare statement <<<
			package require tdbc
			set stmt_name	[incr name_seq]
			#::pgwire::log notice "No prepared statement, creating one called ($stmt_name)"

			lassign [my tokenize $sql] \
				params_assigned \
				compiled

			if {[incr age_count] > 10} {
				if {[dict size $prepared] > 50} {
					log_duration "Cache check" {
					# Age cache <<<
					set expired	0
					# Pre-scan for one-offs to expire <<<
					dict for {sql info} $prepared {
						if {[dict exists $preserved $sql]} continue
						if {[dict get $info heat] == 0} {
							set closemsg	S[encoding convertto $tcl_encoding [dict get $info stmt_name]]\u0
							puts -nonewline $socket [binary format aIa* C [+ 4 [string length $closemsg]] $closemsg]
							incr expired
							dict unset prepared $sql
						}
					}
					# Pre-scan for one-offs to expire >>>
					if {[dict size $prepared] > 50} {
						#::pgwire::log notice "Prescan expired $expired entries, [dict size $prepared] remain, aging:\n\t[join [lmap {k v} $prepared {format {%s: %3d} $k [dict get $v heat]}] \n\t]"
						# Age the entries that have hits <<<
						dict for {sql info} $prepared {
							if {[dict exists $preserved $sql]} continue
							set heat	[expr {[dict get $info heat] >> 1}]
							if {$heat eq 0} {
								set closemsg	S[encoding convertto $tcl_encoding [dict get $info stmt_name]]\u0
								puts -nonewline $socket [binary format aIa* C [+ 4 [string length $closemsg]] $closemsg]
								incr expired
								dict unset prepared $sql
							} else {
								dict set prepared $sql heat $heat
							}
						}
						# Age the entries that have hits >>>
					}
					# Age cache >>>
					}
				}
				set age_count	0
			}

			try {
				# Parse, to $stmt_name
				# Describe S $stmt_name
				# Flush
				set parsemsg	[encoding convertto $tcl_encoding $stmt_name]\u0[encoding convertto $tcl_encoding $compiled]\u0\u0\u0
				set describemsg	S[encoding convertto $tcl_encoding $stmt_name]\u0
				set busy_sql	$sql
				puts -nonewline $socket [binary format aI P [+ 4 [string length $parsemsg]]]$parsemsg[binary format aI D [expr {4+[string length $describemsg]}]]${describemsg}H\u0\u0\u0\u4

				flush $socket

				# Parse responses <<<
				while 1 {
					if {[binary scan [read $socket 5] aI msgtype len] != 2} {my connection_lost}
					incr len -4	;# len includes itself
					if {$msgtype eq 1} break elseif {$msgtype eq 3} {
						# Eat any CloseComplete (3) msgs here from the cache
						# expiries above (to save a round-trip time when
						# expiring statements)
						#incr expired -1
						#::pgwire::log notice "Got CloseComplete, $expired more expected"
					} else {
						my default_message_handler $msgtype $len
					}
				}
				#>>>
				# Describe responses <<<
				set param_desc		{}
				set param_types		{}
				while 1 {
					if {[binary scan [read $socket 5] aI msgtype len] != 2} {my connection_lost}
					incr len -4	;# len includes itself

					switch -exact -- $msgtype {
						t { # ParameterDescription: compile build_params <<<
							binary scan [read $socket $len] SI* count parameter_type_oids
							if {[eof $socket]} {my connection_lost}

							#::pgwire::log notice "ParameterDescription, $count params, type oids: $parameter_type_oids, params_assigned: ($params_assigned)"
							set build_params	{
								set pdesc		{}
								set pformats	{}
							}
							append build_params [list set pcount [llength $parameter_type_oids]] \n
							set build_params_vars	{}
							set build_params_dict	{}

							set param_seq	0
							foreach oid $parameter_type_oids name [dict keys $params_assigned] {
								if {[dict exists $type_oids $oid]} {
									set type_name	[dict get $type_oids $oid]
								} else {
									set type_name	__unknown($oid)
								}
								lappend param_types $name $type_name

								set encode_field	[switch -exact -- $type_name {
									bool	{return -level 0 {append pformats \u0\u1; append pdesc [binary format Ic 1 [expr {!!($value)}]]}}
									int1	{return -level 0 {append pformats \u0\u1; append pdesc [binary format Ic 1 $value]}}
									smallint -
									int2	{return -level 0 {append pformats \u0\u1; append pdesc [binary format IS 2 $value]}}
									int4 -
									integer	{return -level 0 {append pformats \u0\u1; append pdesc [binary format II 4 $value]}}
									bigint -
									int8	{return -level 0 {append pformats \u0\u1; append pdesc [binary format IW 8 $value]}}
									bytea	{return -level 0 {append pformats \u0\u1; append pdesc [binary format Ia* [string length $value] $value]}}
									float4	{return -level 0 {append pformats \u0\u1; append pdesc [binary format IR 4 $value]}}
									float8	{return -level 0 {append pformats \u0\u1; append pdesc [binary format IQ 8 $value]}}
									text -
									varchar {
										return -level 0 {
											set bytes	[encoding convertto $tcl_encoding $value]
											set bytelen	[string length $bytes]
											append pformats \u0\u1; append pdesc [binary format Ia* $bytelen $bytes]
										}
									}
									default {
										return -level 0 {
											set bytes	[encoding convertto $tcl_encoding $value]
											set bytelen	[string length $bytes]
											append pformats \u0\u0; append pdesc [binary format Ia* $bytelen $bytes]
										}
									}
								}]

								append build_params_vars	[string map [list \
									%v%				[list $name] \
									%null%			[list [binary format I -1]] \
									%encode_field%	$encode_field \
								] {
									try {
										if {[uplevel 1 {info exists %v%}]} {
											set value	[uplevel 1 {set %v%}]
											%encode_field%
										} else {
											append pdesc	%null%
											append pformats	\u0\u1	;# binary
										}
									} on error {errmsg options} {
										throw {PGWIRE INPUT_PARAM %v%} "Error fetching value for \"%v%\": $errmsg"
									}
								}]
								append build_params_dict	[string map [list \
									%v%				[list $name] \
									%null%			[list [binary format I -1]] \
									%encode_field%	$encode_field \
								] {
									try {
										if {[dict exists $param_values %v%]} {
											set value	[dict get $param_values %v%]
											%encode_field%
										} else {
											append pdesc	%null%
											append pformats	\u0\u1	;# binary
										}
									} on error {errmsg options} {
										throw {PGWIRE INPUT_PARAM %v%} "Error fetching value for \"%v%\": $errmsg"
									}
								}]
								append param_desc	"# \$[incr param_seq]: $name\t$type_name\n"
							}

							append build_params "
								if {\[info exists param_values\]} {$build_params_dict} else {$build_params_vars}
							" \n
							append build_params	{
								binary format Sa*Sa* $pcount $pformats $pcount $pdesc
							} \n
							#>>>
						}
						T { # RowDescription: compile rformats, c_types and columns <<<
							set data	[read $socket $len]
							if {[eof $socket]} {my connection_lost}

							binary scan $data S fields_per_row
							set i	2
							set c_types		{}
							set columns		{}
							set rformats	[binary format Su $fields_per_row]
							for {set c 0} {$c < $fields_per_row} {incr c} {
								set idx	[string first \u0 $data $i]
								if {$idx == -1} {
									#::pgwire::log notice "No c-string found in [regexp -all -inline .. [binary encode hex $data]]"
									throw unterminated_string "No null terminator found"
								}
								set field_name	[string range $data $i [- $idx 1]]
								set i	[+ $idx 1]

								#binary scan $data @${i}ISISIS \
								#		table_oid \
								#		attrib_num \
								#		type_oid \
								#		data_size \
								#		type_modifier \
								#		format
								#incr i 18
								incr i 6
								binary scan $data @${i}I type_oid
								incr i 12

								if {[dict exists $type_oids $type_oid]} {
									set type_name	[dict get $type_oids $type_oid]
								} else {
									set type_name	text
								}

								switch -glob -- $type_name {
									bool	-
									int1	-
									smallint -
									int2	-
									int4	-
									integer	-
									bigint	-
									int8	-
									bytea	-
									float4	-
									float8	{
										set colfmt		\u0\u1	;# binary
									}
									default	{
										set colfmt		\u0\u0	;# text
										set type_name	text
									}
								}

								append rformats	$colfmt
								lappend c_types	$field_name $type_name
								lappend columns	$field_name
							}

							break
							#>>>
						}
						n { # NoData <<<
							set rformats	\u0\u0
							set c_types		{}
							set columns		{}
							break
							#>>>
						}
						default {my default_message_handler $msgtype $len}
					}
				}
				#>>>
				set cached		{}
				#::pgwire::log notice "execute script:\n$execute\nmakerow_vars: $makerow_vars\nmakerow_dict: $makerow_dict\nmakerow_list: $makerow_list"
				# Results:
				#	- $stmt_name: the name of the prepared statement (as known to the server)
				#	- $build_params: script to run to gather the input params
				#	- $rformats: the marshalled count and formats that we will send the input params as
				#	- $c_types:	the result column names and the formats that they are transported as
				#	- $columns: a list of column names (can contain duplicates)
				#	- $heat: how frequently this prepared statement has been used recently, relative to others
				#	- $ops_cache: dictionary, keyed by format, of [pgwire::build_ops $format $c_types]
				#	- $param_types: input types
				dict set prepared $sql [set stmt_info [dict create \
					stmt_name			$stmt_name \
					build_params		$build_params \
					rformats			$rformats \
					c_types				$c_types \
					columns				$columns \
					heat				0 \
					ops_cache			{} \
					param_types			$param_types \
				]]
				#::pgwire::log notice "Finished preparing statement, execute:\n$execute"
			} on error {errmsg options} { #<<<
				#::pgwire::log error "Error preparing statement,\n$sql\n-- compiled ----------------\n$compiled\ syncing: [dict get $options -errorinfo]"
				if {[info exists socket]} {
					if {!$sync_outstanding} {
						puts -nonewline $socket S\u0\u0\u0\u4; set sync_outstanding 1
						flush $socket
					}
					my skip_to_sync
				}
				return -options $options $errmsg
			}
			#>>>
			#>>>
		} else { # Retrieve prepared statement <<<
			set stmt_info	[dict get $prepared $sql]
			dict with stmt_info {}
			set heat	[dict get $stmt_info heat]
			if {$heat < 128} {
				incr heat
				dict set prepared $sql heat $heat
			}
			# Sets:
			#	- $stmt_name: the name of the prepared statement (as known to the server)
			#	- $build_params: script to run to gather the input params
			#	- $rformats: the marshalled count and formats that we will send the input params as
			#	- $c_types:	the result column names and the formats that they are transported as
			#	- $columns: a list of column names (can contain duplicates)
			#	- $heat: how frequently this prepared statement has been used recently, relative to others
			#	- $ops_cache: dictionary, keyed by format, of [pgwire::build_ops $format $c_types]
			#	- $param_types: input types
		}
		#>>>

		set stmt_info
	}

	#>>>
	method reprepare sql { # If a prepared statement becomes stale (changed schema, etc), this can be used to replace it <<<
		dict unset prepared $sql
		my prepare_extended_query $sql
	}

	#>>>
	method save_ops {sql format ops} { #<<<
		if {![dict exists $prepared $sql]} return
		dict set prepared $sql ops_cache $format $ops
		set ops
	}

	#>>>
	method _read_batch {} { #<<<
		set datarows	{}

		while 1 {
			if {[binary scan [read $socket 5] aI msgtype len] != 2} {my connection_lost}
			incr len -4	;# len includes itself
			#::pgwire::log notice "_read_batch msg \"$msgtype\", len: $len"
			if {$msgtype eq "D"} { # DataRow <<<
				lappend datarows [read $socket $len]
				if {[eof $socket]} {my connection_lost}
				#>>>
			} else {
				switch -exact -- $msgtype {
					C { # CommandComplete <<<
						set data	[read $socket $len]
						if {[eof $socket]} {my connection_lost}
						if {!$sync_outstanding} {
							puts -nonewline $socket S\u0\u0\u0\u4; set sync_outstanding 1
							flush $socket
						}
						set idx	[string first "\u0" $data]
						if {$idx == -1} {
							#::pgwire::log notice "No c-string found in [regexp -all -inline .. [binary encode hex $data]]"
							throw unterminated_string "No null terminator found"
						}
						set message	[string range $data 0 [- $idx 1]]
						return [list CommandComplete $message $datarows]
						#>>>
					}
					I { # EmptyQueryResponse <<<
						return {EmptyQueryResponse {} {}}
						#>>>
					}
					s { # PortalSuspended <<<
						return [list PortalSuspended [llength $datarows] $datarows]
						#>>>
					}
					E { # ErrorResponse <<<
						my default_message_handler $msgtype $len
						#>>>
					}
					G { # CopyInResponse <<<
						read $socket $len
						if {[eof $socket]} {my connection_lost}
						# Send CopyFail response
						set message	"not supported\u0"
						puts -nonewline $socket [binary format aIa* f [+ 4 [string length $message]] $msg 0]
						#>>>
					}
					H - d - c { # CopyOutResponse / CopyData / CopyDone <<<
						if {$len > 0} {read $socket $len}
						if {[eof $socket]} {my connection_lost}
						#error "CopyOutResponse not supported yet"
						#>>>
					}
					default {my default_message_handler $msgtype $len}
				}
			}
		}
	}

	#>>>
	method _bind_and_execute {stmt_name portal_name rformats parameters max_rows_per_batch} { #<<<
		set enc_stmt	[encoding convertto $tcl_encoding $stmt_name]

		if {$portal_name eq ""} {
			set enc_portal	""
		} else {
			set enc_portal	[encoding convertto $tcl_encoding $portal_name]
		}

		set bindmsg		$enc_portal\u0$enc_stmt\u0$parameters$rformats
		set executemsg	$enc_portal\u0

		if {$max_rows_per_batch == 0} {
			# Simplified flow: no batches, pre-send Sync
			# Bind statment $stmt_name, portal $portal_name
			# Execute $max_rows_per_batch
			# Sync
			puts -nonewline $socket [binary format \
				{ \
					aIa* \
					aIa*I \
					a* \
				} \
					B [+ 4 [string length $bindmsg]] $bindmsg \
					E [+ 8 [string length $executemsg]] $executemsg 0 \
					S\u0\u0\u0\u4 \
			]; set sync_outstanding	1
		} else {
			# Batched flow: batches into $max_rows_per_batch, defer Sync
			# Bind statment $stmt_name, portal $portal_name
			# Execute $max_rows_per_batch
			# Flush
			puts -nonewline $socket [binary format \
				{ \
					aIa* \
					aIa*I \
					a* \
				} \
					B [+ 4 [string length $bindmsg]] $bindmsg \
					E [+ 8 [string length $executemsg]] $executemsg $max_rows_per_batch \
					H\u0\u0\u0\u4 \
			]
		}
		flush $socket
		set ready_for_query	0

		try {
			# Bind responses <<<
			while 1 {
				if {[binary scan [read $socket 5] aI msgtype len] != 2} {my connection_lost}
				incr len -4	;# len includes itself
				#if {[info exists rtt_start]} {
				#	::pgwire::log notice "server RTT: [format %.6f [expr {([clock microseconds] - $rtt_start)/1e6}]]"
				#}
				if {$msgtype eq 2} break else {my default_message_handler $msgtype $len}
			}
			#>>>

			my _read_batch
		} on error {errmsg options} {
			#::pgwire::log notice "error: [dict get $options -errorinfo]"
			if {[info exists socket] && !$sync_outstanding} {
				puts -nonewline $socket S\u0\u0\u0\u4; set sync_outstanding 1
				flush $socket
			}
			return -options $options $errmsg
		} finally {
			if {[info exists socket] && $sync_outstanding} {
				my skip_to_sync
			}
		}
	}

	#>>>
	method _execute {portal_name max_rows_per_batch} { #<<<
		if {$portal_name eq ""} {
			set enc_portal	""
		} else {
			set enc_portal	[encoding convertto $tcl_encoding $portal_name]
		}

		set executemsg	$enc_portal\u0

		if {$max_rows_per_batch == 0} {
			# Simplified flow: no batches, pre-send Sync
			# Execute $max_rows_per_batch
			# Sync
			puts -nonewline $socket [binary format \
				{ \
					aIa*I \
					a* \
				} \
					E [+ 8 [string length $executemsg]] $executemsg 0 \
					S\u0\u0\u0\u4 \
			]; set sync_outstanding	1
		} else {
			# Batched flow: batches into $max_rows_per_batch, defer Sync
			# Execute $max_rows_per_batch
			# Flush
			puts -nonewline $socket [binary format \
				{ \
					aIa*I \
					a* \
				} \
					E [+ 8 [string length $executemsg]] $executemsg $max_rows_per_batch \
					H\u0\u0\u0\u4 \
			]
		}
		flush $socket
		set ready_for_query	0

		try {
			my _read_batch
		} on error {errmsg options} {
			if {[info exists socket] && !$sync_outstanding} {
				puts -nonewline $socket S\u0\u0\u0\u4; set sync_outstanding 1
				flush $socket
			}
			return -options $options $errmsg
		} finally {
			if {[info exists socket] && $sync_outstanding} {
				my skip_to_sync
			}
		}
	}

	#>>>
	method tcl_makerow {as c_types} { #<<<
		my variable makerow_cache
		set key	[list $as $c_types]

		if {[dict exists $makerow_cache $key]} {
			set makerow		[dict get $makerow_cache $key script]
		} else {
			# Compile makerow from c_types, as <<<
			set makerow		{}
			#append makerow	"binary scan \$datarow S col_count\n"	;# $col_count isn't actually used
			append makerow	"set o 2\n"

			switch -exact -- $as {
				dicts - lists - dicts_no_nulls {
					append makerow	{set row {}} \n
				}
				default {
					error "Unhandled result format: \"$as\""
				}
			}

			set remain	[expr {[llength $c_types] / 2}]
			foreach {field_name type_name} $c_types {
				append makerow	"binary scan \$datarow @\${o}I collen; incr o 4\n"
				append makerow	"if {\$collen != -1} \{\n"
				append makerow	[switch -glob -- $type_name {
					bool	{return -level 0 {	binary scan $datarow @${o}c val}}
					int1	{return -level 0 {	binary scan $datarow @${o}c val}}
					smallint -
					int2	{return -level 0 {	binary scan $datarow @${o}S val}}
					int4 -
					integer	{return -level 0 {	binary scan $datarow @${o}I val}}
					bigint -
					int8	{return -level 0 {	binary scan $datarow @${o}W val}}
					bytea	{return -level 0 {	binary scan $datarow @${o}a val}}
					float4	{return -level 0 {	binary scan $datarow @${o}R val}}
					float8	{return -level 0 {	binary scan $datarow @${o}Q val}}
					text {
						string cat {
							if {$collen == 0} {
								set val	{}
							} else {
								set val	[encoding convertfrom $tcl_encoding [string range $datarow $o [expr {$o + $collen -1}]]]
							}
						} \n
					}
					default {
						error "Unhandled type: \"$type_name\""
					}
				}] \n

				switch -exact -- $as {
					dicts - dicts_no_nulls { append makerow	"\tlappend row [list $field_name] \$val\n" }
					lists                  { append makerow	"\tlappend row \$val\n" }
				}

				# Suppress the unnecessary advance of o if this is the last field
				if {[incr remain -1]} {
					append makerow	\t {incr o $collen} \n
				}

				switch -exact -- $as {
					dicts          {}
					dicts_no_nulls { append makerow	"\} else \{\n\tlappend row [list $field_name] {}\n" }
					lists          { append makerow	"\} else \{\n\tlappend row {}\n" }
				}

				append makerow "\}\n"
			}
			# Compile makerow from c_types, as >>>

			#::pgwire::log notice "tcl_makerow $as $c_types:\n$makerow"
			dict set makerow_cache $key script $makerow
		}

		set makerow
	}

	#>>>
	method allrows args { #<<<
		variable ::tdbc::generalError

		set args	[::tdbc::ParseConvenienceArgs $args[set args {}] opts]
		switch -exact -- [llength $args] {
			1 {set sqlcode [lindex $args 0]}
			2 {lassign $args sqlcode param_values}
			default {
				return -code error -errorcode [concat $generalError wrongNumArgs] \
						"wrong # args: should be [lrange [info level 0] 0 1]\
						 ?-option value?... ?--? sqlcode ?dictionary?"
			}
		}
		set as	[dict get $opts -as]

		my buffer_nesting

		if {[dict exists $opts -columnsvariable]} {
			upvar 1 [dict get $opts -columnsvariable] columns
		}

		#set max_rows_per_batch	17
		set max_rows_per_batch	$::pgwire::default_batchsize
		#set max_rows_per_batch	0
		#set max_rows_per_batch	500

		my variable coro_seq
		set rowbuffer	[namespace current]::rowbuffer_coro_[incr coro_seq]

		set retries	1
		while 1 {
			set stmt_info	[my prepare_extended_query $sqlcode]
			dict with stmt_info {}
			# Sets:
			#	- $stmt_name: the name of the prepared statement (as known to the server)
			#	- $field_names:	the result column names and the local variables they are unpacked into
			#	- $build_params: script to run to gather the input params
			#	- $columns: a list of column names (can contain duplicates)
			#	- $heat: how frequently this prepared statement has been used recently, relative to others
			#	- $ops_cache: dictionary, keyed by format, of [pgwire::build_ops $format $c_types]
			try $build_params on ok parameters {}

			if {$::pgwire::accelerators} {
				if {[dict exists $ops_cache $as]} {
					set ops	[dict get $ops_cache $as]
				} else {
					set ops	[::pgwire::build_ops $as $c_types]
					my save_ops $sqlcode $as $ops
				}
			} else {
				set addrow	[format {
					%s
					lappend rows	$row
				} [my tcl_makerow $as $c_types]]
			}

			try {
				coroutine $rowbuffer my rowbuffer_coro $stmt_name "" $rformats $parameters $max_rows_per_batch
			} trap {PGWIRE ErrorResponse ERROR 0A000} {errmsg options} - \
			  trap {PGWIRE ErrorResponse ERROR 42883} {errmsg options} {
				# 0A000 - happens if a schema change alters the result row format
				# 42883 "operator does not exist" - can occur if a schema change altered an expression using bind params
				my close_statement $stmt_name
				dict unset prepared $sqlcode
				if {[incr retries -1] >= 0} {
					continue
				}
				return -options $options $errmsg
			}
			break
		}

		try {
			set rows	{}
			while 1 {
				lassign [$rowbuffer nextbatch] outcome details datarows

				if {$::pgwire::accelerators} {
					switch -exact -- $outcome \
						CommandComplete - \
						PortalSuspended {
							#::pgwire::log notice "before c_allrows_batch: [tcl::unsupported::representation $rows]"
							::pgwire::c_allrows_batch rows $ops $columns $tcl_encoding $datarows
						}
				} else {
					switch -exact -- $outcome \
						CommandComplete - \
						PortalSuspended "
							foreach datarow \$datarows [list $addrow]
						"
				}
				#set h	[open /tmp/datarow wb]
				#puts -nonewline $h $datarow
				#close $h
				#set h	[open /tmp/vars w]
				#puts $h [list \
				#	ops				$ops \
				#	columns			$columns \
				#	tcl_encoding	$tcl_encoding \
				#	c_types			$c_types \
				#]
				#close $h

				switch -exact -- $outcome {
					CommandComplete -
					EmptyQueryResponse {
						break
					}
					PortalSuspended {}
					default {
						error "Unexpected outcome from _read_batch: \"$outcome\""
					}
				}
			}

			set rows
		} finally {
			if {[info exists rowbuffer] && [llength [info commands $rowbuffer]] > 0} {
				$rowbuffer destroy
			}
			if {!$ready_for_query && !$sync_outstanding} {
				#::pgwire::log notice "foreach finally send sync"
				puts -nonewline $socket S\u0\u0\u0\u4; set sync_outstanding 1
				flush $socket
			}
			if {$sync_outstanding} {
				#::pgwire::log notice "foreach finally skip_to_sync"
				my skip_to_sync
			}
		}
	}

	#>>>
	method rowbuffer_coro {stmt_name portal_name rformats parameters batchsize} { #<<<
		set pending_rowbuffer	[info coroutine]

		try {
			#::pgwire::log notice "rowbuffer_coro [info coroutine] bind and execute $batchsize"
			set waiting	[my _bind_and_execute $stmt_name $portal_name $rformats $parameters $batchsize]
			lassign $waiting last_outcome
			#::pgwire::log notice "rowbuffer_coro [info coroutine] last_outcome $last_outcome"
			set res	started

			while 1 {
				set rest	[lassign [yield $res] op]
				#::pgwire::log notice "rowbuffer_coro [info coroutine] op ($op)"
				switch -exact -- $op {
					nextbatch {
						# If we already have a result waiting, return that
						if {[info exists waiting]} {
							set res	$waiting
							unset waiting
							continue
						}

						if {$last_outcome eq "PortalSuspended"} {
							#::pgwire::log notice "rowbuffer_coro [info coroutine] execute $batchsize"
							set res [my _execute $portal_name $batchsize]
							lassign $res last_outcome
							#::pgwire::log notice "rowbuffer_coro [info coroutine] last_outcome $last_outcome"
							if {$last_outcome ne "PortalSuspended"} {
								unset -nocomplain pending_rowbuffer
							}
							continue
						} else {
							error "No waiting results, and last outcome was \"$last_outcome\""
						}
					}

					preread_all {
						# We're being asked to read all our results and get out of the way (by an inner query)
						if {$last_outcome eq "PortalSuspended"} {
							#::pgwire::log notice "rowbuffer_coro [info coroutine] execute all remaining"
							set waiting [my _execute $portal_name 0]
							lassign $waiting outcome
							#::pgwire::log notice "rowbuffer_coro [info coroutine] last_outcome $last_outcome"
							unset -nocomplain pending_rowbuffer
							set res	preread_ok
						} else {
							set res preread_na
						}
					}

					destroy {
						return
					}
				}
			}
		} finally {
			if {[info exists pending_rowbuffer] && $pending_rowbuffer eq [info coroutine]} {
				unset pending_rowbuffer
			}
			if {[info exists socket]} {
				if {!$ready_for_query && !$sync_outstanding} {
					#::pgwire::log notice "rowbuffer_coro [info coroutine] send sync"
					puts -nonewline $socket S\u0\u0\u0\u4; set sync_outstanding 1
					flush $socket
				}
				if {$sync_outstanding} {
					#::pgwire::log notice "rowbuffer_coro [info coroutine] skip_to_sync"
					my skip_to_sync
				}
				if {$portal_name ne ""} {
					my close_portal $portal_name
				}
			}
			#::pgwire::log notice "rowbuffer_coro [info coroutine] exiting"
		}
	}

	#>>>
	method foreach args { #<<<
		variable ::tdbc::generalError

		set args	[::tdbc::ParseConvenienceArgs $args[set args {}] opts]
		switch -exact -- [llength $args] {
			3 {lassign $args row_varname sqlcode script}
			4 {lassign $args row_varname sqlcode param_values script}
			default {
				return -code error -errorcode [concat $generalError wrongNumArgs] \
						"wrong # args: should be [lrange [info level 0] 0 1]\
						 ?-option value?... ?--? varName sqlcode ?dictionary? script"
			}
		}
		set as	[dict get $opts -as]

		my buffer_nesting

		if {[dict exists $opts -columnsvariable]} {
			upvar 1 [dict get $opts -columnsvariable] columns
		}

		#set max_rows_per_batch	17
		set max_rows_per_batch	$::pgwire::default_batchsize
		#set max_rows_per_batch	0
		#set max_rows_per_batch	500

		my variable coro_seq
		set rowbuffer	[namespace current]::rowbuffer_coro_[incr coro_seq]

		set retries	1
		while 1 {
			set stmt_info	[my prepare_extended_query $sqlcode]
			dict with stmt_info {}
			# Sets:
			#	- $stmt_name: the name of the prepared statement (as known to the server)
			#	- $field_names:	the result column names and the local variables they are unpacked into
			#	- $build_params: script to run to gather the input params
			#	- $columns: a list of column names (can contain duplicates)
			#	- $heat: how frequently this prepared statement has been used recently, relative to others
			#	- $ops_cache: dictionary, keyed by format, of [pgwire::build_ops $format $c_types]

			try $build_params on ok parameters {}

			try {
				if {$::pgwire::accelerators} {
					if {[dict exists $ops_cache $as]} {
						set ops	[dict get $ops_cache $as]
					} else {
						set ops	[::pgwire::build_ops $as $c_types]
						my save_ops $sqlcode $as $ops
					}
					set foreach_batch {
						uplevel 1 [list ::pgwire::c_foreach_batch_nr $row_varname $ops $columns $tcl_encoding $datarows $script]
					}
				} else {
					upvar 1 $row_varname row
					#set makerow	[my tcl_makerow $as $c_types]
					set foreach_batch [string map [list \
						%makerow%		[my tcl_makerow $as $c_types] \
						%script%		[list $script] \
					] {
						foreach datarow $datarows {
							%makerow%
							try {
								uplevel 1 %script%
							} on break {} {
								set broken	1
								break
							} on continue {} {
							} on return {r o} {
								#::pgwire::log notice "foreach script caught return r: ($r), o: ($o)"
								set broken	1
								dict incr o -level 1
								dict set o -code return
								set rethrow	[list -options $o $r]
								break
							} on error {r o} {
								#::pgwire::log notice "foreach script caught error r: ($r), o: ($o)"
								set broken	1
								dict incr o -level 1
								set rethrow	[list -options $o $r]
								break
							}
						}
					}]
				}
				coroutine $rowbuffer my rowbuffer_coro \
					$stmt_name $rowbuffer $rformats $parameters $max_rows_per_batch
			} trap {PGWIRE ErrorResponse ERROR 0A000} {errmsg options} - \
			  trap {PGWIRE ErrorResponse ERROR 42883} {errmsg options} {
				# 0A000 - happens if a schema change alters the result row format
				# 42883 "operator does not exist" - can occur if a schema change altered an expression using bind params
				my close_statement $stmt_name
				dict unset prepared $sqlcode
				if {[incr retries -1] >= 0} {
					continue
				}
				return -options $options $errmsg
			}
			break
		}

		try {
			set broken	0
			while {!$broken} {
				lassign [$rowbuffer nextbatch] outcome details datarows

				try $foreach_batch

				switch -exact -- $outcome {
					CommandComplete -
					EmptyQueryResponse {
						break
					}
					PortalSuspended {}
					default {
						error "Unexpected outcome from _read_batch: \"$outcome\""
					}
				}
			}

			if {[info exists rethrow]} {
				#::pgwire::log notice "rethrowing"
				return {*}$rethrow
			}
		} finally {
			if {[info exists rowbuffer] && [llength [info commands $rowbuffer]] > 0} {
				$rowbuffer destroy
			}
			if {!$ready_for_query && !$sync_outstanding} {
				#::pgwire::log notice "foreach finally send sync"
				puts -nonewline $socket S\u0\u0\u0\u4; set sync_outstanding 1
				flush $socket
			}
			if {$sync_outstanding} {
				#::pgwire::log notice "foreach finally skip_to_sync"
				my skip_to_sync
			}
		}
	}

	#>>>
	method onecolumn sql { #<<<
		variable ::tdbc::generalError

		my buffer_nesting

		set retries	1
		while 1 {
			try {
				set stmt_info	[my prepare_extended_query $sql]
				dict with stmt_info {}
				# Sets:
				#	- $stmt_name: the name of the prepared statement (as known to the server)
				#	- $field_names:	the result column names and the local variables they are unpacked into
				#	- $build_params: script to run to gather the input params
				#	- $columns: a list of column names (can contain duplicates)
				#	- $heat: how frequently this prepared statement has been used recently, relative to others
				#	- $ops_cache: dictionary, keyed by format, of [pgwire::build_ops $format $c_types]

				try $build_params on ok parameters {}

				set max_rows_per_batch	0

				if {$::pgwire::accelerators} {
					if {[dict exists $ops_cache lists]} {
						set ops	[dict get $ops_cache lists]
					} else {
						set ops	[::pgwire::build_ops lists $c_types]
						my save_ops $sql lists $ops
					}
				} else {
					set makerow	[my tcl_makerow lists $c_types]
				}

				lassign [my _bind_and_execute $stmt_name "" $rformats $parameters $max_rows_per_batch] \
					outcome details datarows
			} trap {PGWIRE ErrorResponse ERROR 0A000} {errmsg options} - \
			  trap {PGWIRE ErrorResponse ERROR 42883} {errmsg options} {
				# 0A000 - happens if a schema change alters the result row format
				# 42883 "operator does not exist" - can occur if a schema change altered an expression using bind params
				my close_statement $stmt_name
				dict unset prepared $sql
				if {[incr retries -1] >= 0} {
					continue
				}
				return -options $options $errmsg
			}
			break
		}

		try {
			if {$::pgwire::accelerators} {
				switch -exact -- $outcome {
					CommandComplete -
					PortalSuspended {
						if {[llength $datarows] > 0} {
							return [lindex [::pgwire::c_makerow2 $ops $columns $tcl_encoding [lindex $datarows 0]] 0]
						} else {
							return {}
						}
					}
					EmptyQueryResponse {
						return {}
					}
				}
			} else {
				switch -exact -- $outcome {
					CommandComplete -
					PortalSuspended {
						if {[llength $datarows] > 0} {
							set datarow	[lindex $datarows 0]
							try $makerow
							return [lindex $row 0]
						} else {
							return {}
						}
					}
					EmptyQueryResponse {
						return {}
					}
				}
			}
		} finally {
			if {!$ready_for_query && !$sync_outstanding} {
				puts -nonewline $socket S\u0\u0\u0\u4; set sync_outstanding 1
				flush $socket
			}
			if {$sync_outstanding} {
				my skip_to_sync
			}
		}

	}

	#>>>

	method transaction_status {} { set transaction_status }
	method ready_for_query {} { set ready_for_query }
	method buffer_nesting {} {
		if {$transaction_status eq "I" && [info exists pending_rowbuffer]} {
			$pending_rowbuffer preread_all
		}
	}
}

# vim: foldmethod=marker foldmarker=<<<,>>> ts=4 shiftwidth=4
