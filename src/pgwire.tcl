package require Thread

set block_accelerators	[info exists ::pgwire::block_accelerators]

if {[info object isa object ::pgwire]} {
	::pgwire destroy
}

if {[namespace exists ::pgwire]} {
	namespace delete ::pgwire
}
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
}

if {$block_accelerators} {
	namespace eval ::pgwire {variable block_accelerators 1}
}

#>>>

set ::pgwire::accelerators	0
if {!$block_accelerators} {
if 1 {
try {
	package require critcl
} on ok ver {
	#::pgwire::log notice "Have critcl $ver"
	# With a little help from my friends (c) <<<
	if {[critcl::compiling]} {
		#::pgwire::log notice "critcl::compiling: [critcl::compiling]"
		#critcl::cheaders -I/path/to/headers/
		#critcl::tcl [info tclversion]
		critcl::tcl 8.6
		#critcl::debug memory
		critcl::debug symbols
		critcl::cflags -O2 -g
		#critcl::cflags -O3 -march=native
		critcl::ccode {
			#include <byteswap.h>
			#include <stdint.h>
			#include <string.h>
		}
		critcl::ccommand ::pgwire::c_makerow {cdata interp objc objv} { //<<<
			const char* restrict		data = NULL;
			int							data_len;
			Tcl_Obj**					c_types = NULL;
			int							c_types_len;
			const uint16_t*	restrict	rformats = NULL;
			int							rformats_len;
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

			if (objc != 6) {
				Tcl_WrongNumArgs(interp, 1, objv, "data c_types rformats tcl_encoding format");
				return TCL_ERROR;
			}

			p = data = Tcl_GetByteArrayFromObj(objv[1], &data_len);
			e = p + data_len;
			if (Tcl_ListObjGetElements(interp, objv[2], &c_types_len, &c_types) != TCL_OK)
				return TCL_ERROR;
			rformats = (const uint16_t* restrict)Tcl_GetByteArrayFromObj(objv[3], &rformats_len);
			if (Tcl_GetEncodingFromObj(interp, objv[4], &encoding) != TCL_OK)
				return TCL_ERROR;
			if (Tcl_GetIndexFromObj(interp, objv[5], formats, "format", TCL_EXACT, &format) != TCL_OK)
				return TCL_ERROR;

			if (c_types_len % 2 != 0) {
				Tcl_SetObjResult(interp, Tcl_ObjPrintf("c_types must be an even number of elements"));
				return TCL_ERROR;
			}

			if (rformats_len-2 != c_types_len) {
				Tcl_SetObjResult(interp, Tcl_ObjPrintf("rformats must be two bytes long for each column, plus 2 bytes, have %d for %d columns", rformats_len, c_types_len/2));
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

					/* rformats[c] is byteswapped (big endian), but 0 is still 0 in either byte order */
					if (rformats[c] == 0) {
						/* Text */
						Tcl_DString		utf8;

						Tcl_DStringInit(&utf8);

						Tcl_ExternalToUtfDString(encoding, p, collen, &utf8);
						Tcl_IncrRefCount(rowv[rs++] = Tcl_NewStringObj(Tcl_DStringValue(&utf8), Tcl_DStringLength(&utf8)));
						Tcl_DStringFree(&utf8);
					} else {
						/* Binary */
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
								Tcl_IncrRefCount(rowv[rs++] = Tcl_NewIntObj(bswap_16(*(uint16_t*)p)));	// Signed?
								break;
							case TYPE_INT4:
							case TYPE_INTEGER:
								Tcl_IncrRefCount(rowv[rs++] = Tcl_NewIntObj(bswap_32(*(int32_t*)p)));	// Signed?
								break;
							case TYPE_BIGINT:
							case TYPE_INT8:
								Tcl_IncrRefCount(rowv[rs++] = Tcl_NewIntObj(bswap_64(*(uint64_t*)p)));	// Signed?
								break;
							case TYPE_BYTEA:
								Tcl_IncrRefCount(rowv[rs++] = Tcl_NewByteArrayObj(p, collen));
								break;
							case TYPE_FLOAT4:
								{
									char buf[4];
									*(uint32_t*)buf = bswap_32(*(uint32_t*)p);
									Tcl_IncrRefCount(rowv[rs++] = Tcl_NewDoubleObj(*(float*)buf));
								}
								break;
							case TYPE_FLOAT8:
								{
									char buf[8];
									*(uint64_t*)buf = bswap_64(*(uint64_t*)p);
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
		#>>>

		# Force compile and load.  Not necessary, without it commands will lazy compile and load when called
		critcl::load
	}
	# With a little help from my friends (c) >>>
	set ::pgwire::accelerators	1
}
} else {
try {
	package require tcc4tcl
} on ok ver {
	::pgwire::log notice "Have tcc4tcl $ver"
	set cc	[tcc4tcl::new]
	# With a little help from my friends (c) <<<
	#::pgwire::log notice "critcl::compiling: [critcl::compiling]"
	$cc add_include_path /usr/include
	$cc ccode {
		#include <byteswap.h>
		#include <stdint.h>
		#include <string.h>

		int c_makerow(ClientData cdata, Tcl_Interp* interp, int objc, Tcl_Obj *const objv[]) //<<<
		{
			const char* restrict		data = NULL;
			int							data_len;
			Tcl_Obj**					c_types;
			int							c_types_len;
			const uint16_t*	restrict	rformats = NULL;
			int							rformats_len;
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
				(char*)NULL
			};
			int format;
			enum {
				FORMAT_DICTS,
				FORMAT_LISTS
			};

			if (objc != 6) {
				Tcl_WrongNumArgs(interp, 1, objv, "data c_types rformats tcl_encoding format");
				return TCL_ERROR;
			}

			p = data = Tcl_GetByteArrayFromObj(objv[1], &data_len);
			e = p + data_len;
			if (Tcl_ListObjGetElements(interp, objv[2], &c_types_len, &c_types) != TCL_OK)
				return TCL_ERROR;
			rformats = (const uint16_t* restrict)Tcl_GetByteArrayFromObj(objv[3], &rformats_len);
			if (Tcl_GetEncodingFromObj(interp, objv[4], &encoding) != TCL_OK)
				return TCL_ERROR;
			if (Tcl_GetIndexFromObj(interp, objv[5], formats, "format", TCL_EXACT, &format) != TCL_OK)
				return TCL_ERROR;

			if (c_types_len % 2 != 0) {
				Tcl_SetObjResult(interp, Tcl_ObjPrintf("c_types must be an even number of elements"));
				return TCL_ERROR;
			}

			if (rformats_len-2 != c_types_len) {
				Tcl_SetObjResult(interp, Tcl_ObjPrintf("rformats must be two bytes long for each column, plus 2 bytes, have %d for %d columns", rformats_len, c_types_len/2));
				return TCL_ERROR;
			}

			if (data_len < 4) {
				Tcl_SetObjResult(interp, Tcl_ObjPrintf("data is too short"));
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

				Tcl_IncrRefCount(nullobj = Tcl_NewStringObj("", 0));	/* TODO: store a static null obj in an interp AssocData */

				/* Zero the pointers, so that we don't leak partial rows of Tcl_Objs on error */
				memset(rowv, 0, sizeof(Tcl_Obj*) * rslots);

				for (i=0, c=1, rs=0; i<c_types_len; i+=2, c++) {
					const int collen = bswap_32(*(uint32_t*)p);

					p += 4;

					if (collen == -1) {
						/* NULL */
						if (format == FORMAT_LISTS)
							Tcl_IncrRefCount(rowv[rs++] = nullobj);
						continue;
					}

					if (e-p < collen) {
						Tcl_SetObjResult(interp, Tcl_ObjPrintf("Insufficient data for column %d: need %d but %ld remain", i/2, collen, e-p));
						retcode = TCL_ERROR;
						goto done;
					}

					if (format == FORMAT_DICTS) {
						Tcl_IncrRefCount(rowv[rs++] = c_types[i]);
					}

					/* rformats[c] is byteswapped (big endian), but 0 is still 0 in either byte order */
					if (rformats[c] == 0) {
						/* Text */
						Tcl_DString		utf8;

						Tcl_DStringInit(&utf8);

						Tcl_ExternalToUtfDString(encoding, p, collen, &utf8);
						Tcl_IncrRefCount(rowv[rs++] = Tcl_NewStringObj(Tcl_DStringValue(&utf8), Tcl_DStringLength(&utf8)));
						Tcl_DStringFree(&utf8);
					} else {
						/* Binary */
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
								Tcl_IncrRefCount(rowv[rs++] = Tcl_NewIntObj(bswap_16(*(uint16_t*)p)));	// Signed?
								break;
							case TYPE_INT4:
							case TYPE_INTEGER:
								Tcl_IncrRefCount(rowv[rs++] = Tcl_NewIntObj(bswap_32(*(int32_t*)p)));	// Signed?
								break;
							case TYPE_BIGINT:
							case TYPE_INT8:
								Tcl_IncrRefCount(rowv[rs++] = Tcl_NewIntObj(bswap_64(*(uint64_t*)p)));	// Signed?
								break;
							case TYPE_BYTEA:
								Tcl_IncrRefCount(rowv[rs++] = Tcl_NewByteArrayObj(p, collen));
								break;
							case TYPE_FLOAT4:
								{
									char buf[4];
									*(uint32_t*)buf = bswap_32(*(uint32_t*)p);
									Tcl_IncrRefCount(rowv[rs++] = Tcl_NewDoubleObj(*(float*)buf));
								}
								break;
							case TYPE_FLOAT8:
								{
									char buf[8];
									*(uint64_t*)buf = bswap_64(*(uint64_t*)p);
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
	}

	#::pgwire::log notice "tcc c code: [$cc code]"

	$cc linktclcommand ::pgwire::c_makerow c_makerow

	::pgwire::log notice "build tcc accelerators: [timerate {
		$cc go
	} 1 1]"

	# With a little help from my friends (c) >>>
	set ::pgwire::accelerators	1
}
}
} else {
	puts stderr "Not using accelerators"
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

			4 {
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
	method connect {chan db user password} { #<<<
		set socket			$chan
		set name_seq		0
		set prepared		{}
		set ready_for_query	0
		set age_count		0

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

		my _startup $db $user $password
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
				prepared			[dict map {k v} $prepared {dict merge $v {cached {}}}] \
				ready_for_query		$ready_for_query \
				age_count			$age_count
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
			puts -nonewline $socket S\u0\u0\u0\u4
			flush $socket
			while 1 {
				if {[binary scan [read $socket 5] aI msgtype len] != 2} {my connection_lost}
				incr len -4	;# len includes itself

				switch -exact -- $msgtype {
					Z { # ReadyForQuery <<<
						set data	[read $socket $len]
						if {[eof $socket]} {my connection_lost}
						binary scan $data a transaction_status
						set ready_for_query	1
						break
						#>>>
					}

					Z - R - E - S - K - C - D - T - t - 1 - 2 - n - s {
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
	method _startup {db user password} { #<<<
		set payload	[binary format I [expr {
			3 << 16 | 0
		}]]	;# Version 3.0

		foreach {k v} [list \
			user						$user \
			database					$db \
			client_encoding				UTF8 \
			standard_conforming_strings	on \
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
					set ready_for_query	1
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
	method prepare_statement {stmt_name sql} { #<<<
		try {
			lassign [my tokenize $sql] \
				params_assigned \
				compiled

			# Parse, to $stmt_name
			# Describe S $stmt_name
			# Flush
			set parsemsg	[encoding convertto $tcl_encoding $stmt_name]\u0[encoding convertto $tcl_encoding $compiled]\u0\u0\u0
			set describemsg	S[encoding convertto $tcl_encoding $stmt_name]\u0
			set ready_for_query	0
			set busy_sql $sql
			puts -nonewline $socket [binary format aI P [+ 4 [string length $parsemsg]]]$parsemsg[binary format aI D [expr {4+[string length $describemsg]}]]${describemsg}S\u0\u0\u0\u4

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
			set field_names		{}
			set param_desc		{}
			while 1 {
				if {[binary scan [read $socket 5] aI msgtype len] != 2} {my connection_lost}
				incr len -4	;# len includes itself

				switch -exact -- $msgtype {
					t { # ParameterDescription <<<
						binary scan [read $socket $len] SI* count parameter_type_oids
						if {[eof $socket]} {my connection_lost}

						set build_params	{
							set pdesc		{}
							set pformats	{}
						}
						append build_params [list set pcount [llength $parameter_type_oids]] \n
						set build_params_vars	{}
						set build_params_dict	{}

						set param_seq	0
						foreach oid $parameter_type_oids name [dict keys $params_assigned] {
							set type_name	[dict get $type_oids $oid]

							set encode_field	[switch -exact -- [dict get $type_oids $oid] {
								bool	{return -level 0 {append pformats \u0\u1; append pdesc [binary format Ic 1 [expr {!!($value)}]]}}
								int1	{return -level 0 {append pformats \u0\u1; append pdesc [binary format Ic 1 $value]}}
								smallint -
								int2	{return -level 0 {append pformats \u0\u1; append pdesc [binary format IS 1 $value]}}
								int4 -
								integer	{return -level 0 {append pformats \u0\u1; append pdesc [binary format II 4 $value]}}
								bigint -
								int8	{return -level 0 {append pformats \u0\u1; append pdesc [binary format IW 8 $value]}}
								bytea	{return -level 0 {append pformats \u0\u1; append pdesc [binary format Ia* [string length $value] $value]}}
								float4	{return -level 0 {append pformats \u0\u1; append pdesc [binary format IR 4 $value]}}
								float8	{return -level 0 {append pformats \u0\u1; append pdesc [binary format IQ 8 $value]}}
								varchar -
								default {
									return -level 0 {
										set bytes	[encoding convertto $tcl_encoding $value]
										set bytelen	[string length $bytes]
										append pformats \u0\u0; append pdesc [binary format Ia$bytelen $bytelen $bytes]
									}
								}
							}]

							append build_params_vars	[string map [list \
								%v%				[list $name] \
								%null%			[list [binary format I -1]] \
								%encode_field%	$encode_field \
							] {
								if {[uplevel 1 {info exists %v%}]} {
									set value	[uplevel 1 {set %v%}]
									%encode_field%
								} else {
									append pdesc	%null%
									append pformats	\u0\u1	;# binary
								}
							}]
							append build_params_dict	[string map [list \
								%v%				[list $name] \
								%null%			[list [binary format I -1]] \
								%encode_field%	$encode_field \
							] {
								if {[dict exists $param_values %v%]} {
									set value	[dict get $param_values %v%]
									%encode_field%
								} else {
									append pdesc	%null%
									append pformats	\u0\u1	;# binary
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
					T { # RowDescription <<<
						set makerow_vars			"set o 0\n"
						set makerow_dict			"set o 0\n"
						set makerow_dict_no_nulls	"set o 0\n"
						set makerow_list			"set o 0\n"
						set colnum	0

						append makerow_vars				{binary scan $data @${o}S col_count; incr o 2} \n
						append makerow_dict				{binary scan $data @${o}S col_count; incr o 2} \n {set row {}} \n
						append makerow_dict_no_nulls	{binary scan $data @${o}S col_count; incr o 2} \n {set row {}} \n
						append makerow_list				{binary scan $data @${o}S col_count; incr o 2} \n {set row {}} \n

						set data	[read $socket $len]
						if {[eof $socket]} {my connection_lost}

						binary scan $data S fields_per_row
						set i	2
						set c_types		{}
						set rformats	[binary format Su $fields_per_row]
						for {set c 0} {$c < $fields_per_row} {incr c} {
							set idx	[string first \u0 $data $i]
							if {$idx == -1} {
								#::pgwire::log notice "No c-string found in [regexp -all -inline .. [binary encode hex $data]]"
								throw unterminated_string "No null terminator found"
							}
							set field_name	[string range $data $i [- $idx 1]]
							set i	[+ $idx 1]
							incr colnum
							set myname	field_$colnum
							lappend field_names	$field_name field_$colnum

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
							set setvar		{}
							if {[dict exists $type_oids $type_oid]} {
								set type_name	[dict get $type_oids $type_oid]
								set colfmt		\u0\u1	;# binary
								append setvar [switch -glob -- $type_name {
									bool	{format {binary scan $data @${o}c %s} [list $myname]}
									int1	{format {binary scan $data @${o}c %s} [list $myname]}
									smallint -
									int2	{format {binary scan $data @${o}S %s} [list $myname]}
									int4 -
									integer	{format {binary scan $data @${o}I %s} [list $myname]}
									bigint -
									int8	{format {binary scan $data @${o}W %s} [list $myname]}
									bytea	{format {binary scan $data @${o}a %s} [list $myname]}
									float4	{format {binary scan $data @${o}R %s} [list $myname]}
									float8	{format {binary scan $data @${o}Q %s} [list $myname]}
									default {
										set colfmt		\u0\u0	;# text
										set type_name	text
										format {set %s [if {$collen == 0} {
											return -level 0 {}
										} else {
											encoding convertfrom %s [string range $data $o [+ $o $collen -1]]
										}]} [list $myname] [list $tcl_encoding]
									}
								}]

								append rformats	$colfmt
								lappend c_types $field_name $type_name
							} else {
								append setvar	[format {set %s [if {$collen == 0} {
									return -level 0 {}
								} else {
									encoding convertfrom %s [string range $data $o [+ $o $collen -1]]
								}]} [list $myname] [list $tcl_encoding]]
								append rformats	\u0\u0	;# text
								lappend c_types $field_name text
							}

							if {![regexp "^set [list $myname] \\\[(.*)\\\]$" $setvar - set_res]} {
								set set_res	"$setvar; set [list $myname]"
							}

							set makerow_prefix	"binary scan \$data @\${o}I collen; incr o 4\n"
							append makerow_vars $makerow_prefix "if {\$collen == -1} {unset -nocomplain [list $myname]} else {$setvar\nincr o \$collen\n}\n"
							append makerow_dict $makerow_prefix "if {\$collen != -1} {dict set row [list $field_name] \[$set_res\]\nincr o \$collen\n}\n"
							append makerow_dict_no_nulls $makerow_prefix "if {\$collen != -1} {dict set row [list $field_name] \[$set_res\]\nincr o \$collen\n} else {dict set row [list $field_name] {}}\n"
							append makerow_list $makerow_prefix "if {\$collen == -1} {lappend row {}} else {lappend row \[$set_res\]\nincr o \$collen\n}\n"
						}
						#>>>
					}
					n { # NoData <<<
						set rformats		\u0\u0
						set makerow_vars	{}
						set makerow_dict	{}
						set makerow_dict_no_nulls	{}
						set makerow_list	{}
						set c_types			{}
						#>>>
					}
					Z { # ReadyForQuery <<<
						set data	[read $socket $len]
						if {[eof $socket]} {my connection_lost}
						binary scan $data a transaction_status
						set ready_for_query	1
						break
						#>>>
					}
					default {my default_message_handler $msgtype $len}
				}
			}
			#>>>
			# Build execute script <<<
			set desc	"# SQL extended-query execute script for:\n# [join [split $compiled \n] "\n# "]\n" 
			append desc "# - Parameters ------------------------\n$param_desc"
			append desc "# - Explicit prepared statement: $stmt_name\n"

			set execute [string map [list \
				%desc%				$desc \
				%stmt_name%			[list $stmt_name] \
				%field_names%		[list $field_names] \
				%rformats%			[list $rformats] \
				%c_types%			[list $c_types] \
				%sql%				[list $sql] \
			] {%desc%
				my variable tcl_encoding
				my variable transaction_status
				my variable ready_for_query
				my variable busy_sql
				set field_names	%field_names%

				set c_types		%c_types%

				set ready_for_query	0
				set busy_sql	%sql%
				set rformats	%rformats%
				set enc_portal	[encoding convertto $tcl_encoding $portal_name]
				set enc_stmt	[encoding convertto $tcl_encoding %stmt_name%]
				set executemsg	$enc_portal\u0
				#::pgwire::log notice "Executing portal: ($portal_name)"
				if {$bind} {
					set bindmsg		$enc_portal\u0$enc_stmt\u0$parameters$rformats
					# Bind statment $stmt_name, default portal
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
				} else {
					# Continue an open portal
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
				#set rtt_start	[clock microseconds]
				yield [list [dict keys $field_names] $rformats $c_types]	;# Attempt to hide some of the setup time in the first RTT to the server

				try {
					if {$bind} {
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
					}
					# Execute responses <<<
					while 1 {
						if {[binary scan [read $socket 5] aI msgtype len] != 2} {my connection_lost}
						incr len -4	;# len includes itself
						#::pgwire::log notice "prepared statement \"%stmt_name%\" read msg \"$msgtype\", len: $len"
						if {$msgtype eq "D"} { # DataRow <<<
							yield [list DataRow $len]
							#>>>
						} else {
							switch -exact -- $msgtype {
								C { # CommandComplete <<<
									set data	[read $socket $len]
									if {[eof $socket]} {my connection_lost}
									set idx	[string first "\u0" $data]
									if {$idx == -1} {
										#::pgwire::log notice "No c-string found in [regexp -all -inline .. [binary encode hex $data]]"
										throw unterminated_string "No null terminator found"
									}
									set message	[string range $data 0 [- $idx 1]]
									puts -nonewline $socket S\u0\u0\u0\u4
									flush $socket
									my read_to_sync
									yield [list CommandComplete $message]
									break
									#>>>
								}
								I { # EmptyQueryResponse <<<
									puts -nonewline $socket S\u0\u0\u0\u4
									flush $socket
									my read_to_sync
									break
									#>>>
								}
								s { # PortalSuspended <<<
									# TODO: wait for ReadyForQuery?
									yield [list PortalSuspended $executemsg]
									break
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
								ErrorResponse { #<<<
									::pgwire::log notice "portal coroutine got ErrorResponse, sending Sync"
									puts -nonewline $socket S\u0\u0\u0\u4
									flush $socket
									try {
										::pgwire::log notice "dispatching to default_message_handler"
										my default_message_handler $msgtype $len
									} finally {
										::pgwire::log notice "skip_to_sync"
										my skip_to_sync
										::pgwire::log notice "back from skip_to_sync"
									}
									#>>>
								}
								default {my default_message_handler $msgtype $len}
							}
						}
					}
					#>>>
				} on error {errmsg options} {
					set rethrow	[list -options $options $errmsg]
					::pgwire::log error "Error during execute: $options"
					#puts -nonewline $socket S\u0\u0\u0\u4
					#flush $socket
					::pgwire::log notice "portal on error skip_to_sync"
					my skip_to_sync
					::pgwire::log notice "back from skip_to_sync"
				}

				if {[info exists rethrow]} {
					::pgwire::log notice "rethrowing"
					return {*}$rethrow
				}

				return finished
			}]
			# Build execute script >>>
			set cached		{}
			#::pgwire::log notice "execute script:\n$execute\nmakerow_vars: $makerow_vars\nmakerow_dict: $makerow_dict\nmakerow_list: $makerow_list"
			# Results:
			#	- $field_names:	the result column names and the local variables they are unpacked into
			#	- $execute: script to run to execute the statement
			#	- $makerow_vars
			#	- $makerow_dict
			#	- $makerow_list
			dict create \
				field_names			$field_names \
				socket				$socket \
				execute_lambda		[list {socket portal_name max_rows_per_batch bind parameters} $execute [self namespace]] \
				makerow_vars		$makerow_vars \
				makerow_dict		$makerow_dict \
				makerow_dict_no_nulls	$makerow_dict_no_nulls \
				makerow_list		$makerow_list \
				build_params		$build_params
			#::pgwire::log notice "Finished preparing statement, execute:\n$execute"
		} on error {errmsg options} { #<<<
			#::pgwire::log error "Error preparing statement, closing and syncing: [dict get $options -errorinfo]"
			if {[info exists socket]} {
				if {!$ready_for_query} {
					my skip_to_sync
				}
				# Close statement $stmt_name
				set close_payload	S[encoding convertto $tcl_encoding $stmt_name]\u0
				puts -nonewline $socket [binary format aIa* C [+ 4 [string length $close_payload]] $close_payload]S\u0\u0\u0\u4
				flush $socket
				my skip_to_sync
			}
			return -options $options $errmsg
		}
		#>>>
	}

	#>>>
	method read_to_sync {} { #<<<
		while 1 {
			if {[binary scan [read $socket 5] aI msgtype len] != 2} {my connection_lost}
			incr len -4
			if {$msgtype eq "Z"} { # ReadyForQuery
				if {[binary scan [read $socket $len] a transaction_status] != 1} {my connection_lost}
				set ready_for_query	1
				break
			} else {
				my default_message_handler $msgtype $len
			}
		}
		set transaction_status
	}

	#>>>
	method sync {} { #<<<
		puts -nonewline $socket S\u0\u0\u0\u4
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
						set ready_for_query	1
						break
						#>>>
					}

					Z - R - E - S - K - C - D - T - t - 1 - 2 - n - s {
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
	method tcl_encoding {} { set tcl_encoding }

	method Flush {} { #<<<
		puts -nonewline $socket H\u0\u0\u0\u4
		flush $socket
	}

	#>>>
	method Sync {} { #<<<
		puts -nonewline $socket S\u0\u0\u0\u4
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
	method begintransaction {} { #<<<
		tailcall my extended_query begin {set execute_setup {set acc {}}; set makerow {}; set on_row {}}
	}

	#>>>
	method rollback {} { #<<<
		if {$transaction_status ne "I"} {
			tailcall my extended_query rollback {set execute_setup {set acc {}}; set makerow {}; set on_row {}}
		}
	}

	#>>>
	method commit {} { #<<<
		if {$transaction_status ne "I"} {
			tailcall my extended_query commit {set execute_setup {set acc {}}; set makerow {}; set on_row {}}
		}
	}

	#>>>
	method simple_query_dict {rowdict sql on_row} { #<<<
		if {!$ready_for_query} {
			if {!$ready_for_query} {
				error "Re-entrant query while another is processing: $sql while processing: $busy_sql"
			}
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
	method extended_query {sql variant_setup {max_rows_per_batch 0} args} { #<<<
		if {!$ready_for_query} {
			error "Re-entrant query while another is processing: $sql while processing: $busy_sql"
		}
		set busy_sql	$sql
		switch -exact -- [llength $args] {
			0 {}
			1 {set param_values	[lindex $args 0]}
			default {
				error "Too many arguments, must be sql variant_setup ?max_rows_per_batch? ?param_values?"
			}
		}

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
				set ready_for_query	0
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
				set field_names		{}
				set param_desc		{}
				while 1 {
					if {[binary scan [read $socket 5] aI msgtype len] != 2} {my connection_lost}
					incr len -4	;# len includes itself

					switch -exact -- $msgtype {
						t { # ParameterDescription <<<
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
								set type_name	[dict get $type_oids $oid]

								set encode_field	[switch -exact -- [dict get $type_oids $oid] {
									bool	{return -level 0 {append pformats \u0\u1; append pdesc [binary format Ic 1 [expr {!!($value)}]]}}
									int1	{return -level 0 {append pformats \u0\u1; append pdesc [binary format Ic 1 $value]}}
									smallint -
									int2	{return -level 0 {append pformats \u0\u1; append pdesc [binary format IS 1 $value]}}
									int4 -
									integer	{return -level 0 {append pformats \u0\u1; append pdesc [binary format II 4 $value]}}
									bigint -
									int8	{return -level 0 {append pformats \u0\u1; append pdesc [binary format IW 8 $value]}}
									bytea	{return -level 0 {append pformats \u0\u1; append pdesc [binary format Ia* [string length $value] $value]}}
									float4	{return -level 0 {append pformats \u0\u1; append pdesc [binary format IR 4 $value]}}
									float8	{return -level 0 {append pformats \u0\u1; append pdesc [binary format IQ 8 $value]}}
									varchar -
									default {
										return -level 0 {
											set bytes	[encoding convertto $tcl_encoding $value]
											set bytelen	[string length $bytes]
											append pformats \u0\u0; append pdesc [binary format Ia$bytelen $bytelen $bytes]
										}
									}
								}]

								append build_params_vars	[string map [list \
									%v%				[list $name] \
									%null%			[list [binary format I -1]] \
									%encode_field%	$encode_field \
								] {
									if {[uplevel 1 {info exists %v%}]} {
										set value	[uplevel 1 {set %v%}]
										%encode_field%
									} else {
										append pdesc	%null%
										append pformats	\u0\u1	;# binary
									}
								}]
								append build_params_dict	[string map [list \
									%v%				[list $name] \
									%null%			[list [binary format I -1]] \
									%encode_field%	$encode_field \
								] {
									if {[dict exists $param_values %v%]} {
										set value	[dict get $param_values %v%]
										%encode_field%
									} else {
										append pdesc	%null%
										append pformats	\u0\u1	;# binary
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
						T { # RowDescription <<<
							set makerow_vars	{}
							set makerow_dict	{}
							set makerow_dict_no_nulls	{}
							set makerow_list	{}
							set colnum	0

							append makerow_vars	{binary scan [read $socket 2] S col_count} \n
							append makerow_dict	{binary scan [read $socket 2] S col_count} \n {set row {}} \n
							append makerow_dict_no_nulls	{binary scan [read $socket 2] S col_count} \n {set row {}} \n
							append makerow_list	{binary scan [read $socket 2] S col_count} \n {set row {}} \n

							set data	[read $socket $len]
							if {[eof $socket]} {my connection_lost}

							binary scan $data S fields_per_row
							set i	2
							set c_types	{}
							set rformats	[binary format Su $fields_per_row]
							for {set c 0} {$c < $fields_per_row} {incr c} {
								set idx	[string first \u0 $data $i]
								if {$idx == -1} {
									#::pgwire::log notice "No c-string found in [regexp -all -inline .. [binary encode hex $data]]"
									throw unterminated_string "No null terminator found"
								}
								set field_name	[string range $data $i [- $idx 1]]
								set i	[+ $idx 1]
								incr colnum
								set myname	field_$colnum
								lappend field_names	$field_name field_$colnum

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
								set setvar		{}
								if {[dict exists $type_oids $type_oid]} {
									set type_name	[dict get $type_oids $type_oid]
									set colfmt		\u0\u1	;# binary
									append setvar [switch -glob -- $type_name {
										bool	{format {set %s [expr {[read $socket $collen] eq "\u1"}]} [list $myname]}
										int1	{format {binary scan [read $socket $collen] c %s} [list $myname]}
										smallint -
										int2	{format {binary scan [read $socket $collen] S %s} [list $myname]}
										int4 -
										integer	{format {binary scan [read $socket $collen] I %s} [list $myname]}
										bigint -
										int8	{format {binary scan [read $socket $collen] W %s} [list $myname]}
										bytea	{format {set %s [read $socket $collen]} [list $myname]}
										float4	{format {binary scan [read $socket $collen] R %s} [list $myname]}
										float8	{format {binary scan [read $socket $collen] Q %s} [list $myname]}
										default	{
											set colfmt		\u0\u0	;# text
											set type_name	text
											format {set %s [if {$collen == 0} {return -level 0 {}} else {encoding convertfrom %s [read $socket $collen]}]} [list $myname] [list $tcl_encoding]
										}
									}]

									append rformats	$colfmt
									lappend c_types	$field_name $type_name
								} else {
									append setvar [format {set %s [if {$collen == 0} {return -level 0 {}} else {encoding convertfrom %s [read $socket $collen]}]} [list $myname] [list $tcl_encoding]]
									append rformats	\u0\u0	;# text
									lappend c_types	$field_name text
								}

								if {![regexp "^set [list $myname] \\\[(.*)\\\]$" $setvar - set_res]} {
									set set_res	"$setvar; set [list $myname]"
								}

								set makerow_prefix	"binary scan \[read \$socket 4\] I collen\n"
								append makerow_vars $makerow_prefix "if {\$collen == -1} {unset -nocomplain [list $myname]} else [list $setvar]\n"
								append makerow_dict $makerow_prefix "if {\$collen != -1} {dict set row [list $field_name] \[$set_res\]}\n"
								append makerow_dict_no_nulls $makerow_prefix "if {\$collen != -1} {dict set row [list $field_name] \[$set_res\]} else {dict set row [list $field_name] {}}\n"
								append makerow_list $makerow_prefix "if {\$collen == -1} {lappend row {}} else {lappend row \[$set_res\]}\n"
							}

							break
							#>>>
						}
						n { # NoData <<<
							set rformats		\u0\u0
							set makerow_vars	{}
							set makerow_dict	{}
							set makerow_dict_no_nulls	{}
							set makerow_list	{}
							set c_types			{}
							break
							#>>>
						}
						default {my default_message_handler $msgtype $len}
					}
				}
				#>>>
				# Build execute script <<<
				set desc	"# SQL extended-query execute script for:\n# [join [split $compiled \n] "\n# "]\n" 
				append desc "# - Parameters ------------------------\n$param_desc"
				append desc "# - Variant ---------------------------\n# [join [split $variant_setup \n] "\n# "]\n"

				set execute [string map [list \
					%desc%				$desc \
					%stmt_name%			[list $stmt_name] \
					%field_names%		[list $field_names] \
					%rformats%			[list $rformats] \
					%c_types%			[list $c_types] \
					%sql%				[list $sql] \
				] {%desc%
					my variable tcl_encoding
					my variable transaction_status
					my variable ready_for_query
					my variable busy_sql
					set field_names	%field_names%
					set c_types		%c_types%

					# Execute setup <<<
					%execute_setup%
					# Execute setup >>>

					# Bind statment $stmt_name, default portal
					# Execute $max_rows_per_batch
					# Sync
					set ready_for_query	0
					set busy_sql	%sql%
					set rformats	%rformats%
					set bindmsg		\u0[encoding convertto $tcl_encoding %stmt_name%]\u0$parameters$rformats
					puts -nonewline $socket [binary format \
						{ \
							aIa* \
							aIcI \
							a* \
						} \
							B [+ 4 [string length $bindmsg]] $bindmsg \
							E 9 0 $max_rows_per_batch \
							S\u0\u0\u0\u4 \
					]
					flush $socket
					#set rtt_start	[clock microseconds]

					try {
						# Bind responses <<<
						while 1 {
							if {[binary scan [read $socket 5] aI msgtype len] != 2} {my connection_lost}
							#if {[info exists rtt_start]} {
							#	::pgwire::log notice "server RTT: [format %.6f [expr {([clock microseconds] - $rtt_start)/1e6}]]"
							#}
							incr len -4	;# len includes itself
							if {$msgtype eq 2} break else {my default_message_handler $msgtype $len}
						}
						#>>>
						# Execute responses <<<
						while 1 {
							if {[binary scan [read $socket 5] aI msgtype len] != 2} {my connection_lost}
							incr len -4	;# len includes itself
							if {$msgtype eq "D"} { # DataRow <<<
								%on_datarow%
								#>>>
							} else {
								switch -exact -- $msgtype {
									C { # CommandComplete <<<
										set data	[read $socket $len]
										if {[eof $socket]} {my connection_lost}
										set idx	[string first "\u0" $data]
										if {$idx == -1} {
											#::pgwire::log notice "No c-string found in [regexp -all -inline .. [binary encode hex $data]]"
											throw unterminated_string "No null terminator found"
										}
										set message	[string range $data 0 [- $idx 1]]
										# TODO: report this somehow?
										break
										#>>>
									}
									I { # EmptyQueryResponse <<<
										break
										#>>>
									}
									s { # PortalSuspended <<<
										# Execute unnamed portal
										puts -nonewline $socket [binary format aIcI E 9 0 $max_rows_per_batch]
										flush $socket
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
						#>>>
						# Sync response <<<
						while 1 {
							if {[binary scan [read $socket 5] aI msgtype len] != 2} {my connection_lost}
							incr len -4	;# len includes itself

							switch -exact -- $msgtype {
								Z { # ReadyForQuery <<<
									if {![binary scan [read $socket $len] a transaction_status] == 1} {my connection_lost}
									set ready_for_query	1
									break
									#>>>
								}
								default {my default_message_handler $msgtype $len}
							}
						}
						#>>>
					} on error {errmsg options} {
						set rethrow	[list -options $options $errmsg]
						#::pgwire::log error "Error during execute: $options"
						my skip_to_sync
					}

					if {[info exists rethrow]} {
						return {*}$rethrow
					}
					set acc
				}]
				# Build execute script >>>
				set cached		{}
				#::pgwire::log notice "execute script:\n$execute\nmakerow_vars: $makerow_vars\nmakerow_dict: $makerow_dict\nmakerow_list: $makerow_list"
				# Results:
				#	- $stmt_name: the name of the prepared statement (as known to the server)
				#	- $field_names:	the result column names and the local variables they are unpacked into
				#	- $build_params: script to run to gather the input params
				#	- $execute: script to run to execute the statement
				#	- $makerow_vars
				#	- $makerow_dict
				#	- $makerow_dict_no_nulls
				#	- $makerow_list
				#	- $cached
				#	- $heat - how frequently this prepared statement has been used recently, relative to others
				dict set prepared $sql [dict create \
					stmt_name			$stmt_name \
					field_names			$field_names \
					build_params		$build_params \
					execute				$execute \
					makerow_vars		$makerow_vars \
					makerow_dict		$makerow_dict \
					makerow_dict_no_nulls		$makerow_dict_no_nulls \
					makerow_list		$makerow_list \
					cached				$cached \
					heat				0 \
				]
				#::pgwire::log notice "Finished preparing statement, execute:\n$execute"
			} on error {errmsg options} { #<<<
				#::pgwire::log error "Error preparing statement,\n$sql\n-- compiled ----------------\n$compiled\ syncing: [dict get $options -errorinfo]"
				if {[info exists socket]} {
					puts -nonewline $socket S\u0\u0\u0\u4
					flush $socket
					my skip_to_sync
				}
				return -options $options $errmsg
			}
			#>>>
			#>>>
		} else { # Retrieve prepared statement <<<
			set stmt_info	[dict get $prepared $sql]
			dict with stmt_info {}
			if {$heat < 128} {
				incr heat
				dict set prepared $sql heat $heat
			}
			# Sets:
			#	- $stmt_name: the name of the prepared statement (as known to the server)
			#	- $field_names:	the result column names and the local variables they are unpacked into
			#	- $build_params: script to run to gather the input params
			#	- $execute: script to run to execute the statement
			#	- $makerow_vars
			#	- $makerow_dict
			#	- $makerow_list
			#	- $cached
			#	- $heat - how frequently this prepared statement has been used recently, relative to others
		}
		#>>>

		try {
			try {
				dict get $cached $variant_setup
			} on ok e {
			} trap {TCL LOOKUP DICT} {} {
				try $variant_setup
				set e	[list {socket max_rows_per_batch parameters} [string map [list \
					%on_datarow%	"$makerow\nif {\[eof \$socket\]} {my connection_lost}\n$on_row" \
					%execute_setup%	$execute_setup \
				] $execute] [self namespace]]
				dict set prepared $sql cached $variant_setup $e
				#writefile /tmp/e [tcl::unsupported::disassemble lambda $e]
				#writefile /tmp/e.tcl $e
			}
			try $build_params on ok parameters {}
			uplevel 1 [list apply $e $socket $max_rows_per_batch $parameters]
		} trap {PGWIRE ErrorResponse} {errmsg options} {
			return -code error -errorcode [dict get $options -errorcode] $errmsg
		} on error {errmsg options} {
			#::pgwire::log error "Error in execution phase of extended query: [dict get $options -errorinfo]"
			return -options $options $errmsg
		}
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

		if {[info exists param_values]} {
			set extra	[list $param_values]
		} else {
			set extra	{}
		}

		switch -exact -- [dict get $opts -as] {
			lists {
				if {[dict exists $opts -columnsvariable]} {
					tailcall my extended_query $sqlcode [string map [list \
						%columnsvariable%	[list [dict get $opts -columnsvariable]] \
					] {
						set execute_setup	{
							upvar 1 %columnsvariable% column_names
							set column_names	[dict keys $field_names]
							set acc	{}
						}
						if {$::pgwire::accelerators} {
							set makerow			{
								set data	[read $socket $len]
								if {[chan eof $socket]} {my connection_lost}
								set row	[::pgwire::c_makerow $data $c_types $rformats $tcl_encoding lists]
							}
						} else {
							set makerow			$makerow_list
						}
						set on_row			{lappend acc $row}
					}] 0 {*}$extra
				} else {
					tailcall my extended_query $sqlcode {
						#set execute_setup {
						#	set parts			{}
						#	set column_names	{}
						#	foreach {f d} $field_names {
						#	   lappend column_names $f
						#		append parts "\[if {\[info exists [list $d]\]} {set [list $d]}] "
						#	}
						#	set __onrow "lappend acc \[dict create $parts\]"
						#}
						set execute_setup	{set acc {}}
						if {$::pgwire::accelerators} {
							set makerow			{
								set data	[read $socket $len]
								if {[chan eof $socket]} {my connection_lost}
								set row	[::pgwire::c_makerow $data $c_types $rformats $tcl_encoding lists]
							}
						} else {
							set makerow			$makerow_list
						}
						set on_row			{lappend acc $row}
					} 0 {*}$extra
				}
			}

			dicts {
				tailcall my extended_query $sqlcode {
					#set execute_setup {
					#	set parts	{}
					#	foreach {f d} $field_names {
					#		append parts "{*}\[if {\[info exists [list $d]\]} {list [list $f] \[set [list $d]\]}\] "
					#	}
					#	set __onrow "lappend acc \[dict create $parts\]"
					#	set __onrow "lappend acc \$row"
					#	::pgwire::log notice "__onrow:\n$__onrow"
					#}
					set execute_setup	{set acc {}}
					if {$::pgwire::accelerators} {
						set makerow			{
							set data	[read $socket $len]
							if {[chan eof $socket]} {my connection_lost}
							set row	[::pgwire::c_makerow $data $c_types $rformats $tcl_encoding dicts]
						}
					} else {
						set makerow			$makerow_dict
					}
					set on_row			{lappend acc $row}
				} 0 {*}$extra
			}

			default {
				error "-as must be dicts or lists"
			}
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

		if {[info exists param_values]} {
			set extra	[list $param_values]
		} else {
			set extra	{}
		}

		switch -exact -- [dict get $opts -as] {
			lists {
				if {[dict exists $opts -columnsvariable]} {
					tailcall my extended_query $sqlcode [string map [list \
						%rowvarname%		[list $row_varname] \
						%columnsvariable%	[list [dict get $opts -columnsvariable]] \
						%script%			[list $script] \
					] {
						set execute_setup	{
							upvar 1 %columnsvariable% column_names  %rowvarname% row
							set column_names	[dict keys $field_names]
							set acc	{}
							set broken	0
						}
						if {$::pgwire::accelerators} {
							set makerow			{
								set data	[read $socket $len]
								if {[chan eof $socket]} {my connection_lost}
								if {!$broken} {
									set row	[::pgwire::c_makerow $data $c_types $rformats $tcl_encoding lists]
								}
							}
						} else {
							set makerow			"if {\$broken} {read \$socket \$len} else {\n$makerow_list\n}"
						}
						set on_row			{
							if {!$broken} {
								try {
									uplevel 1 %script%
								} on break {} {
									set broken	1
								} on continue {} {
								} on return {r o} {
									#::pgwire::log notice "foreach script caught return r: ($r), o: ($o)"
									set broken	1
									dict incr o -level 1
									dict set o -code return
									set rethrow	[list -options $o $r]
								} on error {r o} {
									#::pgwire::log notice "foreach script caught error r: ($r), o: ($o)"
									set broken	1
									dict incr o -level 1
									set rethrow	[list -options $o $r]
								}
							}
						}
					}] 0 {*}$extra
				} else {
					tailcall my extended_query $sqlcode [string map [list \
						%rowvarname%		[list $row_varname] \
						%script%			[list $script] \
					] {
						set execute_setup	{
							upvar 1 %rowvarname% row
							set column_names	[dict keys $field_names]
							set acc	{}
							set broken	0
						}
						if {$::pgwire::accelerators} {
							set makerow			{
								set data	[read $socket $len]
								if {[chan eof $socket]} {my connection_lost}
								if {!$broken} {
									set row	[::pgwire::c_makerow $data $c_types $rformats $tcl_encoding lists]
								}
							}
						} else {
							set makerow			"if {\$broken} {read \$socket \$len} else {\n$makerow_list\n}"
						}
						set on_row			{
							if {!$broken} {
								try {
									uplevel 1 %script%
								} on break {} {
									set broken	1
								} on continue {} {
								} on return {r o} {
									#::pgwire::log notice "foreach script caught return r: ($r), o: ($o)"
									set broken	1
									dict incr o -level 1
									dict set o -code return
									set rethrow	[list -options $o $r]
								} on error {r o} {
									#::pgwire::log notice "foreach script caught error r: ($r), o: ($o)"
									set broken	1
									dict incr o -level 1
									set rethrow	[list -options $o $r]
								}
							}
						}
					}] 0 {*}$extra
				}
			}

			dicts {
				tailcall my extended_query $sqlcode [string map [list \
					%rowvarname%		[list $row_varname] \
					%script%			[list $script] \
				] {
					set execute_setup	{
						upvar 1 %rowvarname% row
						set column_names	[dict keys $field_names]
						set acc	{}
						set broken	0
					}
					if {$::pgwire::accelerators} {
						set makerow			{
							set data	[read $socket $len]
							if {[chan eof $socket]} {my connection_lost}
							if {!$broken} {
								set row	[::pgwire::c_makerow $data $c_types $rformats $tcl_encoding dicts]
							}
						}
					} else {
						set makerow			"if {\$broken} {read \$socket \$len} else {\n$makerow_dict\n}"
					}
					set on_row			{
						if {!$broken} {
							try {
								uplevel 1 %script%
							} on break {} {
								set broken	1
							} on continue {} {
							} on return {r o} {
								#::pgwire::log notice "foreach script caught return r: ($r), o: ($o)"
								set broken	1
								dict incr o -level 1
								dict set o -code return
								set rethrow	[list -options $o $r]
							} on error {r o} {
								#::pgwire::log notice "foreach script caught error r: ($r), o: ($o)"
								set broken	1
								dict incr o -level 1
								set rethrow	[list -options $o $r]
							}
						}
					}
				}] 0 {*}$extra
			}

			default {
				error "-as must be dicts or lists"
			}
		}
	}

	#>>>
	method onecolumn sql { #<<<
		tailcall my extended_query $sql {
			set execute_setup	{set broken 0; set acc {}}
			if {0 && $::pgwire::accelerators} {
				set makerow			{
					set data	[read $socket $len]
					if {[chan eof $socket]} {my connection_lost}
					if {!$broken} {
						set row	[::pgwire::c_makerow $data $c_types $rformats $tcl_encoding lists]
					}
				}
			} else {
				set makerow			"if {\$broken} {read \$socket \$len} else {\n$makerow_list\n}"
			}
			set on_row			{
				if {!$broken} {
					set acc		[lindex $row 0]
					set broken	1
				}
			}
		}
	}

	#>>>

	method transaction_status {} { set transaction_status }
	method ready_for_query {} { set ready_for_query }
}

# vim: foldmethod=marker foldmarker=<<<,>>> ts=4 shiftwidth=4
