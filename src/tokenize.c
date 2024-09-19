#line 1 "tokenize.c"
Tcl_Obj*	g_lit_id = NULL;

INIT {
	replace_tclobj(&g_lit_id, Tcl_NewStringObj("id", 2));
	return TCL_OK;
}

RELEASE {
	replace_tclobj(&g_lit_id, NULL);
}

OBJCMD(tokenize) {
	int			code = TCL_OK;
	Tcl_Obj*	res = NULL;
	Tcl_Obj*	bindvar = NULL;
	Tcl_Obj*	val = NULL;
	Tcl_Obj*	bindvars = NULL;
	Tcl_Obj*	bindslots = NULL;
	Tcl_Obj*	tmp = NULL;
	Tcl_Obj*	tmp2 = NULL;
	int			slotseq = 0;
	int			standard_conforming_strings = 0;
	static const char*	modes[] = {
		"interpolate",
		"bindparse",
		NULL
	};
	int	bindparse;

	enum {A_cmd, A_MODE, A_SQL, A_STDSTR, A_args, A_objc};
	const int	A_DICT = A_args;
	CHECK_RANGE_ARGS_LABEL(finally, code, "mode sql standard_conforming_strings ?dict?");
	TEST_OK_LABEL(finally, code, Tcl_GetBooleanFromObj(interp, objv[A_STDSTR], &standard_conforming_strings));
	TEST_OK_LABEL(finally, code, Tcl_GetIndexFromObj(interp, objv[A_MODE], modes, "mode", TCL_EXACT, &bindparse));

	if (bindparse) {
		replace_tclobj(&bindvars, Tcl_NewListObj(0, NULL));
		replace_tclobj(&bindslots, Tcl_NewDictObj());
	}
	replace_tclobj(&res, Tcl_NewObj());
	const char*	sql = Tcl_GetString(objv[A_SQL]);
	const char* cur = sql;
	const char*	tok = cur;
	const char* mar;
	for (;;) {
		const char	*b1, *b2;
		/*!stags:re2c:sql format = "const char* @@;"; */
		/*!local:re2c:sql
			re2c:define:YYCTYPE		= char;
			re2c:define:YYCURSOR	= cur;
			re2c:define:YYMARKER	= mar;
			re2c:yyfill:enable		= 0;
			re2c:tags				= 1;

			end		= [\x00];
			any		= [^] \ end;
			esc		= [\\];
			dquote	= ["];
			squote	= ['];
			dqpair	= esc any;
			sqpair	= esc any | squote squote;
			schar	= any \ squote | sqpair;
			dchar	= any \ dquote | dqpair;
			sqlit	= squote schar* squote;
			dqlit	= dquote dchar* dquote;
			comment	= "--" [^\n\x00]*;
			pgcast	= "::";
			bindvar	= [_a-zA-Z0-9]+;
			ign		= comment
					| sqlit
					| dqlit
					| pgcast;

			end		{ Tcl_AppendToObj(res, tok, (int)(cur-tok-1)); break; }
			ign		{ continue; }
			*		{ continue; }

			":" @b1 bindvar @b2 {
				Tcl_AppendToObj(res, tok, (int)(b1-1-tok));
				tok = b2;
				replace_tclobj(&bindvar, Tcl_NewStringObj(b1, (int)(b2-b1)));
				goto interpolate_bindvar;
			}
		*/

	interpolate_bindvar:
		if (bindparse) {
			Tcl_Obj*	loan = NULL;
			TEST_OK_LABEL(finally, code, Tcl_DictObjGet(interp, bindslots, bindvar, &loan));
			if (loan) {
				TEST_OK_LABEL(finally, code, Tcl_DictObjGet(interp, loan, g_lit_id, &loan));
				replace_tclobj(&tmp, loan);
			} else {
				replace_tclobj(&tmp, Tcl_NewIntObj(++slotseq));
				replace_tclobj(&tmp2, Tcl_NewDictObj());
				TEST_OK_LABEL(finally, code, Tcl_DictObjPut(interp, tmp2, g_lit_id, tmp));
				TEST_OK_LABEL(finally, code, Tcl_DictObjPut(interp, bindslots, bindvar, tmp2));
				TEST_OK_LABEL(finally, code, Tcl_ListObjAppendElement(interp, bindvars, bindvar));
			}
			Tcl_AppendToObj(res, "$", 1);
			Tcl_AppendObjToObj(res, tmp);
			continue;
		}

		if (A_DICT < objc) {
			Tcl_Obj*	loan = NULL;
			TEST_OK_LABEL(finally, code, Tcl_DictObjGet(interp, objv[A_DICT], bindvar, &loan));
			replace_tclobj(&val, loan);
		} else {
			replace_tclobj(&val, Tcl_ObjGetVar2(interp, bindvar, NULL, 0));
		}

		if (!val) {
			Tcl_AppendToObj(res, "NULL", 4);
			continue;
		}

		Tcl_AppendToObj(res, "'", 1);
		const char*	valstr = Tcl_GetString(val);
		const char* valcur = valstr;
		const char*	valmar;
		for (;;) {
			const char*	valtok = valcur;
			/*!local:re2c:val
				re2c:define:YYCTYPE		= char;
				re2c:define:YYCURSOR	= valcur;
				re2c:define:YYMARKER	= valmar;
				re2c:yyfill:enable		= 0;

				end		= [\x00];
				ok		= [^'\\] \ end;

				end		{ break; }
				ok+		{ Tcl_AppendToObj(res, valtok, (int)(valcur-valtok)); continue; }
				"'"		{ Tcl_AppendToObj(res, "''", 2); continue; }
				"\\"	{ Tcl_AppendToObj(res, "\\\\", standard_conforming_strings ? 1 : 2); continue; }
				*		{ Tcl_AppendToObj(res, tok, 1); continue; }
			*/
		}
		Tcl_AppendToObj(res, "'", 1);
	}

	if (bindparse)
		replace_tclobj(&res, Tcl_NewListObj(2, (struct Tcl_Obj*[]){bindslots, res}));

	Tcl_SetObjResult(interp, res);

finally:
	replace_tclobj(&res, NULL);
	replace_tclobj(&bindvar, NULL);
	replace_tclobj(&bindvars, NULL);
	replace_tclobj(&bindslots, NULL);
	replace_tclobj(&val, NULL);
	replace_tclobj(&tmp, NULL);
	replace_tclobj(&tmp2, NULL);
	return code;
}

// vim: foldmethod=marker foldmarker=<<<,>>> ts=4 shiftwidth=4
