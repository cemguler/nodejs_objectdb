var	emitter = require("events").EventEmitter,
	async = require("async"),
	util = require("util"),
	
	hlp = require("c:/ordergrinder/objsync/obj.utils.js");

var execFile = require('child_process').execFile; 

var query_operator_map = {
	"like":"indexOf",
	"equals":"==",
	"greaterthanorequalto":">=",
	"lessthanorequalto":"<=",
	"greaterthan":">",
	"lessthan":"<",
	"notequaltobrackets":"!=",
	"notequaltoexclamation":"!=",
	"and":"&&",
	"or":"||",
	"add":"+",
	"subtract":"-",
	"divide":"/",
	"multiply":"*"
}
var query_function_map = {
	"round":"Math.round({0})",
	"between":"({0}>={1} && {0}<={2})",
	"notbetween":"({0}<{1} || {0}>{2})",
	"charindex":"{1}.indexOf({0})",
	"left":"String({0}).substring(0,{1})",
	"right":"String({0}).substring(String({0}).length,String({0}).length-{1})",
	"mid":"String({0}).substring({1},{2})",
	//"inlist":"isinlist({0},{1})",
	"inlist":"{1}.indexOf({0})>-1",
	"max":"getmax({0})",
	"min":"getmin({0})",
	"sum":"getsum({0})",
	"avg":"getavg({0})",
	"count":"getcnt({0})"
	//"max":"[\"max\",{0}]",
	//"min":"[\"min\",{0}]"
}
var query_groupfunction_map = {
	"max":"getmax({0})",
	"min":"getmin({0})",
	"sum":"getsum({0})",
	"avg":"getavg({0})",
	"count":"getcnt({0})"
}

util.inherits(ogquery, emitter);
function ogquery(srv, vslave) {
    if(false === (this instanceof ogquery)) {
        return new ogquery(srv);
    }
	ogquery.super_.call(this);
	this.slave = vslave || false;
	var begintime = process.hrtime();
	this.result = {
		stats : { },
		data : null,
		alias : "",
		queryobject : null
	}
	var that = this;
	
	var markstat = function(evt) {
		that.result.stats[evt] = process.hrtime(begintime);
	}
	
	var validateoperators = function(expr) {
		if (typeof expr == "object" && expr) {
			if (expr.hasOwnProperty("Operator")) {	//expression
				if (query_operator_map.hasOwnProperty(expr.Operator.toLowerCase())) {
					return validateoperators(expr.leftside) + validateoperators(expr.rightside);
				} else {
					return expr["Operator"];
				}
			} else {
				if (expr.hasOwnProperty("function")) {	//function
					if (query_function_map.hasOwnProperty(expr["function"].toLowerCase())) {
						var vfuncres = "";
						for (var x=0; x<expr.parameters.length; x++) {
							vfuncstr = vfuncres + validateoperators(expr.parameters[x]);
						}
						return vfuncres;
					} else {
						return expr["function"];
					}
				}
			}
		} 
		return "";	//literal
	}	
	
	var validate = function() {
		var qry = that.result.queryobject;
		if (!qry.hasOwnProperty("tablenames")) return "no select tables found";
		for (var x=0; x<qry.tablenames.length; x++) {
			var vtbls = qry.tablenames[x].split(".");
			if (vtbls.length!=2) return "invalid table specification : " + qry.tablenames[x] + ", use <db>.<table>";
			if (!srv.hasOwnProperty(vtbls[0])) return "database does not exist : " + qry.tablenames[x];
			if (!srv[vtbls[0]].hasOwnProperty(vtbls[1])) return "table does not exist : " + qry.tablenames[x];
		}
		if (!qry.hasOwnProperty("columns")) return "no select columns specified";
		for (var x=0; x<qry.columns.length; x++) {
			var vcol = qry.columns[x];
			if (vcol.hasOwnProperty("parameters")) {	//this is a function
				
			} else {									//normal column
				if (vcol.table=="" && qry.tablealiases.length>1)  return "join statements require table aliases to be specified in select columns : " + vcol.field;
				var vtblind = qry.tablealiases.indexOf(vcol.table);
				if (vtblind==-1 && vcol.table!="") return "unknown table alias : " + vcol.table + "\r\n" + JSON.stringify(vcol,null,5);
			}
		}
		if (qry.hasOwnProperty("groupby")) {
			//groupby expressions must exist in select columns
			for (var x=0; x<qry.groupby.length; x++) {
				var buldu = false;
				for (var y=0; y<qry.columns.length; y++) {
					if ((qry.columns[y].alias==qry.groupby[x].alias) || (qry.columns[y].table + "." + qry.columns[y].field == qry.groupby[x].table + "." + qry.groupby[x].field)) {
						buldu=true;
						break;
					}
				}
				if (!buldu) return "group by <" + qry.groupby[x].alias + "> missing in select columns";
			}
			//and select columns should not have any additional non-groupby function columns			
			for (var x=0; x<qry.columns.length; x++) {
				if (!isgroupbyexpr(qry.columns[x])) {
				var buldu = false;
				for (var y=0; y<qry.groupby.length; y++) {
					if ((qry.columns[x].alias==qry.groupby[y].alias) || (qry.columns[x].table + "." + qry.columns[x].field == qry.groupby[y].table + "." + qry.groupby[y].field)) {
						buldu=true;
						break;
					}
				}
				if (!buldu) return "select column <" + qry.columns[x].alias + "> missing in group by expression";
				}
			}
		}
		/*
		for (var x=0; x<qry.globalcolumns.length; x++) {
			var vcol = qry.globalcolumns[x];
			if (qry.columns.indexOf(vcol)==-1) {
				if (vcol.table=="" && qry.tablealiases.length>1)  return "join statements require table aliases to be specified in select columns : " + vcol.field;
				var vtblind = qry.tablealiases.indexOf(vcol.table);
				if (vtblind==-1 && vcol.table!="") return "unknown table alias : " + vcol.table + "\r\n" + JSON.stringify(vcol,null,5);		
			}
		}
		*/
		if (!qry.hasOwnProperty("from")) return "missing from clause";		
		var opvalid = validateoperators(that.result.queryobject.where);
		if (opvalid!="") return "unsupported operator/function(s) : " + opvalid;
		return "";
	}

	var clean_queryobject=function() {
		if (that.result.queryobject.hasOwnProperty("where")) {
			if (that.result.queryobject.where) {
				if (that.result.queryobject.where.expr=="") delete that.result.queryobject["where"];
			} else {
				delete that.result.queryobject["where"];
			}
		}
		if (that.result.queryobject.hasOwnProperty("groupby")) {
			if (that.result.queryobject.groupby) {
				if (that.result.queryobject.groupby.length==0) delete that.result.queryobject["groupby"];
			} else {
				delete that.result.queryobject["groupby"];
			}
		}
		if (that.result.queryobject.hasOwnProperty("having")) {
			if (that.result.queryobject.having) {
				if (that.result.queryobject.having.expr=="") delete that.result.queryobject["having"];
			} else {
				delete that.result.queryobject["having"];
			}
		}
		if (that.result.queryobject.hasOwnProperty("orderby")) {
			if (that.result.queryobject.orderby) {
				if (that.result.queryobject.orderby.length==0) delete that.result.queryobject["orderby"];
			} else {
				delete that.result.queryobject["orderby"];
			}
		}
		if (that.result.queryobject.hasOwnProperty("originalstring")) {
			delete that.result.queryobject["originalstring"];
		}
	}
	
	var getexprvalue = function(expr, withalias) {
		if (expr) {
			withalias = withalias || "obj";
			if (typeof expr == "object") {
				if (expr.hasOwnProperty("leftside")) {	//expression
					if (expr.Operator.toLowerCase()=="like") {
						return "(" + getexprvalue(expr.leftside, withalias) + ".indexOf(" + getexprvalue(expr.rightside, withalias).replace(/%/g,"") + ")>-1)";
					} else {
						return "(" + getexprvalue(expr.leftside, withalias) + " " + query_operator_map[expr.Operator.toLowerCase()] + " " + getexprvalue(expr.rightside, withalias) + ")";
					}
				} else {
					if (expr.hasOwnProperty("function")) {	//function
						//console.log(expr);
						var funcstr = query_function_map[expr["function"].toLowerCase()] + "";
						var argsarr = [];
						for (var x=0; x<expr.parameters.length; x++) {
							if (util.isArray(expr.parameters[x])) {
								argsarr.push(JSON.stringify(expr.parameters[x]));
							} else {
								argsarr.push(getexprvalue(expr.parameters[x], withalias));
							}
						}
						//console.log(funcstr);
						//console.log(argsarr);
						funcstr = funcstr.format.apply(funcstr, argsarr);
						//console.log(funcstr);
						return funcstr;
					} else {
						if (expr.hasOwnProperty("field")) {	//column
							if (withalias=="hash") 
								return 'itm["' + expr.table + "." + expr.field + '"]';
							if (withalias=="obj") 
								return "itm." + expr.table + "." + expr.field;
								//return "itm." + expr.field;
							if (withalias=="obj2") 
								return "itm." + expr.field;
							if (withalias=="duo") 
								return expr.table + "." + expr.field;
							if (withalias=="duo2") 
								return expr.table + "[\"" + expr.field + "\"]";
							/*if (withalias=="duo3") {
								var tblind = that.result.queryobject.tablealiases.indexOf(expr.table);
								var tblnm = that.result.queryobject.tablenames[tblind].split(".")[1];
								return  "itm[\"" + tblnm + "." + expr.field + "\"]";
							}*/
						} else {
							if (util.isArray(expr)) {
								return expr;
							} else {
								return "?";
							}
						}
					}
				}
			} else {
				return expr;	//literal
			}
		} else {
			return null;
		}
	}		
	
	var isgroupbyexpr = function(expr) {
		if (expr) {
			if (typeof expr == "object") {
				if (expr.hasOwnProperty("leftside")) {	//expression
					return isgroupbyexpr(expr.leftside) || isgroupbyexpr(expr.rightside);
				} else {
					if (expr.hasOwnProperty("function")) {	//function						
						if (query_groupfunction_map.hasOwnProperty(expr["function"].toLowerCase())) {
							return true;
						} else {
							var argsarr = false;
							for (var x=0; x<expr.parameters.length; x++) {
								argsarr = argsarr || isgroupbyexpr(expr.parameters[x]);
							}
							return argsarr;
						}
					} else {
						return false;
					}
				}
			} else {
				return false;	//literal
			}
		} else {
			return false;
		}
	}
	
	var resolvejoin = function(tjoin) {
		var t1 = { data:[], alias:"t1" };
		/*
		console.log("*****************************************************************************");
		console.log(tjoin.table);
		console.log("");
		console.log(typeof tjoin.table);
		console.log("*****************************************************************************");
		*/
		if (typeof tjoin.table == "object") {
			if (typeof tjoin.table.table == "object") {
				t1 = resolvejoin(tjoin.table);
			} else {
				var t1cnt = (srv[tjoin.table.db])[tjoin.table.table].count();
				t1.alias = tjoin.table.alias;
				t1.data = (srv[tjoin.table.db])[tjoin.table.table].range(0,t1cnt).result;
				/*
				var vtjoindata = (srv[tjoin.table.db])[tjoin.table.table].data;
				for (var x=0; x<vtjoindata.length; x++) {
					var vobj = {};
					for (var y=0; y<Object.keys(vtjoindata[x]).length; y++) {
						vobj[tjoin.table.alias + "." + Object.keys(vtjoindata[x])[y]] = vtjoindata[x][Object.keys(vtjoindata[x])[y]];
					}
					t1.data.push(vobj);
				}
				console.log(t1.data);
				*/
			}
		} else {
				var t1cnt = (srv[tjoin.table.db])[tjoin.table].count();
			t1.alias = tjoin.alias;
			t1.data = (srv[tjoin.db])[tjoin.table].range(0,t1cnt);
			/*
			var vtjoindata = (srv[tjoin.db])[tjoin.table].data;
			for (var x=0; x<vtjoindata.length; x++) {
				var vobj = {};
				for (var y=0; y<Object.keys(vtjoindata[x]).length; y++) {
					vobj[tjoin.alias + "." + Object.keys(vtjoindata[x])[y]] = vtjoindata[x][Object.keys(vtjoindata[x])[y]];
				}
				t1.data.push(vobj);
			}
			console.log(t1.data);
			*/
		}
		var t2 = { data:[], alias:"t2" };
		if (typeof tjoin.joinwith == "object") {
			if (typeof tjoin.joinwith.table == "object") {
				t2 = resolvejoin(tjoin.joinwith);
			} else {
				var t2cnt = (srv[tjoin.joinwith.db])[tjoin.joinwith.table].count();
				t2.data = (srv[tjoin.joinwith.db])[tjoin.joinwith.table].range(0,t2cnt);
				t2.alias = tjoin.joinwith.alias;
				/*
				var vtjoindata = (srv[tjoin.joinwith.db])[tjoin.joinwith.table].data;
				for (var x=0; x<vtjoindata.length; x++) {
					var vobj = {};
					for (var y=0; y<Object.keys(vtjoindata[x]).length; y++) {
						vobj[tjoin.joinwith.alias + "." + Object.keys(vtjoindata[x])[y]] = vtjoindata[x][Object.keys(vtjoindata[x])[y]];
					}
					t2.data.push(vobj);
				}
				console.log(t2.data);
				*/
			}
		} else {
				var t2cnt = (srv[tjoin.joinwith.db])[tjoin.joinwith.table].count();
			t2.data = (srv[tjoin.joinwith.db])[tjoin.joinwith.table].range(0,t2cnt);
			t2.alias = tjoin.joinwith.alias;
			/*
			var vtjoindata = (srv[tjoin.joinwith.db])[tjoin.joinwith.table].data;
			for (var x=0; x<vtjoindata.length; x++) {
				var vobj = {};
				for (var y=0; y<Object.keys(vtjoindata[x]).length; y++) {
					vobj[tjoin.joinwith.alias + "." + Object.keys(vtjoindata[x])[y]] = vtjoindata[x][Object.keys(vtjoindata[x])[y]];
				}
				t2.data.push(vobj);
			}
			console.log(t2.data);
			*/
		}
		
		var expr = "";
		if (tjoin.joinexpr) expr = "var joinfunc = function(" + t1.alias + "," + t2.alias + ") { return " + getexprvalue(tjoin.joinexpr, "duo") + " };";

		if (t2 && expr) {
		//console.log(expr);
			eval(expr);
			var resarr = []
			for (var x=0; x<t1.data.length; x++) {
				for (var y=0; y<t2.data.length; y++) {
					if (joinfunc(t1.data[x], t2.data[y])) {
						//var vobj = t1.data[x].clone();
						var vobj = {};	//cloner(t1.data[x])[0];	//.clone();
						for (var z=0; z<Object.keys(t1.data[x]).length; z++) {
							vobj[t1.alias + "." + Object.keys(t1.data[x])[z]] = t1.data[x][Object.keys(t1.data[x])[z]];
						}
						/*
						if (t1.alias) {
							for (var z=0; z<Object.keys(vobj).length; z++) {
								Object.keys(vobj)[z] = t1.alias + "." + Object.keys(vobj)[z];
							}
						}
						*/
						var tmpobj2 = t2.data[y];
						for (var z=0; z<Object.keys(tmpobj2).length; z++) {
							var vkey = t2.alias + "." + Object.keys(tmpobj2)[z];
							var vval = tmpobj2[Object.keys(tmpobj2)[z]];
							if (typeof vval!="object" && typeof vval!="function") {
								vobj[vkey]=vval;
							}
						}
						resarr.push(vobj);
					}
				}
			}
			var resobj = {data:resarr, alias:t1.alias}
			return resobj;

		} else {
			if (t1) {
				return { data:t1.data, alias:t1.alias };
			} else {
				return null;
			}
		}
	}
	
	var add_function_columns = function(functioncallback, withalias) {
		if (that.result.data) {
			withalias = withalias || "hash";
			var colselector = function(itm, cb) {
				//calculate non-groupby expression columns
				for (var y=0; y<that.result.queryobject.columns.length; y++) {
					var col = that.result.queryobject.columns[y];
					if (col.hasOwnProperty("parameters")) {
						if (!isgroupbyexpr(col)) {						
							var exprexpr = getexprvalue(col,withalias);
							var exprval = eval(exprexpr);
							itm[col.alias] = exprval;
						}
					}
				}
				cb(null, itm);
			}
			async.map(that.result.data, colselector, function(err, results) {
				that.result.data = results;
				functioncallback(err);
			});
		} else {
			functioncallback(null);
		}
	}
	
	var apply_where = function(wherecallback, withalias) {
		//filter using where clause
		//console.log(that.result.queryobject.where);
		withalias = withalias || "hash";
		var qwhere = "";	
		if (that.result.queryobject.hasOwnProperty("where")) qwhere = 'var funcwhere = function(itm) { return ' + getexprvalue(that.result.queryobject.where, withalias) + '; }';
		if (qwhere!="") {
			try {
				//console.log(that.result.data);
				//console.log(qwhere);
				eval(qwhere);
				var viterator = function(itm, cb) {
					var res = funcwhere(itm); 
					cb(res);
				}
				async.filter(that.result.data, viterator, function(results) {
					that.result.data = results;
					wherecallback(null);
				});
			} catch(errx) {
				wherecallback(errx);
			}
		} else {
			wherecallback(null);
		}
	}

	/*
	var isinlist = function(vval, lst) {
		if (lst.indexOf(vval)>-1) {
			return true;
		} else {
			return false;
		}
	}
	*/
	
	var apply_groupby = function(groupbycallback) {
		//console.log(JSON.stringify(that.result.queryobject,null,5));
		var isgroupby = false || that.result.queryobject.hasOwnProperty("groupby");
		var singleresult = false;
		for (var y=0; y<that.result.queryobject.columns.length; y++) isgroupby = isgroupby || isgroupbyexpr(that.result.queryobject.columns[y]);
		if (isgroupby && !that.result.queryobject.hasOwnProperty("groupby")) singleresult = true;				
		if (!isgroupby) {
			groupbycallback(null);
			return;
		}

		var grouparr = [];
		var grouparr2 = [];
		var groupfnarr = [];
		var groupcntarr = [];
		var vcurgroupfnind = 0;
		var vcurgroupfnalias = null;
		
		function getmax(colval) {
			var gitm = groupfnarr[vcurgroupfnind];
			if (gitm.hasOwnProperty(vcurgroupfnalias)) {
				if (colval>gitm[vcurgroupfnalias]*1) {
					gitm[vcurgroupfnalias]=colval;
				}
			} else {
				gitm[vcurgroupfnalias] = colval;
			}
		}
		function getmin(colval) {
			var gitm = groupfnarr[vcurgroupfnind];
			if (gitm.hasOwnProperty(vcurgroupfnalias)) {
				if (colval<gitm[vcurgroupfnalias]*1) {
					gitm[vcurgroupfnalias]=colval;
				}
			} else {
				gitm[vcurgroupfnalias] = colval;
			}
		}
		function getsum(colval) {
			var gitm = groupfnarr[vcurgroupfnind];
			if (!gitm.hasOwnProperty(vcurgroupfnalias)) {
				gitm[vcurgroupfnalias] = 0;
			}
			if (!colval) colval=0;
			gitm[vcurgroupfnalias]=gitm[vcurgroupfnalias]*1+colval*1;
			//console.log(vcurgroupfnalias + " = " + colval + " .... " + gitm[vcurgroupfnalias]);
		}
		function getavg(colval) {
			if (!colval) colval=0;
			var gcnt = groupcntarr[vcurgroupfnind];
			var gitm = groupfnarr[vcurgroupfnind];
			if (!gitm.hasOwnProperty(vcurgroupfnalias)) {
				gitm[vcurgroupfnalias] = 0;
			}
			var newval = ((gitm[vcurgroupfnalias]*(gcnt-1)) + colval)/gcnt;
			gitm[vcurgroupfnalias]=newval;	//gitm[vcurgroupfnalias]*1+colval*1;
		}		
		function getcnt(colval) {
			var gitm = groupfnarr[vcurgroupfnind];
			if (!gitm.hasOwnProperty(vcurgroupfnalias)) {
				gitm[vcurgroupfnalias] = 0;
			}
			gitm[vcurgroupfnalias]=gitm[vcurgroupfnalias]*1+1;
		}
		
		var colselector = function(itm, cb) {
			var tmpitm = {};
			//console.log("**********************************************************************");
			//console.log(itm);
			if (isgroupby && !singleresult) {
				//console.log(that.result.queryobject.groupby);
				for (var y=0; y<that.result.queryobject.groupby.length; y++) {
					var col = that.result.queryobject.groupby[y];
					//console.log(col);
					if (itm[col.alias]) {
						//console.log("has alias .... " + col.alias);
						tmpitm[col.alias] = itm[col.alias];
						//break;
					} else {
						if (itm[col.table + "." + col.field]) {
							//console.log("has table.field .... " + col.table + "." + col.field);
							tmpitm[col.table + "." + col.field] = itm[col.table + "." + col.field];
							//break;
						}
					}						
				}
			}
			var itmstr = JSON.stringify(tmpitm);
			if (grouparr.indexOf(itmstr)==-1) {
				grouparr.push(itmstr);
				grouparr2.push(tmpitm);
				groupcntarr.push(0);
				groupfnarr.push({});
			}
			
			var vind = grouparr.indexOf(itmstr);
			groupcntarr[vind] += 1;
			vcurgroupfnind = vind;
			for (var y=0; y<that.result.queryobject.columns.length; y++) {
				var col = that.result.queryobject.columns[y];
				if (isgroupbyexpr(col)) {
					vcurgroupfnalias = col.alias;
					var exprexpr = getexprvalue(col,"hash");
					//console.log(exprexpr);
					var exprval = eval(exprexpr);
				} 
			}
			cb(null, null);
		}
		async.map(that.result.data, colselector, function(err, results) {
			if (isgroupby) {
				//console.log(grouparr2);
				//console.log(groupfnarr);
				for (var x=0; x<grouparr2.length; x++) hlp.ogextend(grouparr2[x], groupfnarr[x]);					
				that.result.data = grouparr2;
			}
			groupbycallback(err);
		});
	}
	
	var apply_having = function(havingcallback) {
		havingcallback(null);
	}
	
	var filter_select_columns = function(selectcolumnscallback) {	
		//check row columns against requested columns, delete unwanted
		//this is valid only if no groupby is specified, because groupby does it itself
		if (!that.result.queryobject.hasOwnProperty("groupby")) {
			var colselector = function(itm, cb) {
				var delkeys = [];
				var oarr = Object.keys(itm);
				for (var x=0; x<oarr.length; x++) {
					var vkey = oarr[x];
					var vtbl = vkey.split(".")[0];
					var vfld = vkey.split(".")[1];
					var vval = itm[vkey];
					var buldu = false;
					for (var y=0; y<that.result.queryobject.columns.length; y++) {
						var col = that.result.queryobject.columns[y];
						if ((vkey==col.table + "." + col.field) || (vtbl==col.table && col.field=="*") || (vkey==col.alias)) {
							buldu = true;								
							break;
						}
						if  (!vfld && vtbl && col.field=="*") {	//probably single table with no table alias in front of field
							buldu = true;								
							break;
						}
					}
					/*
					if (isgroupby && !singleresult) {
						for (var y=0; y<that.result.queryobject.groupby.length; y++) {
							var col = that.result.queryobject.groupby[y];
							if ((vkey==col.table + "." + col.field) || (vtbl==col.table && col.field=="*")) {
								buldu = true;								
								break;
							}
						}
					}
					*/
					if (!buldu) delkeys.push(vkey);
				}
				for (var x=0; x<delkeys.length; x++) delete itm[delkeys[x]];
				cb(null, itm);
			}
			async.map(that.result.data, colselector, function(err, results) {
				that.result.data = results;
				selectcolumnscallback(err);
			})
		} else {
			selectcolumnscallback(null);
		}
	}
	
	var sortresult = function() {
		if (that.result.queryobject.hasOwnProperty("orderby")) {
			var ordby = that.result.queryobject.orderby;
			//console.log(that.result.queryobject.orderby);
			if (ordby.length>0) {
				//var ordby = that.result.queryobject.orderby;
				function cmp(cx, cy){
					return cx > cy ? 1 : cx < cy ? -1 : 0;  
				}
				var arrstr = "";
				var arr1 = []
				var arr2 = []
				for (var x=0; x<ordby.length; x++) {
					var oby = ordby[x];
					arr1.push(((oby.sort.toLowerCase().substring(0,4)=="desc")?"-":"") + "cmp(a[\"" + oby.table + "." + oby.field + "\"], b[\"" + oby.table + "." + oby.field + "\"])");
					arr2.push(((oby.sort.toLowerCase().substring(0,4)=="desc")?"-":"") + "cmp(b[\"" + oby.table + "." + oby.field + "\"], a[\"" + oby.table + "." + oby.field + "\"])");
				}
				var sortstr = "that.result.data.sort(function(a,b) {" +
									"return [" + arr1.join(",") + "]<[" + arr2.join(",") + "] ? -1 : 1;" +
								"})";
								//console.log(sortstr);
				eval(sortstr);
			}
		}
	}
		
	var _execute = function(executecallback) {
		//main select
		if (that.result.queryobject.from[0].jointype=="Inner") {
			//console.log(JSON.stringify(that.result.queryobject,null,5));
			//var resp = resolvejoin(that.result.queryobject, that.result.queryobject.from[0]);
			var resp = resolvejoin(that.result.queryobject.from[0]);
			that.result.data  = resp.data;
			that.result.alias = resp.alias;
			add_function_columns(function(err) {
			//normalize_columns(function(err) {
				if (err) {
					executecallback(err);
				} else {
					apply_where(function(err2) {
						if (err2) {
							executecallback(err2);
						} else {
							sortresult();
							apply_groupby(function(err3) {
								if (err3) {
									executecallback(err3);
								} else {
									apply_having(function(err4) {
										if (err4) {
											executecallback(err4);
										} else {
											filter_select_columns(function(err5) {
												executecallback(err5);
											});
										}
									});
								}
							});
						}
					});
				}
			});
		} else {
			if (that.result.queryobject.tablenames.length==1) {
				var vtbls = that.result.queryobject.tablenames[0].split(".");

				var vscount = (srv[vtbls[0]])[vtbls[1]].count();
				var vsdata = (srv[vtbls[0]])[vtbls[1]].range(0,vscount).result;
				var vsalias = that.result.queryobject.tablealiases[0];
				that.result.data = vsdata;	//[];
				
				that.result.alias = vsalias;
				apply_where(function(err) {											// consider adding function columns during where
					markstat("--- where completed");
					if (err) {
						executecallback(err);
					} else {
						add_function_columns(function(err2) {
							markstat("--- addfunctioncolumns completed");
							if (err2) {
								executecallback(err2);
							} else {
								if (that.result.data) {
								if (that.result.data.length>1) {
									sortresult();
									markstat("--- sort completed");
								}
								apply_groupby(function(err3) {
									markstat("--- groupby completed");
									if (err3) {
										executecallback(err3);
									} else {
										apply_having(function(err4) {
											markstat("--- having completed");
											if (err4) {
												executecallback(err4);
											} else {
												filter_select_columns(function(err5) {
													markstat("--- filterselectcolumns completed");
													executecallback(err5);
												});
											}
										});
									}
								});
								} else {
									executecallback("no rows");
								}
							}
						}, "obj2");
					}
				},"obj2");

			} else {
				executecallback("select statement not supported");
			}
		}		
	}

	var parse = function(sqlstr, parsecallback) {
		markstat("tsqlparser parsing");
		if (srv.querycache.hasOwnProperty(sqlstr)) {
			that.result.queryobject = srv.querycache[sqlstr].obj;
			srv.querycache[sqlstr].cnt += 1;
			markstat("tsqlparser parsing completed (from cache)");
			parsecallback(null);
		} else {
			var vsql = 'tsqlparser "' + sqlstr + '"';
			execFile("tsqlparser", [sqlstr], null, function callback(error, stdout, stderr) {
				markstat("tsqlparser parsing completed");
				if (error) {
					parsecallback(error);
				} else {
					markstat("tsqlparser result conversion");
					that.result.queryobject = JSON.parse(stdout);
					clean_queryobject();
					markstat("tsqlparser result conversion completed");
					if (that.result.queryobject.hasOwnProperty("error")) {				
						parsecallback(that.result.queryobject.error);
					} else {
						markstat("validating parse result");
						var verr = validate();
						markstat("validating parse result completed");
						if (verr!="") { 
							parsecallback(verr);
						} else {
							parsecallback(null);
						}
					}
				}
			});
		}	
	}

	//----------------------- BEGIN THE REAL THING
	var _data_cache_cnt = 99;
	this.cacheit = function(sqlstr) {
		markstat("compile result caching");
		if (!srv.querycache.hasOwnProperty(sqlstr)) {
			srv.querycache[sqlstr]={
				obj : that.result.queryobject,
				cnt : 1,
				dat : null,
				time : null
			}
		} 
		var vcacheitem = srv.querycache[sqlstr];
		if (vcacheitem.cnt<_data_cache_cnt && vcacheitem.dat==null) {				
			vcacheitem.dat = that.result.data;
			vcacheitem.time = process.hrtime()[0];
			vcacheitem.cnt=1;
		}
		markstat("compile result caching completed");
	}
	
	this.execute = function(sqlstr, xtoken) {
		markstat("begin");
		var doexec = true;
		if (!srv.querycache) {
			srv.querycache = {};
		} else {
			if (srv.querycache.hasOwnProperty(sqlstr)) {
				var vcacheitem = srv.querycache[sqlstr];
				if (vcacheitem.dat!=null) {
					markstat("getting data from query cache");
					vcacheitem.cnt += 1;
					that.result.data = vcacheitem.dat;
					that.result.queryobject = vcacheitem.obj;
					doexec = false;
					markstat("getting data from query cache completed");
				} else {
					//console.log("cache found but no data");
				}
			} else {
				//console.log("not in cache");
			}
		}
		if (doexec) {
			parse(sqlstr, function(err) {
				if (err) {
					markstat("end0");
					that.emit("ogquery.error", err);
				} else {
					markstat("query execution started");					
					_execute(function(err2) {
						markstat("query execution completed");
						if (err2) {
							markstat("end1");
							that.emit("ogquery.error", err2);
						} else {
							that.cacheit(sqlstr);
							markstat("end2");
							that.emit("ogquery.data", that.result);							
						}
					});
				}
			});
		} else {
			//sortresult();
			that.cacheit(sqlstr);
			markstat("end3");
			that.emit("ogquery.data", that.result);
		}
	}

	this.execute2 = function(sqlstr, xtoken) {
		markstat("begin");
		var doexec = true;
		if (!srv.querycache) {
			srv.querycache = {};
		} else {
			if (srv.querycache.hasOwnProperty(sqlstr)) {
				var vcacheitem = srv.querycache[sqlstr];
				if (vcacheitem.dat!=null) {
					markstat("getting data from query cache");
					vcacheitem.cnt += 1;
					that.result.data = vcacheitem.dat;
					that.result.queryobject = vcacheitem.obj;
					doexec = false;
					markstat("getting data from query cache completed");
				}
			}
		}
		if (doexec) {
			parse(sqlstr, function(err) {
				if (err) {
					markstat("end");
					that.emit("ogquery.error", err);
				} else {
					markstat("query execution started");					
					_execute(function(err2) {
						markstat("query execution completed");
						if (err2) {
							markstat("end");
							that.emit("ogquery.error", err2);
						} else {
							if (srv.netserver && !that.slave) {
								if (Object.keys(srv.netserver.hosts).length>0) {					
									markstat("broadcasting");
									var msg = { "action":"query","from":srv.netserver.ip + ":" + srv.netserver.port,"type":srv.netserver.type, "data":that.result.queryobject };
									srv.netserver.broadcastwithresponse(msg);
									var broadcastresponsetimer = setInterval(function() {
										if (srv.netserver.responsequeue.hasOwnProperty(msg.token)) {
											if (srv.netserver.responsequeue[msg.token].length==Object.keys(srv.netserver.hosts).length) {
												clearInterval(broadcastresponsetimer);
												markstat("broadcasting completed");
												var merge_arr = [];
												var rqobj = srv.netserver.responsequeue[msg.token];											
												for (var x=0; x<rqobj.length; x++) {
													var rqelm = rqobj[x];
													if (rqelm.data)
													merge_arr = merge_arr.concat(rqelm.data.result)
												}
												if (merge_arr.length>0) {
													that.result.data = that.result.data.concat(merge_arr);
													markstat("broadcasting result merged");
												}
												//sortresult();
												that.cacheit(sqlstr);
												markstat("end");
												that.emit("ogquery.data", that.result);
											}
										}
									}, 10);
								} else {
									//sortresult();
									that.cacheit(sqlstr);
									markstat("end");
									that.emit("ogquery.data", that.result);
								}
							} else {
								//sortresult();
								that.cacheit(sqlstr);
								markstat("end");
								that.emit("ogquery.data", that.result);
							}
						}
					});
				}
			});
		} else {
			//sortresult();
			that.cacheit(sqlstr);
			markstat("end");
			that.emit("ogquery.data", that.result);
		}
	}

}

module.exports = ogquery;