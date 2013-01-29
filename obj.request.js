var url = require("url"),
	events = require("events"),	
	util = require("util"),
	async = require("async"),
	
	hlp = require("./obj.utils.js");

var oghttprequest = function(srv, httpreq, vbody) {
	var starttime = process.hrtime();
	events.EventEmitter.call(this);
	srv.info.health.requests.active += 1;
	srv.info.health.requests.recv += 1;	

	var account = null;
	var requeststart = process.hrtime();	
	var requesttoken = httpreq.headers["token"];
	var urlobject=url.parse(httpreq.url.toLowerCase(),true, true);		
	var urlobject2=url.parse(httpreq.url,true, true);		
	var paths = urlobject.pathname.slice(1).split("/");
	
	//props
	this.responsetime = 0;
	this.ip=httpreq.headers["x-real-ip"] || httpreq.connection.remoteAddress || httpreq.headers["x-forwarded-for"];
	this.to = "http://" + srv.info.http_address;	
	this.cross = false;	
	this.from = null;
	if (httpreq.headers["origin"]) {
		this.from = httpreq.headers["origin"];		
		this.cross = true;
	} else {
		this.from = httpreq.headers["host"];		
	}	
	this.zone = paths[0];
	this.reqtype = null;
	this.objectname = null;
	this.objectindex  = null;
	this.action = null;
	this.appcalls = null;
	this.querystring = unescape(urlobject2.search.substring(1,urlobject2.search.length));	
	this.querystring_filter = null;
	this.hash = null;
	if (urlobject.hash) this.hash = urlobject.hash;
	this.requestbody = null;	
	if (vbody) {
		vbody = vbody.toString();
		try {
			this.requestbody = JSON.parse(vbody);
		} catch(errp) {
			console.log(vbody)
			console.log(errp);
		}
		if (this.requestbody) {
			if (this.requestbody.hasOwnProperty("appcalls")) {
				this.appcalls = this.requestbody["appcalls"];
			}
		}
	} 
	var parsequerystring = function(tmpstring) {
		var ops = ["&&","||","==","!=","<>",">=","<=","=",">","<","+","-","*","/"]
		var qsarr = tmpstring.split(/(&&)|(\|\|)|(==)|(\!=)|(\<\>)|(\>=)|(\<=)|(=)|(\>)|(\<)|(\+)|(\-)|(\*)|(\/)/g);
		qsarr = qsarr.filter(function(e) { return e; });		
		for (var x=0; x<qsarr.length; x++) {
			if (ops.indexOf(qsarr[x])>-1) {
				if (qsarr[x]=="=") qsarr[x]="=="
				else if (qsarr[x]=="<>") qsarr[x]="!=";
			} else {
				if (!qsarr[x].isNumeric()) {
					if (qsarr[x].charCodeAt(0)!=34 && qsarr[x].charCodeAt(0)!=39) {
						qsarr[x]="itm." + qsarr[x];
					}
				}
			}
		}
		var tmpstr = qsarr.join("");
		console.log(tmpstr);
		return tmpstr;
	}
	
	if (this.requestbody && this.requestbody.requesttoken) requesttoken = this.requestbody.requesttoken;
	if (this.querystring) this.querystring_filter = parsequerystring(this.querystring);
	//this.actionparams = ((urlobject.query)?urlobject.query:null);
	//console.log(paths);
	if (paths.length>1) {
		this.reqtype = paths[1];		
		if (paths.length>2) {
			this.objectname = paths[2];	
			if (paths.length>3) {
				if (paths[3].isNumeric()) {
					this.objectindex = paths[3];
				}
			}
		}
	}	

	var self = this;
	var http_sys_anon_commands = {
		"gettime" : function(cb) {
			cb(null, process.hrtime());
		},
		"gettoken" : function(cb) {
			var tkn = newrequesttoken(self.requestbody.serverkey);
			account.requesttokens.insert({ token:tkn[2], guid:tkn[1], ip:self.ip, host:self.from }, function() {});
			cb(null,tkn[2]);
		}
	}
	var validate_request = function(cb) {
		var valerr = "";
		if (!srv) valerr = "unable to attach to server"
		else if (self.zone=="") valerr = "account not specified"
		else if (self.reqtype!=undefined && self.reqtype!=null && self.reqtype!="" && self.reqtype!="db" && self.reqtype!="sys" && self.reqtype!="app") valerr = "unsupported request type"
		else if (!self.requestbody) valerr = "request message not found"
		else if (srv.info.databases.indexOf(self.zone)==-1) valerr = "account not found"
		else if (srv[self.zone].info.blocked == true) valerr = "account is blocked"
		//else if (self.server[self.zone].info.tables.indexOf("requestservers")==-1) valerr = "no server is defined to generate requesttokens, contact your administrator for registering your server"
		if (valerr!="") {
			cb(valerr, null);
		} else {
			account = srv[self.zone];
			//validate server key
			account.requestservers.filter(function(itm) { return itm.host == self.from }, function(err,res) {
				if (err) {
					cb("host is not registered with your account, please register : " + err, null);
				} else {
					if (self.requestbody.serverkey==res.result[0].token) {
						//server key ok, now auth user
						var auth_resp = "";
						if (!self.requestbody.auth) {
							auth_resp = "no user credentials supplied";		
						} else {
							self.requestbody.auth = new Buffer(self.requestbody.auth, 'base64').toString('ascii');
							if (self.requestbody.auth==":") {
								auth_resp = "missing user credential";
							} else {
								if (self.requestbody.auth.length<=4) {
									auth_resp = "credential supplied is not supported";
								} else {
									var vauth = self.requestbody.auth.split(":");
									if (vauth.length!=2) {
										auth_resp = "missing user credential";
									} else {
										var usrres = account.users.find(vauth[0]);
										if (usrres.count==0) {
											auth_resp = "incorrect credential";
										} else {
											if (usrres.result[0].password!=vauth[1]) auth_resp = "incorrect credential";
										}
									}
								}
							}
						}
						if (auth_resp=="") {
							//user ok, now auth request
							if (self.objectname!="gettoken") {								
								if (!requesttoken) {
									cb("request token missing, get a new token",null);
								} else {
									if (!account.hasOwnProperty("requesttokens")) {
										cb("request token distribution not enabled",null);
									} else {
										account.requesttokens.filter(function(itm) { if (itm) { return itm.token == requesttoken } else { return false }}, function(err2,res2) {
											if (err2) {
												cb("request token is not valid, get a new token : " + err2,null);
											} else {
												if (res2.count>0) {
												var tok = res2.result[0];
												if (tok.ip==self.ip && tok.host==self.from) {
													//check credits													
													if (account.info.account.credits<=0) {
														valerr="not enough credits";
													} else {
														//do simple request validation here
														if (self.reqtype=="sys") {
															if (!http_sys_anon_commands.hasOwnProperty(self.objectname)) valerr = "unsupported command : " + self.objectname
														} else if (self.reqtype=="db") {
															if (account.info.tables.indexOf(self.objectname)==1) valerr = "invalid table specified : " + self.zone + "." + self.objectname
														} else if (self.reqtype=="app") {
															if (self.objectname) {
																if (srv.apps.indexOf(self.objectname)==-1) {
																	valerr = "invalid app specified : " + self.zone + " : " + self.objectname
																} else {
																	if (self.requestbody.appcalls.indexOf(self.objectname)==-1) self.requestbody.appcalls.apps.push(self.objectname);
																}
															}
														} else {
															if (!self.reqtype && self.requestbody.appcalls) {
																for (var x=0;x<self.requestbody.appcalls.length;x++) {
																	if (srv.apps.indexOf(self.requestbody.appcalls[x])==-1) {
																		valerr = "Invalid app call : " + self.requestbody.appcalls[x];
																		break;
																	}
																	self.reqtype = "app";
																}
															} else {
																//probably unsupported command if we reached this far
															}
														}
													}
													if (valerr!="") {
														cb(valerr, null);
													} else {															
														cb(null,"");
													}
												} else {
													cb("invalid token use, get a new token",null);
												}
												} else {
													cb("token not found or expired, get a new token",null);
												}
											}
										});
									}
								}
							} else {
								cb(null, "");
							}
						} else {
							cb(auth_resp, null);
						}
					} else {
						cb("incorrect serverkey", null)
					}
				}
			})			
		}
	}
	var respond = function(err, res, vcredits) {
		//vcredits = vcredits || 0;
		vcredits = 0;	//for now all db operations are free of charge, payment required only for database size.
		account.setinfo("credits", account.info.account.credits - vcredits);
		var resp = {
			stats 		 : getprocesstime(requeststart),
			server		 : srv.info.http_address,
			account		 : self.zone,
			credits		 : account.info.account.credits,
			responsedate : new Date(),
			requesttoken : requesttoken,
			requestpath	 : urlobject.pathname
		}
		if (err) {
			resp["error"] = err;
			//console.log(resp);
			self.setstatistics(0);
			self.emit("requestcomplete", resp, null);
		} else {
			resp["data"] = res;
			//console.log(resp);
			self.setstatistics(1);
			self.emit("requestcomplete", null, resp);
		}
	}
	var process_sys_level_request = function() {
		if (http_sys_anon_commands.hasOwnProperty(self.objectname)) {
			var resp = http_sys_anon_commands[self.objectname](function(err,res,vcredits) {
				if (err) {
					respond(err,null);
				} else {
					respond(null,res,vcredits);
				}
			});
		} else {
			respond("under construction : " + self.objectname, null);
		}
	}	
	
	var process_table_level_request = function() {
		var _table = self.objectname;
		var _table_index = self.objectindex;
		var _table_filter = self.querystring_filter || self.requestbody.filter;
		var _table_action = self.requestbody.action || "select";
		var _table_data = self.requestbody.data;
		
		var tblacts = ["select","update","insert","delete"]
		if (tblacts.indexOf(_table_action)==-1) {
			respond("unsupported db action : " + _table_action,null);
		} else {
			if (account.info.tables.indexOf(_table)==-1) {
				respond("table (" + _table + ") not found in database " + self.zone,null);
			} else {
				if (_table_action=="select") {
					if (_table_index!=undefined && _table_index!=null) {
						//if (_table_index < account[_table].data.length) {
						if (_table_index < account[_table].count()) {
							respond(null, {
								"stats":process.hrtime(requeststart),
								"count":1,
								//"result":[ account[_table].data[_table_index] ]
								"result":[ account[_table].item(_table_index) ]
							}, 1);
						} else {
							respond("record not found",null,1);
						}
					} else {
						if (_table_filter) {
							var qfiltstr = "var filteval = function(itm) { return " + _table_filter + " }";
							try {
								eval(qfiltstr);
								account[_table].filter(filteval, function(err1,res1) {
									if (err1) {
										respond(err1, null, 1);
									} else {
										respond(null, res1, 1);
									}
								});	
							} catch(er) {
								respond("unsupported filter in call to " + _table,null);
							}
						} else {
							//if (account[_table].data.length>100) {
							if (account[_table].count()>100) {
								//respond(null, account[_table].data.splice(0,100), 1);
								respond(null, account[_table].range(1,100), 1);
							} else {
								//respond(null, account[_table].data, 1);
								respond(null, account[_table].range(1), 1);
							}
						}
					}
				} else {
					if (!_table_data) {
						respond("no data specified for the requested db action : " + _table_action,null);
					} else {
						if (_table_action=="insert") {
							//var vcreds = 1;
							//if (util.isArray(_table_data)) vcreds = _table_data.length;
							account[_table].insert(_table_data, null, function(err,res) { respond(err, res); }, 1);
						} else {
							if (_table_index==undefined || _table_index==null) {
								//this is probably a multi update or multi delete
								//respond("no record index was specified for " + _table_action, null);
								if (_table_action=="update") {
									account[_table].updateFiltered(_table_data, _table_filter, function(err, res) { respond(err, res); }, 1);
								} else {
									//account[_table].deleteFiltered(_table_data, _table_filter, function(err, res) { respond(err, res); }, 1);
									respond("deleteFiltered is under construction", null, 0);
								}								
							} else {
								//if (_table_index < account[_table].data.length) {
								if (_table_index < account[_table].count()) {
									_table_data["__id"] = _table_index;
									if (_table_action=="update") {
										account[_table].update(_table_data, function(err, res) { respond(err, res); }, 1);
									} else {
										account[_table].deleteRow(_table_data, function(err, res) { respond(err, res); }, 1);
									}
								} else {
									respond("record not found", null, 1);
								}									
							}
						}
					}		
				}
			}
		}
	}
	
	var process_db_level_request = function() {
		if (self.objectname) {
			process_table_level_request();
			return;
		}
		/*if (self.requestbody.jql) {
			respond(null, "jql is underconstruction");
		} else {*/
			if (self.requestbody.sql) {
				srv.query(self.requestbody.sql, function(err, res) {
					if (res) res.stats = process.hrtime(requeststart);
					respond(err,res,1);
				});
				//respond(null, "sql is underconstruction");				
			} else {
				respond("invalid database level call",null);
			}
		//}
	}
	
	var _______________process_db_level_request = function() {
		if (self.requestbody.action=="select" || !self.requestbody.action) {
			if (self.objectindex!=undefined) {
				if (self.objectindex < account[self.objectname].data.length) {
					respond(null, account[self.objectname].data[self.objectindex]);
				} else {
					respond("record not found",null);
				}
			} else {
				if (self.querystring_filter) {
					var qfiltstr = "var filteval = function(itm) { return " + self.querystring_filter + " }";
					try {
						eval(qfiltstr);
						account[self.objectname].filter(filteval, function(err1,res1) {
							if (err1) {
								respond(err1, null);
							} else {
								respond(null, res1);
							}
						});	
					} catch(er) {
						respond("unsupported filter in call to " + self.objectname,null);
					}
				} else {
					if (self.objectname && account.hasOwnProperty(self.objectname)) {
						if (account[self.objectname].data.length>100) {
							respond(null, account[self.objectname].data.splice(0,100));
						} else {
							respond(null, account[self.objectname].data);
						}
					} else {
						if (self.requestbody.jql) {
							respond(null, "jql is underconstruction");
						} else {
							if (self.requestbody.sql) {
								respond(null, "sql is underconstruction");
							} else {
								respond("invalid database object : " + self.objectname,null);
							}
						}
					}
				}	
			}
		} else {
			if (self.requestbody.action=="update" || self.requestbody.action=="delete" || self.requestbody.action=="insert") {
				if (!self.requestbody.data) {
					respond("no data specified for the requested db action : " + self.requestbody.action,null);
				} else {
					if (self.objectname && account.hasOwnProperty(self.objectname)) {
						if (self.requestbody.action=="insert") {
							account[self.objectname].insert(self.requestbody.data, function(itm) {
								respond(null, itm);
							});
						} else {
							if (self.objectindex==undefined || self.objectindex==null) {
								respond("no record index was specified for " + self.requestbody.action, null);
							} else {
								if (self.objectindex < account[self.objectname].data.length) {
									//respond(null, dbsrv[self.zone][self.objectname].data[self.objectindex]);
									self.requestbody.data["__id"]=self.objectindex;
									if (self.requestbody.action=="update") {
										account[self.objectname].update(self.requestbody.data, function(err, res) {
											respond(err, res);
										});
									} else {
										account[self.objectname].deleteRow(self.requestbody.data, function(err, res) {
											respond(err, res);
										});
									}
								} else {
									respond("record not found",null);
								}									
							}
						}
					} else {
						respond("invalid database object : " + self.objectname,null);
					}								
				}
			} else {
				respond("unsupported db action : " + self.requestbody.action,null);
			}
		}
	}	
	
	var process_app_level_request = function() {	
		var appiterator = function(app, callback) {
			if (self.requestbody.appcalls.indexOf(app.name)>-1) {
				app.process(self, function(err,appresp) {
					callback(err, appresp, 1);		//instead of 1 use app.credits
				})
			}
		}
		var resparr = [];
		async.map(srv.apps, appiterator, function(result) {
			respond(null, result);
		});
	}	
	this.process = function() {
		validate_request(function(err,res) {
			if (err) {
				respond(err,null);			//for now invalid requests are not charged, soon use db.invalidrequest_credits
			} else {
				//ok process request
				if (self.reqtype=="sys") process_sys_level_request()
				else if (self.reqtype=="db") process_db_level_request()
				else if (self.reqtype=="app") process_app_level_request()
				else if (1==1) respond("request cannot be processed : " + valreq, null);
			}
		});
	}	
	this.setstatistics = function(tip) {
		self.responsedate = new Date();
		self.responsetime = process.hrtime(starttime);
		srv.info.health.requests.active -= 1;
		if (tip==1) srv.info.health.requests.succ += 1;
		if (tip==0) srv.info.health.requests.fail += 1;
		var vtime = ((self.responsetime[0]*1000*1000000) + self.responsetime[1]);
		srv.info.health.requests.responsetimes.total += vtime;
		srv.info.health.requests.responsetimes.avg = Math.round(srv.info.health.requests.responsetimes.total / srv.info.health.requests.succ);
		if (srv.info.health.requests.responsetimes.min>vtime) srv.info.health.requests.responsetimes.min=vtime;
		if (srv.info.health.requests.responsetimes.max<vtime) srv.info.health.requests.responsetimes.max=vtime;
	}
	
	return this;		//for method chaining
}

oghttprequest.prototype = new events.EventEmitter();
module.exports = oghttprequest;


