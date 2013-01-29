var	fs = require("fs"),
	os = require("os"),
	dns = require("dns"),
	stream = require("stream"),
	Buffer = require("buffer").Buffer,
	readline = require("readline"),
	util = require("util"),
	njhttp = require("http"),
	async = require("async"),
	
	ogquery = require("./obj.query.js"),
	hlp = require("./obj.utils.js"),
	ogrequest = require("./obj.request.js");

var getfrom_args = function(vkey) {
	var args = process.argv;
	//args.splice(0,2);
	var res = null;
	for (var x=2; x<args.length; x++) {
		if (args[x]==vkey) {
			if (x+1<args.length) {
				res = args[x+1];
			}
			break;
		}
	}
	return res;
}
	
var getCPUobject = function() {
	var cpus = os.cpus(); 
	var obj = [];
	for (var i=0; i<cpus.length; i++) {
		var cpu = cpus[i];
		var total = 0; 
		for(type in cpu.times) if (typeof cpu.times[type]!="function") total += cpu.times[type]; 
		obj.push(100-Math.round(100 * cpu.times["idle"] / total));
	} 
	var newobj = {
		"avg" : obj.avg(),
		"cores" : obj
	}
	return newobj;
}
	
var httpmodules = {
	"test"	:"./apps/og.test.js",
	"pack"	:"./apps/og.pack.js",
}

var dbpath = "db";	// if shared then a full path like "c:\\sharedroot\\db"; should be defined during startup

var dbserver = function(vsrvname) {	

	vsrvname = vsrvname || "127.0.0.1:8100";
	
	this.info =  {
		name : vsrvname,
		basepath : dbpath,
		maxmem:1024*1024*1024,	//1Gb default for data
		curmem:0,
		datascope:null,
		maxresult:100,			//only 100 record will be returned on queries, a page number needs to be specified
		http_address: [vsrvname.split(":")[0],vsrvname.split(":")[1]],
		http_modules: ["test","pack"],
		health : {
			"type":"",
			"platform":process.platform + "," + process.arch,
			"status":"down",
			"score":0,
			"loadavg":[],
			"uptimes":[],
			"cpus":{},
			"memory":[],
			"requests":{
				"active":0,
				"recv":0,
				"succ":0,
				"fail":0,
				"responsetimes":{
					"total":0,
					"min":999999999,
					"max":0,
					"avg":0
				}
			}
		},
		databases: []
	}
	this.apps = [];
	var thatdbsrv = this;
	var loadapps = function() {
		for (var i=0; i<thatdbsrv.info.http_modules.length; i++) {
			var appname = thatdbsrv.info.http_modules[i];
			var apppath = httpmodules[appname];
			var vind = thatdbsrv.apps.push(require(apppath));
		}
	}
	var sethealth = function() {
		thatdbsrv.info.health.lastcheck = process.hrtime();
		thatdbsrv.info.health.status = "up";
		thatdbsrv.info.health.uptimes = [os.uptime(),process.uptime()];
		thatdbsrv.info.health.memory = [ Math.round((os.freemem()/os.totalmem())*100,2), os.freemem(), os.totalmem()];
		thatdbsrv.info.health.cpus = getCPUobject();
		thatdbsrv.info.health.loadavg = os.loadavg();
		thatdbsrv.info.health.score = thatdbsrv.info.health.requests.active*100 + thatdbsrv.info.health.cpus.avg*10 + thatdbsrv.info.health.memory[0]*2			
	}
	sethealth();
	setInterval(sethealth,1000);
	
	var setup = function() {
		console.log("please wait while setting up dbserver for the first time");
		fs.mkdirSync(thatdbsrv.info.basepath);
		console.log("setup completed");
		console.log("");
	}
	if (!fs.existsSync(this.info.basepath)) setup();

	this.createdb = function(dbname, cb) {
		if (dbname.substring(0,2)=="__") {
				if (typeof cb=="function") {
					cb("Invalid database name");
				} else {
					return "Invalid database name";
				}
		} else {
			if (thatdbsrv.info.databases.indexOf(dbname)==-1) {
				thatdbsrv.info.databases.push(dbname);
				thatdbsrv[dbname]=new db(dbname);
				thatdbsrv[dbname].load(cb);
			} else {
				if (typeof cb=="function") {
					cb("Database already exists");
				} else {
					return "Database already exists";
				}
			}
		}
	}
	this.removedb = function(dbname, cb) {
		var dbindex = thatdbsrv.info.databases.indexOf(dbname);
		if (dbindex>-1) {
			var vpath = thatdbsrv.info.basepath + "\\" + dbname;
			var vdest = thatdbsrv.info.basepath + "\\__trash\\" + dbname;
			fs.rename(vpath, vdest, function(err) {
				if (!err) {
					thatdbsrv.info.databases.splice(dbindex,1);
					delete thatdbsrv[dbname];
					cb(null,"");
				} else {
					cb(err,null); 
				}
			});
		} else {
			if (typeof cb=="function") { 
				cb("not found"); 
			} else {
				return "not found";
			}
		}		
		
	}
	this.renamedb = function(volddbname, vnewdbname, cb) {
		var olddbindex=thatdbsrv.info.databases.indexOf(volddbname);
		if (olddbindex>-1) {
			var vdb = thatdbsrv[volddbname];
			var vpath = thatdbsrv.info.basepath + "\\" + volddbname;
			var vdest = thatdbsrv.info.basepath + "\\" + vnewdbname;			
			fs.rename(vpath, vdest, function(err) {
				if (!err) {
					thatdbsrv.info.databases.splice(olddbindex,1);
					delete thatdbsrv[volddbname];
					thatdbsrv.createdb(vnewdbname, cb);
				} else {
					cb(err,null); 
				}
			});
		} else {
			cb("database with name " + volddbname + " does not exist",null);
		}
	}
	this.save = function(cb) {
		var savearr = [];
		for (var x=0; x<thatdbsrv.info.databases.length; x++) {
			var itm = thatdbsrv.info.databases[x];
			if (thatdbsrv[itm] && itm.substring(0,2)!="__") {
				savearr.push((function(vdb) {
					return function(callback) { 
						var itmtime = process.hrtime();
						vdb.save(function(err, res) { 
							callback(err, { "path":vdb.info.basepath, "time":process.hrtime(itmtime), "files":res });	
						}); 
					}
				})(thatdbsrv[itm]));
			}
		}
		async.parallel(savearr, function(err, results) {
			cb(err,results);
		});	
	}
	this.load = function(cb) {
		loadapps();
		fs.readdir(thatdbsrv.info.basepath, function(err, files) {
			var viterator = function(itm, callback) {
				if (itm.substring(0,2)!="__") {
					thatdbsrv.createdb(itm, callback);
				} else {
					callback();
				}
			}
			async.forEach(files, viterator, cb);
		});
	}
	this.down = function(cb) {
		thatdbsrv.save(cb);
	}

	var db = function(vdbname, dbmaincallback) {
		vdbname = vdbname || "newdb";
		this.info =  {
			basepath : thatdbsrv.info.basepath + "\\" + vdbname,
			devices : {},
			locked : false,
			tables: [],
			account : {
				token:[],
				createdate:null,
				company:null,
				domain:null,
				email:null,
				phone:null,
				fax:null,
				address1:null,
				address2:null,
				city:null,
				state:null,
				postcode:null,
				country:null,
				financecontact:null,
				admincontact:null,
				maincontact:null,
				approved:false,
				blocked:true,
				credits:0
			}
		}

		if (!fs.existsSync(this.info.basepath)) fs.mkdirSync(this.info.basepath);
		if (!fs.existsSync(this.info.basepath + "\\__trash")) fs.mkdirSync(this.info.basepath + "\\__trash");
		if (!fs.existsSync(this.info.basepath + "\\__files")) fs.mkdirSync(this.info.basepath + "\\__files");

		var thatdb = this;
		var infochanged = false;
		
		var checksystemtables = function() {
			//users : user security
			//requesttokens : user tokens
			//requestservers : registered servers/clients that can access db
			var sysreload = true;
			if (thatdb.info.tables.indexOf("users")==-1) {
				console.log("users table missing in db:" + vdbname);			//add startup warning and error queue to display at cls clause initially
				thatdb.info.tables.push("users");								//create a parameter in createtable function for differentiating between 
				thatdb["users"]=new dbtable("users");							//new table creation and registering old table loads
				thatdb["users"].info.name = "users";
				thatdb["users"].info.mode = "db";
				thatdb["users"].info.struct = [
					{ "name": "__id", "type": "numeric", "size":20 },
					{ "name": "userid", "type": "string", "size":100 },
					{ "name": "password", "type": "string", "size":20 },
					{ "name":"isadmin","type":"boolean" }
				];
				thatdb["users"].info.reclen = 171;
				thatdb["users"].info.pk="userid";
				thatdb["users"].save(function(err) { });
				sysreload = sysreload && false;
			}
			if (thatdb.info.tables.indexOf("requesttokens")==-1) {
				console.log("requesttokens table missing in db:" + vdbname);
				thatdb.info.tables.push("requesttokens");
				thatdb["requesttokens"]=new dbtable("requesttokens");
				thatdb["requesttokens"].info.name = "requesttokens";
				thatdb["requesttokens"].info.mode = "db";
				thatdb["requesttokens"].info.struct = [
					{ "name": "__id", "type": "numeric", "size":20 },
					{ "name":"token","type":"string","size":200 },
					{ "name":"guid","type":"string", "size":50 },
					{ "name":"ip","type":"string", "size":15 },
					{ "name":"host","type":"string", "size":200 }
				];
				thatdb["requesttokens"].info.reclen = 485;
				thatdb["requesttokens"].info.pk="token";
				thatdb["requesttokens"].save(function(err) { });
				sysreload = sysreload && false;
			}
			if (thatdb.info.tables.indexOf("requestservers")==-1) {
				console.log("requestservers table missing in db:" + vdbname);
				thatdb.info.tables.push("requestservers");
				thatdb["requestservers"]=new dbtable("requestservers");
				thatdb["requestservers"].info.name = "requestservers";
				thatdb["requestservers"].info.mode = "db";
				thatdb["requestservers"].info.struct = [
					{ "name": "__id", "type": "numeric", "size":20 },
					{ "name":"token","type":"string","size":200 },
					{ "name":"guid","type":"string", "size":50 },
					{ "name":"ip","type":"string", "size":15 },
					{ "name":"host","type":"string", "size":200 }
				];
				thatdb["requestservers"].info.reclen = 485;
				thatdb["requestservers"].info.pk="token";
				thatdb["requestservers"].save(function(err) { });
				sysreload = sysreload && false;
			}
			return sysreload;
		}
		if (!fs.existsSync(this.info.basepath + "\\" + vdbname + ".dbinfo")) {
			this.info.account.token = hlp.newaccounttoken();
			this.info.account.createdate = new Date();
			var infstr = JSON.stringify(this.info);
			fs.writeFile(this.info.basepath + "\\" + vdbname + ".dbinfo", infstr, function(err) { });
		} else {
			var infdata = fs.readFileSync(this.info.basepath + "\\" + vdbname + ".dbinfo", "utf8");	// function(err, res) {
			this.info = JSON.parse(infdata);
			checksystemtables();
		}
		thatdb = this;
		this.setinfo = function(fld, val) {
			thatdb.info.account[fld] = val;
			infochanged = true;
		}
		
		setInterval(function() {
			if (infochanged) {
				fs.writeFile(thatdb.info.basepath + "\\" + vdbname + ".dbinfo", JSON.stringify(thatdb.info), "utf8");
				infochanged=false;
			}
		}, 1000);
		
		this.validate = function() {
			if (thatdb.info.account.blocked) {
				return "database is blocked";
			} else {
				return null;
			}
		}

		this.saveinfo = function(cb) {
			var infstr = JSON.stringify(this.info);
			fs.writeFile(thatdb.info.basepath + "\\" + vdbname + ".dbinfo", infstr, function(err) { 
				if (err) {
					cb(err, null);
				} else {
					cb(null,vdbname + ".dbinfo file saved");
				}
			});
		}
		this.registerserver = function(vip, vhostname, cb) {
			var valres = thatdb.validate();
			if (valres) {
				cb(valres, null);
			} else {
				if (thatdb.info.account.token.length==3) {
					if (thatdb.info.tables.indexOf("requestservers")==-1) {
						cb("syserror : requestserver table does not exist", null);
					} else {
						var srvtok = newservertoken(thatdb.info.account.token[2]);
						var srvobj = { "token":srvtok[2], "guid":srvtok[1], "ip":vip, "host":vhostname }
						thatdb.requestservers.insert(srvobj, function(itm) {
							cb(null, itm);
						});
					}
				} else {
					cb("account token does not exist", null);
				}
			}
		}
		this.createtable = function(tblname, cb) {
			var valres = thatdb.validate();
			if (valres) {
				cb(valres, null);
			} else {
				if (tblname.substring(0,2)=="__") {
					cb("Invalid table name",null);
				} else {
					if (thatdb.info.tables.indexOf(tblname)==-1) {
						thatdb.info.tables.push(tblname);
						thatdb[tblname]=new dbtable(tblname);
						thatdb[tblname].load(cb);
					} else {
						cb("Table already exists",null);
					}
				}
			}
		}
		this.createtable2 = function(tblname, tblinfo, cb) {
			var valres = thatdb.validate();
			if (valres) {
				cb(valres, null);
			} else {
				if (tblname) {
					if (tblinfo) {
						if (tblinfo.hasOwnProperty("struct")) {
							if (tblinfo.hasOwnProperty("pk")) {
								var haspk = false;
								for (var x=0; x<tblinfo.struct.length; x++) {
									if (tblinfo.pk==tblinfo.struct[x].name) {
										haspk=true;
										break;
									}
								}
								if (haspk) {
									if (tblinfo.hasOwnProperty("mode")) {
										if (tblinfo.mode=="db" || tblinfo.mode=="mem") {
											if (thatdb.info.tables.indexOf(tblname)==-1) {
												var sreclen = 0;
												for (var x=0; x<tblinfo.struct.length; x++) {
													if (tblinfo.struct[x].size) {
														sreclen += tblinfo.struct[x].size;
													} else {
														sreclen += 1;
													}
												}
												thatdb.info.tables.push(tblname);								//create a parameter in createtable function for differentiating between 
												thatdb[tblname]=new dbtable(tblname);							//new table creation and registering old table loads
												thatdb[tblname].info = tblinfo
												thatdb[tblname].info.name = tblname;
												thatdb[tblname].info.basepath = thatdb.info.basepath + "\\" + tblname,
												thatdb[tblname].info.reclen = sreclen;
												thatdb[tblname].save(function(err) {
													thatdb[tblname].load(function(err2) {	
														cb(null, "table created");
													});
												});
											} else {
												cb("Table already exists",null);	
											}
										} else {
											cb("Invalid table mode specified",null);	
										}
									} else {
										cb("Table mode not specified",null);	
									}
								} else {
									cb("Table primary key not found in specified struct",null);	
								}
							} else {
								cb("Table primary key not specified",null);	
							}
						} else {
							cb("Table struct not specified",null);	
						}
					} else {
						cb("Table info not specified",null);	
					}
				} else {
					cb("Table name not specified",null);	
				}
			}
		}		
		this.renametable = function(voldtblname, vnewtblname, cb) {
			var valres = thatdb.validate();
			if (valres) {
				cb(valres, null);
			} else {
				var oldtblindex=thatdb.info.tables.indexOf(voldtblname);
				if (oldtblindex>-1) {
					//rename table folder after renaming table files
					var vpath = thatdb.info.basepath + "\\" + voldtblname;
					var vdest = thatdb.info.basepath + "\\" + vnewtblname;
					//rename info file
					var vinfpath = vpath + "\\" + voldtblname + ".info";
					var vinfdest = vpath + "\\" + vnewtblname + ".info";				
					//rename dat file
					var vdatpath = vpath + "\\" + voldtblname + ".dat";
					var vdatdest = vpath + "\\" + vnewtblname + ".dat";

					async.parallel([
						function(callback){
							setTimeout(function(){
								fs.rename(vinfpath, vinfdest, function(err) {
									callback(null, 'one');
								});
							}, 10);
						},
						function(callback){
							setTimeout(function(){
								fs.rename(vdatpath, vdatdest, function(err) { 
									callback(null, 'two');
								});
							}, 10);
						},
					],
					function(err, results){
						fs.rename(vpath, vdest, function(err2) {
							if (!err2) {

								thatdb.info.tables.splice(oldtblindex,1);
								delete thatdb[voldtblname];
								thatdb.createtable(vnewtblname, cb);
							} else {

								cb(err2,null); 
							}
						});	
					});				
				} else {
					cb("table with name " + voldtblname + " does not exist", null);
				}
			}
		}		
		this.removetable = function(tblname, cb) {
			var valres = thatdb.validate();
			if (valres!="") {
				cb(valres, null);
			} else {
				var tblindex = thatdb.info.tables.indexOf(tblname);
				if (tblindex>-1) {
					var vpath = thatdb.info.basepath + "\\" + tblname;
					var vdest = thatdb.info.basepath + "\\__trash\\" + tblname;
					fs.rename(vpath, vdest, function(err) {
						if (!err) {
							thatdb.info.tables.splice(tblindex,1);
							delete thatdb[tblname];
							cb(null,"");
						} else {
							cb(err,null); 
						}
					});
				} else {
					if (typeof cb=="function") { 
						cb("not found"); 
					} else {
						return "not found";
					}
				}		
			}
		}
		this.save = function(cb) {
			var savearr = [];
			for (var x=0; x<thatdb.info.tables.length; x++) {
				var itm = thatdb.info.tables[x];
				if (thatdb[itm] && itm.substring(0,2)!="__") {
					savearr.push((function(vtbl) {
						return function(callback) { 
							var itmtime = process.hrtime();
							vtbl.save(function(err, res) { 
								callback(err, { "path":vtbl.info.basepath, "time":process.hrtime(itmtime), "files":res });	
							}); 
						}
					})(thatdb[itm]));
				}
			}
			async.parallel(savearr, function(err, results) {
				cb(err,results);
			});
		}
		this.load = function(cb) {
			//first load dbinfo file
			fs.readFile(thatdb.info.basepath + "\\" + vdbname + ".dbinfo", function(err, res) {
				if (!err) {
					thatdb.info = JSON.parse(res);
					thatdb.info.tables = [];	//empty tables because they will be reloaded
					fs.readdir(thatdb.info.basepath, function(err, files) {
						var viterator = function(itm, callback) {
							if (itm.substring(0,2)!="__") {
								fs.stat(thatdb.info.basepath + "\\" + itm, function(err, stat) {
									if (stat && stat.isDirectory()) {
										thatdb.createtable(itm, callback);
									} else {
										callback();
									}
								});
							} else {
								callback();
							}
						}				
						async.forEach(files, viterator, function(err) {
							//checksystemtables();
							cb(err);
						});
					});
				}
			});
		}
		
		var dbtable = function(vtblname, tblmaincallback) {
			vtblname = vtblname || "newtable";
			this.info =  {
				basepath : thatdb.info.basepath + "\\" + vtblname,
				struct : [
					{ name: '__id', type: 'numeric', size:20 }
				],
				reclen : 20,
				pk:null,
				mode:"db",
				expiry:null
			}
			var changes = [];
			var accesslog = {};
			var expiryscaninterval = null;
			/*
				change of structure requires rebuilding of database file and array
				change of pk requires rebuilding of index
				convert index and data attributes to local variables, all access should be done over functions
				
				AutoExpire behaviour:
					store stats into file users.access
					- only used if table expiry is defined
					{ "name": '__id', "type": 'numeric', size:20 },					//points to original record
					{ "name": '__createdate', "type": 'numeric', size:15 },			//milliseconds showing the date the record was created
					{ "name": '__lastupdate', "type": 'numeric', size:15 },			//milliseconds showing the date the record was last updated
					{ "name": '__lastupdatecount', "type": 'numeric', size:15 },	//count of record updates since creation time
					{ "name": '__lastaccess', "type": 'numeric', size:15 }			//milliseconds showing the date the record was last accessed
					{ "name": '__lastaccesscount', "type": 'numeric', size:15 },	//count of record access since creation time
																				//access stats are only changed when tbl.find or tbl.item is used

					- Scans records in memory at specific intervals (default is autoexpire_scaninterval = 10seconds)
					- Checks last access date of records and removes records matching the criteria from memory (default is autoexpire_expiryperiod = null, specifying records never expire)			
					An example of expiry is requesttokens, tokens expire after staying idle for 15 minutes
					lastaccess is only changed if records is subject to .item, .range, .find, .update, .insert functions
					expiry function is run right after table load
					expiry function is triggered for each db table if db table's expiry is set
					function is run every expiry_duration set in srvinfo file, if expiry_duration=-1 then no expiry is run at server level
					expiry function removes record from memory, if disk operation is required it is also removed from disk and stored in __expired folder, both are run in parallel
					disk remove nullifies the record, in long term table will get defragmented, and if you want to save some disk space it should be rebuilt, rebuilding will renumber __id fields.
				
				"expiry":{
					"scan":10000,
					"interval":900000,
					"expiredisk":true,		//if set to true, expired records are also removed from disk
					"logdiskexpiry":false,	//if set to true, expired records are moved to _expired folder
					"check":"createdate",	//possible values createdate|lastupdate|lastaccess
					"immexpire":false	 	//when expiry is set the first time, by default all records are marked with current date and time as if they were created recently
											//if you wish to have specific records to have a date in past so that they expire immediately then a filter function should be defined here
											//this is run once when expiry is set and no tbl.access file exists
				},
				
			*/
			var tbllocked = false;
			var aws = null;	//append writer
			this.reccnt = 0;
			this.inmem = 0;
			this.indisk = 0;
			
			var tablerows = [];
			var tablekeys = {};
			
			var EOL = process.platform === "win32" ? new Buffer ([0x0D, 0x0A]) : new Buffer ([0x0A]);
			if (!fs.existsSync(this.info.basepath)) fs.mkdirSync(this.info.basepath);
			if (!fs.existsSync(this.info.basepath + "\\__backup")) fs.mkdirSync(this.info.basepath + "\\__backup");
			if (!fs.existsSync(this.info.basepath + "\\__expired")) fs.mkdirSync(this.info.basepath + "\\__expired");		//expired records are moved to this folder
			
			var thattbl = this;		
			var cloner = function(obj) {
				var str = JSON.stringify(obj);
				return [ JSON.parse(str), str.length ];
			}			
			var __torecbuf = function(obj) {
				var arr = [];
				var buf = new Buffer(thattbl.info.reclen);
				buf.fill(" ");
				var vst = 0;
				for (var j=0; j<thattbl.info.struct.length; j++) {
					var vlen = thattbl.info.struct[j].size;
					var vnm  = thattbl.info.struct[j].name;
					var vtip = thattbl.info.struct[j].type;
					if (obj.hasOwnProperty(vnm)) {
						var vval = obj[vnm];
						if (vtip=="boolean") {
							vval = ((vval)?"1":"0");
						}
						buf.write(vval+"",vst,vlen,"utf8");
					}
					vst+=vlen;
				}
				return buf;
			}
			var __newobj = function() {
				var obj = {};
				for (var j=0; j<thattbl.info.struct.length; j++) {
					obj[thattbl.info.struct[j].name]=null;
				}
				return obj;
			}
			var __toobj = function(str) {
				var obj = __newobj();
				var pos = 0;
				for (var j=0; j<thattbl.info.struct.length; j++) {
					var len = thattbl.info.struct[j].size*1;
					if (thattbl.info.struct[j].type=="boolean") len=1;
					var valstr = str.substring(pos,pos+len).trim();
					if (valstr=="") {
						obj[thattbl.info.struct[j].name]=null;
					} else {
						if (thattbl.info.struct[j].type=="numeric") {
							obj[thattbl.info.struct[j].name]=valstr*1;
						} else {
							if (thattbl.info.struct[j].type=="boolean") {
								obj[thattbl.info.struct[j].name]=((valstr==1)?true:false);
							} else{
								obj[thattbl.info.struct[j].name]=valstr;
							}
						}
					}
					pos += len;
				}
				return obj;
			}
			this.struct = function(cb) {
				if (typeof cb == "function") {
					cb(null, thattbl.info.struct);
				} else {
					return thattbl.info.struct;
				}
			}
			this.count = function(cb) {
				if (typeof cb == "function") {
					cb(null, tablerows.length);
				} else {
					return tablerows.length;
				}
			}
			this.save = function(cb) {
				var infopath = thattbl.info.basepath + "\\" + thattbl.info.name + ".info";	
				fs.writeFile(infopath, JSON.stringify(thattbl.info), function(err) {
					if (!err) {
						var datpath  = thattbl.info.basepath + "\\" + thattbl.info.name + ".dat";
						aws = null;
						thattbl.reccnt = 0;
						async.map(tablerows, function(itm, callback) {
							if (itm) __updaterecord(itm);
							callback();
						}, function(err, res) {
							cb(null);
						});
					} 
					cb(err);
				});
			}
			
			var load_data = function(cb) {
				var datpath  = thattbl.info.basepath + "\\" + vtblname + ".dat";
				fs.exists(datpath, function(exists) {
					if (exists) {
						fs.readFile(datpath, function(err, ddata) {
							if (err) {
								cb(err, null);
							} else {
								tablerows = [];	
								tablekeys = {};
								thattbl.reccnt = 0;
								var posx = 0;
								while (posx<ddata.length) {
									if (thatdbsrv.info.curmem<thatdbsrv.info.maxmem) {		//consider 1024 bytes offer for relax load
										var recstr = ddata.slice(posx, posx+thattbl.info.reclen).toString().replace(/\u0000/gi," ");
										var obj = null;
										if (recstr.substring(20).trim()!="") {
											obj = __toobj(recstr);
										} 
										thatdbsrv.info.curmem += Buffer.byteLength(JSON.stringify(obj), 'utf8');
										var dind = tablerows.push(obj);							
										thattbl.inmem++;
									}
									posx+=thattbl.info.reclen;
									thattbl.reccnt++;
									thattbl.indisk++;
								}
								cb(null, 'one');
							}
						});
					} else {
						fs.writeFile(datpath, "", "utf8");
						cb(null, "dat created");
					}
				});
			}
			var load_index = function(cb) {
				var idxpath  = thattbl.info.basepath + "\\" + vtblname + ".idx";
				fs.readFile(idxpath, function(err, ddata) {
					if (err) {	//probably index file missing - reindex data
						thattbl.reindex(cb);
					} else {
						tablekeys = JSON.parse(ddata);
						cb(null, 'two');
					}
				});
			}			
			this.reindex = function(cb) {
				if (thattbl.info.pk) {
					var idx = thattbl.info.pk;
					tablekeys = {};
					var indexer = function(itm, callback) {
						if (itm) {
							if (itm.hasOwnProperty(idx)) {							
								if (!tablekeys[itm[idx]]) {
									tablekeys[itm[idx]] = [];
								}
								tablekeys[itm[idx]].push(itm["__id"]);
							}
						}
						callback();
					}
					async.map(tablerows, indexer, cb);
					/*async.map(thattbl.data, indexer, function(err, res) {
						//var idxpath  = thattbl.info.basepath + "\\" + thattbl.info.dbname + "." + thattbl.info.name + ".idx";
						var idxpath  = thattbl.info.basepath + "\\" + thattbl.info.name + ".idx";
						fs.writeFile(idxpath, JSON.stringify(thattbl.keys), cb)						
					});*/
				} else {
					cb("Index not specified, unable to index");
				}
			}
			
			var logexpiry = function(vitm) {
				var dt = new Date();
				var vpath = thattbl.info.basepath + "\\__expired\\__id_" + vitm["__id"] + "___" + dt.format("dd.mm.yyyy HH:MM:ss:l").replace(/\./g,"").replace(" ","_").replace(/:/g,"");
				fs.writeFile(vpath, JSON.stringify(vitm), "utf8", function(err) {
					console.log(vpath);
				});
			}
			var expireRowAt = function(ind) {
				var exp = thattbl.info.expiry;
				if (exp.expiredisk) {
					//var itm = tablerows[ind];
					var itmres = thattbl.item(ind);
					if (itmres) {
						if (itmres.count>0) {
							var itm = itmres.result;
							if (exp.logdiskexpiry) logexpiry(cloner(itm)[0]);
							console.log(itm);
							thattbl.deleteRow(itm, function(err,res) {
								//if (err) console.log(err);
								//if (res) console.log(res);
							});
						}
					}
				} else {
					//delete from accesslog
					delete accesslog[ind]
					//remove from tablekeys if exists
					var keyval = tablerows[ind][thattbl.info.pk];
					if (keyval) {
						if (tablekeys.hasOwnProperty(keyval)) {
							if (tablekeys[keyval].indexOf(ind)>-1) {
								tablekeys[keyval][ind]=null;
							}
						}
					}
					//nullify tablerows
					//only nullify related entries in memory
					tablerows[ind]=null;
				}
			}
			var saveexpirylock = false;
			var saveexpiryinterval = null;
			var saveexpirytime = 1*60000;
			var saveexpiry = function() {
				if (!saveexpirylock) {
					saveexpirylock=true;
					var accpath = thattbl.info.basepath + "\\" + vtblname + ".access";	
					fs.writeFile(accpath, JSON.stringify(accesslog), "utf8", function(err) {
						saveexpirylock=false;
					})
				}
			}
			var runexpiry = function() {
				var exp = thattbl.info.expiry;
				if (exp.scan>0 && exp.interval>0) {
					var chkdate = new Date();
					var acckeys = Object.keys(accesslog);
					var viterator = function(itm,callback) {
						var logitm = accesslog[itm];
						if (logitm) {
							if (util.isDate(logitm[exp.check])) {
								var flddate = new Date(logitm[exp.check].getTime() + exp.interval);
								if (flddate<chkdate) setTimeout(expireRowAt,0,itm*1);
							}
						}
						callback();
					}
					async.map(acckeys, viterator, function(err,res) {	});
				} else {
					clearInterval(expiryscaninterval);
					expiryscaninterval=null;
					clearInterval(saveexpiryinterval);
					saveexpiryinterval=null;
					//save file
				}			
			}
			var setaccesslogforrows = function(lst) {
				var exp = thattbl.info.expiry;
				if (lst) {
					if (util.isArray(lst)) {		
						var viterator = function(itm, callback) {
							setaccesslog(itm["__id"],"access");
							callback();
						}
						async.map(lst, viterator, function(err, res) {
							saveexpiry();
							expiryscaninterval = setInterval(runexpiry, exp.scan);
							saveexpiryinterval = setInterval(saveexpiry, saveexpirytime);	
						});
					} else {
						expiryscaninterval = setInterval(runexpiry, exp.scan);
						saveexpiryinterval = setInterval(saveexpiry, saveexpirytime);	
					}
				} else {
					expiryscaninterval = setInterval(runexpiry, exp.scan);
					saveexpiryinterval = setInterval(saveexpiry, saveexpirytime);	
				}
			}
			var setexpiry = function() {
				var exp = thattbl.info.expiry;
				if (exp.scan>0 && exp.interval>0) {
					//for now use JSON formatted file by writing it in async mode
					var accpath = thattbl.info.basepath + "\\" + vtblname + ".access";	
					if (fs.existsSync(accpath)) {
						//read file and populate accesslog array
						fs.readFile(accpath, "utf8", function(err, data) {
							if (!err) {
								accesslog = JSON.parse(data);
							}
							expiryscaninterval = setInterval(runexpiry, exp.scan);
							saveexpiryinterval = setInterval(saveexpiry, saveexpirytime);
						});
					} else {
						//check if exp.immexpire is set, then populate accesslog array according to this, otherwise create blank accesslog
						accesslog=[];
						if (exp.immexpire) {
							if (typeof exp.immexpire == "string") {
								//build expression and filter and set erows to result
								var qfiltstr = "var filteval = function(itm) { if (itm) { return " + exp.immexpire + " } else { return false; } }";
								try {
									eval(qfiltstr);
									thattbl.filter(filteval, function(err1,res1) {
										if (!err1) setTimeout(setaccesslogforrows,0, res1);
									});	
								} catch(er) {
									
								}
								/*thattbl.filter(immexp, function(err, res) {
									if (!err) setTimeout(setaccesslogforrows,0, res);
								});*/
							} else {
								//immediately create expiration records for all objects
								setTimeout(setaccesslogforrows,0, tablerows);
							}
						} else {
							//save file
							expiryscaninterval = setInterval(runexpiry, exp.scan);
							saveexpiryinterval = setInterval(saveexpiry, saveexpirytime);	
						}
					}
				} else {
					if (expiryscaninterval) {
						clearInterval(expiryscaninterval);
						expiryscaninterval=null;
					}
					if (saveexpiryinterval) {
						clearInterval(saveexpiryinterval);
						saveexpiryinterval=null;
					}
				}
			}
			var setaccesslog = function(vid,tip) {
				if (!thattbl.info.expiry) return;				
				/*
					{ "name": '__id', "type": 'numeric', size:20 },					//points to original record
					{ "name": '__createdate', "type": 'numeric', size:15 },			//milliseconds showing the date the record was created
					{ "name": '__lastupdate', "type": 'numeric', size:15 },			//milliseconds showing the date the record was last updated
					{ "name": '__lastupdatecount', "type": 'numeric', size:15 },	//count of record updates since creation time
					{ "name": '__lastaccess', "type": 'numeric', size:15 }			//milliseconds showing the date the record was last accessed
					{ "name": '__lastaccesscount', "type": 'numeric', size:15 },	//count of record access since creation time
				*/
				
				//what happens if scan<interval
				if (!accesslog.hasOwnProperty(vid) || !accesslog[vid]) {
					accesslog[vid] = {
						createdate:new Date(),
						lastupdate:null,
						lastupdatecount:0,
						lastaccess:null,
						accesscount:0
					}
				}
				if (tip=="update") {
					accesslog[vid].lastupdate = new Date();
					accesslog[vid].lastupdatecount = accesslog[vid].lastupdatecount + 1;
				} else {
					if (tip=="access") {
						accesslog[vid].lastaccess = new Date();
						accesslog[vid].accesscount = accesslog[vid].accesscount + 1;
					}
				}
			}
			this.load = function(cb) {			
				var infopath = thattbl.info.basepath + "\\" + vtblname + ".info";	
				fs.readFile(infopath, "utf8", function(err, data) {
					if (data) {
						thattbl.info = JSON.parse(data);
						var infchanged = false;
						if (thattbl.info.name != vtblname) {
							thattbl.info.name = vtblname;
							infchanged=true;
						}
						if (thattbl.info.basepath != thatdb.info.basepath + "\\" + vtblname) {
							thattbl.info.basepath = thatdb.info.basepath + "\\" + vtblname;
							infchanged=true;
						}
						if (infchanged) fs.writeFile(infopath, JSON.stringify(thattbl.info), "utf8");
						if (thattbl.info.mode=="db") {
							load_data(function(err, res) {								
								if (err) {
									cb(err,null);
								} else {
									//check if expiry is set
									if (thattbl.info.expiry) setexpiry();
									thattbl.reindex(cb);	//temporarily use live indexes instead of storing in idx files
								}
							});
						} else {
							cb();
						}
					} else {
						cb();
					}
				});
			}		
			
			//**************** ROW based Private Functions ****************
			var __validatefieldvalue = function(fld,val) {
				return true;
			}
			var __appendrecord = function(obj, isdisk, cb) {
				isdisk = isdisk || false;
				if (!tbllocked) {
					tbllocked=true;
					if (!aws) {
						var datpath  = thattbl.info.basepath + "\\" + vtblname + ".dat";	
						aws = fs.createWriteStream(datpath, { flags:"a" });
					}
					obj["__id"]=thattbl.reccnt;
					if (!isdisk) tablerows.push(obj);
					if (thattbl.info.mode=="db") {
						var recbuf = __torecbuf(obj);
						aws.write(recbuf);
					}
					setaccesslog(obj["__id"], "update");
					//add to index
					if (thattbl.info.pk && !isdisk) {
						var idx = thattbl.info.pk;
						if (obj.hasOwnProperty(idx)) {
							var idxval = obj[idx];

							if (!tablekeys[idxval]) {
								tablekeys[idxval] = [];
							}
							var rowind = obj["__id"];
							if (tablekeys[idxval].indexOf(rowind)==-1) {
								tablekeys[idxval].push(obj["__id"]);
							}
						}
					}
					thattbl.reccnt++;
					tbllocked=false;
					if (typeof cb == "function") cb(obj);
				} else {
					setTimeout(__appendrecord,1,obj,isdisk,cb);
				}
			}
			var __updaterecord = function(obj, isdel) {
				isdel = isdel || false;
				if (obj.hasOwnProperty("__id")) {
					var oind = obj["__id"];
					var oldobj = tablerows[oind*1];
					if (oldobj) {
						//validate if object is ok, fields exists in struct and correctly set						
						var farr = Object.keys(obj);
						var issame = true;
						for (var x=0; x<farr.length; x++) {
							if (!__validatefieldvalue(farr[x],obj[farr[x]])) return "invalid field name";
							if (obj[farr[x]]!=oldobj[farr[x]]) issame = false;
						}
						//proceed only if oldobj != obj
						if (issame) return "same data already exists, update ignored";
						//merge inexisting fields
						for (var x=0; x<thattbl.info.struct.length; x++) {
							var fname = thattbl.info.struct[x].name;
							if (farr.indexOf(fname)==-1) {
								if (oldobj.hasOwnProperty(fname)) {
									obj[fname]=oldobj[fname];
								}
							}
						}
						oldobj = cloner(oldobj)[0];
						tablerows[oind*1]=obj;
						if (thattbl.info.mode=="db") {
							var sindx = thattbl.info.reclen*oind*1;
							var datpath  = thattbl.info.basepath + "\\" + vtblname + ".dat";	
							var ws = fs.createWriteStream(datpath,{ flags:"r+", start:sindx });	//, { flags:"w" });
							var objdata = __torecbuf(obj);
							ws.write(objdata);
						} else {
							changes.push(oind*1);
						}
						if (!isdel) setaccesslog(oind*1, "update");
						//modify index entry
						if (thattbl.info.pk) {
							var idx = thattbl.info.pk;
							//first remove from old index entry
							if (oldobj.hasOwnProperty(idx)) {
								if (tablekeys[oldobj[idx]]) {
									var rowind = oldobj["__id"];
									var indind = tablekeys[oldobj[idx]].indexOf(rowind);
									if (indind==-1) 
										tablekeys[oldobj[idx]].splice(indind+1,1);
										if (tablekeys[oldobj[idx]].length==0) {
											delete tablekeys[oldobj[idx]];
										}
								}
							}
							//then add to new index entry
							if (obj.hasOwnProperty(idx)) {
								if (!tablekeys[obj[idx]]) {
									tablekeys[obj[idx]] = [];
								}
								var rowind = obj["__id"];
								if (tablekeys[obj[idx]].indexOf(rowind)==-1) 
									tablekeys[obj[idx]].push(obj["__id"]);
							}
						}
					} else {
						return "object not found";
					}
				} else {
					return "object missing __id field";
				}
				return null;
			}
			
			//**************** ROW based PUBLIC Functions ****************
			this.insert = function(obj, oninsert, oncomplete) {
				//validate fields
				if (obj instanceof Array) {
					var viterator = function(vitm, callback) {
						__appendrecord(vitm, false, oninsert);
						callback();
					}
					async.map(obj, viterator, function(err,res) {
						if (typeof oncomplete=="function") {
							if (err) {
								oncomplete(err,null);
							} else {
								oncomplete(null,"insert completed");
							}
						}
					});
				} else {					
					if (typeof oncomplete=="function") {
						__appendrecord(obj, false, function(res) {
							oncomplete(null,res);
						});
					} else {
						if (typeof oninsert=="function") {
							__appendrecord(obj, false, function(res) {
								oninsert(null,res);
							});
						} else {
							console.log("callback function missing in call to insert : " + thatdbsrv.info.name + "." + vtblname);
						}
					}
				}
			}		
			this.update = function(obj,cb) {
				if (obj.hasOwnProperty("__id")) {
					var vind = obj.__id;
					var resp = __updaterecord(obj,vind);
					if (resp) {
						cb(resp,null);
					} else {
						cb(null,"record updated");
					}
				} else {
					cb("record not found",null);
				}
			}
			this.updatebyPK = function(obj, cb) {
				var objarr = [];
				if (obj instanceof Array) {
					objarr = obj;
				} else {
					objarr.push(obj);
				}
				var viterator = function(itm, callback) {
					console.log(itm[thattbl.info.pk]);
					var ores = thattbl.find(itm[thattbl.info.pk]);
					console.log(ores);
					if (ores) {
						if (ores.count>0) {
							var vind = ores.result[0].__id
							itm.__id = vind;
							__updaterecord(itm, vind);
						}
					}
					callback();
				}
				async.map(obj, viterator, function(err,res) {
					cb(null,"update completed");
				});
			}
			var _updatefiltered = function(lst, obj, cb) {
				async.map(lst, function(itm, callback) {
					if (itm) {
						var vitm = cloner(itm)[0];
						var updflds = Object.keys(obj);
						for (var x=0; x<updflds.length; x++) {
							if (vitm.hasOwnProperty(updflds[x]) && updflds!="__id") {
								vitm[updflds[x]] = obj[updflds[x]];
							}
						}
						__updaterecord(vitm);
					}
					callback();
				}, function(err, res) {
					cb(null,"update completed");
				});
			}
			this.updateFiltered = function(obj, filt, cb) {
				if (!obj) {
					cb("update data object is required",null);
				} else {
					if (obj.hasOwnProperty("__id")) {
						cb("__id field not supported in filtered updates",null);
					} else {
						if (filt) {
							var qfiltstr = "var filteval = function(itm) { if (itm) { return " + filt + " } else { return false; } }";
							try {
								eval(qfiltstr);
								thattbl.filter(filteval, function(err1,res1) {
									if (err1) {
										cb(err1, null, 1);
									} else {
										_updatefiltered(res1.result, obj, cb);
									}
								});	
							} catch(er) {
								cb("unsupported filter in update (" + qfiltstr + ")", null);
							}
						} else {
							//there is no filter then update all ?
							//_updatefiltered(thattbl.data, obj, cb);
							cb("filter expression required in table level updates",null);
						}
					}
				}				
			}
			var _deletefiltered = function(lst, cb) {
				async.map(lst, function(itm, callback) {
					if (itm) {
						thattbl.deleteRow(itm, callback);
					} else {
						callback();
					}
				}, function(err, res) {
					cb(null,"delete completed");
				});
			}
			this.deleteFiltered = function(filt, cb) {
				if (filt) {
					var qfiltstr = "var filteval = function(itm) { if (itm) { return " + filt + " } else { return false; } }";
					try {
						eval(qfiltstr);
						thattbl.filter(filteval, function(err1,res1) {
							if (err1) {
								cb(err1, null, 1);
							} else {
								_deletefiltered(res1.result, obj, cb);
							}
						});	
					} catch(er) {
						cb("unsupported filter in delete (" + qfiltstr + ")", null);
					}
				} else {
					//there is no filter then delete all ?
					//_deletefiltered(thattbl.data, obj, cb);
					cb("filter expression required in table level deletes",null);
				}
			}
			
			this.deleteRow = function(obj, cb) {
				if (obj) {
					if (obj.hasOwnProperty("__id")) {
						var vind = obj.__id;
						for (var itm in obj) {
							if (itm!="__id") obj[itm]="";
						}
						var resp = __updaterecord(obj,vind,true);
						if (resp) {
							cb(resp,null);
							return false;
						} else {
							tablerows[vind*1]=null;
							cb(null,"record deleted");
							return true;
						}
					} else {
						cb("record not found",null);
						return false;
					}
				} else {
					cb("no record was supplied",null);
					return false;
				}
			}
			this.item = function(vind,onitemcomplete) {
				var findtime = process.hrtime();
				var itm = tablerows[vind];
				if (itm) {
					var resp = {
						stats : null,
						count : 1,
						result : cloner(itm)[0]
					}
					setaccesslog(vind, "access");
					resp.stats = process.hrtime(findtime);
					if (typeof onitemcomplete=="function") {
						return onitemcomplete(null, resp);
					} else {
						return resp;
					}
				} else {
					var resp = {
						stats : process.hrtime(findtime),
						count : 0,				
						result : "not found"
					}
					if (typeof onitemcomplete=="function") {
						return onitemcomplete(resp, null);
					} else {
						return resp;
					}	
				}
			}
			this.find = function(vkeyval, onfindcomplete) {
				var findtime = process.hrtime();
				if (tablekeys.hasOwnProperty(vkeyval)) {
					var vindlist = tablekeys[vkeyval];
					var arr = [];
					for (var x=0; x<vindlist.length; x++) {
						setaccesslog(vindlist[x], "access");
						var obj = tablerows[vindlist[x]];
						arr.push(cloner(obj)[0]);
					}
					var resp = {									//use new response object model
						stats : null,
						count : arr.length,
						result : ((arr.length>0)?arr:"not found")	//cloner(thattbl.data[vind])[0]
					}
					resp.stats = process.hrtime(findtime);
					if (typeof onfindcomplete=="function") {
						return onfindcomplete(null, resp);
					} else {
						return resp;
					}
				} else {
					var resp = {
						stats : process.hrtime(findtime),
						count : 0,				
						result : "not found"
					}
					if (typeof onfindcomplete=="function") {
						return onfindcomplete(resp, null);
					} else {
						return resp;
					}				
				}
			}
			this.rows = function(vstart, vend, oncomplete) {
				thattbl.range(vstart,vend,oncomplete);
			}
			this.range = function(vstart, vend, oncomplete) {
				var rangetime = process.hrtime();
				var err = "";
				if (typeof vstart=="function") {
					oncomplete = vstart;
					vstart=null;
					vend=null;
				} else {
					if (typeof vend=="function") {
						oncomplete = vend;
						vend=null;
					}
				}
				vstart = vstart || 1;
				vend = vend || tablerows.length;
				if (!(vstart + "").isInteger()) {
					err = "start index should be integer";
				} else if (!(vend + "").isInteger()) {
					err = "end index should be integer";
				} else if (vstart*1<=0) {
					err = "range function does not support zero based seeks";
				}
				if (err!="") {
					var resp = {
						stats : process.hrtime(rangetime),
						count : 0,
						result : err
					}
					if (typeof oncomplete == "function") {
						oncomplete(resp, null);
					} else {
						return resp;
					}
				} else {
					if (vend*1>tablerows.length) vend = tablerows.length;
					console.log(vstart + " - " + vend);
					var vrange = tablerows.slice(vstart-1, vend);
					for (var x=vstart*1-1; x<vend*1+1; x++) {
						setaccesslog(x, "access");
					}
					var resp = {
						stats : null,
						count : vrange.length,
						result : cloner(vrange)[0]
					}
					resp.stats = process.hrtime(rangetime);
					if (typeof oncomplete == "function") {
						oncomplete(null, resp);
					} else {
						return resp;
					}
				}		
			}
			this.filter = function(evaluator, oncomplete) {
				var filtertime = process.hrtime();
				var viterator = function(itm, callback) {
					var res = evaluator(itm); 
					if (res) {
						setaccesslog(itm["__id"], "access");
					}
					callback(res);
				}
				async.filter(tablerows, viterator, function(results) {
					var resptime = process.hrtime(filtertime);
					var resp = {
						stats : resptime,
						count : ((results)?results.length:0),
						result : results
					}
					if (typeof oncomplete=="function") oncomplete(null, resp);
				});	
			}
		}			
	}

	this.query = function(qry, cb, vslave, vtoken) {
		vslave = vslave || false;
		vtoken = vtoken || "";
		var _qtme = process.hrtime();
		var q = new ogquery(thatdbsrv, vslave);
		
		//consider methods to do disk query if thattbl.inmem<thattbl.indisk
				
		q.on("ogquery.error", function(err) {
			var _qtme2 = process.hrtime(_qtme);
			cb(err + "\r\n0 row(s) selected in " + _qtme2, null);
		});
		q.on("ogquery.data", function(result) {
			var _qtme2 = process.hrtime(_qtme);
			var resp = {
				stats : result.stats,
				count : result.data.length,
				result : result.data
			}
			cb(null, resp);
			//cb(null, "\r\n" + JSON.stringify(result.stats) + "\r\n" + JSON.stringify(result.data,null,5) + "\r\n" + result.data.length + " row(s) selected in " + _qtme2);
		});
		q.execute(qry, vtoken);
	}
	
	this.consolecommands = {
			"cls"	: function() {
						var vlines = [];
						vlines.push(hlp.cute(["yellow","bold"]) + "HTTP DB server" + hlp.cute(["off"]));
						vlines.push("Version 1.0.0 - 29.01.2013 02:50");
						vlines.push("cemguler@buton.com.tr");
						vlines.push("");
						vlines.push("dbserver activated");
						if (thatdbsrv.http) vlines.push("httpserver listening on " + thatdbsrv.info.http_address.join(":")); 
						hlp.cls();
						hlp.boxer(80, vlines, true);
					},
			"multi on" : function() {
						that.multiline=true;
					},
			"multi off" : function() {
						that.multiline=false;
					},
			"mem" 	: function() { 
						console.log("");
						console.log(hlp.cute(["white","bold"]) + "OS Memory Usage:" + hlp.cute(["off"]));
						console.log("Total Memory = " + os.totalmem() + " bytes"); 
						console.log("Free Memory  = " + os.freemem() + " bytes"); 
						console.log("");
						console.log(hlp.cute(["white","bold"]) + "DbServer Memory Usage:" + hlp.cute(["off"]));
						var procmem = process.memoryUsage();
						console.log("RSS          = " + procmem.rss + " bytes"); 
						console.log("Heap Total   = " + procmem.heapTotal + " bytes"); 
						console.log("Heap Used    = " + procmem.heapUsed + " bytes"); 
						console.log("");
						console.log(hlp.cute(["white","bold"]) + "Data Memory Usage:" + hlp.cute(["off"]));
						console.log("Data MaxMem  = " + thatdbsrv.info.maxmem + " bytes");
						console.log("Data CurMem  = " + thatdbsrv.info.curmem + " bytes");
						var vutilization = Math.round((thatdbsrv.info.curmem/thatdbsrv.info.maxmem)*1000,3)/1000;
						var vutilwarn = false;
						if (vutilization>0.75) {
							console.log(hlp.cute(["red","bold"]) + "Data Utilization = " + vutilization + " [WARNING]" + hlp.cute(["off"]));
						} else {
							console.log(hlp.cute(["green","bold"]) + "Data Utilization = " + vutilization + " [OK]" + hlp.cute(["off"]));
						}
						//console.log(hlp.cute(["off"]));

						console.log("");
				},
			"exit"	: function() { process.exit(); }
	}
	this.consoleevaluator = function(args, cb, vslave, vtoken) {
		vslave = vslave || false;
		vtoken = vtoken || "-";
		if (args[0]=="select" || args[0]=="xxx") {			
			var _qtme = process.hrtime();
			var q = new ogquery(thatdbsrv);
			q.on("ogquery.error", function(err) {
				var _qtme2 = process.hrtime(_qtme);
				cb(err + "\r\n0 row(s) selected in " + _qtme2, null);
			});
			q.on("ogquery.data", function(result) {
				var _qtme2 = process.hrtime(_qtme);
				var resp = {
					stats : result.stats,
					count : result.data.length,
					result : result.data
				}
				cb(null, resp);
			});
			if (args[0]=="xxx") {
				q.execute("select * from buton.users where userid='ccem'", vtoken);
			} else {
				q.execute(args.join(" "), vtoken);
			}
		} else {
			if (args[0].substring(0,8)=="dbserver") {
				try {
					//convert to current object=that
					var vargs = args[0].split(".");
					vargs[0]="thatdbsrv";
					args[0] = vargs.join(".");
					var newargs = args.join(" ");
					if (newargs.indexOf("(")>-1) {
						//modify parameters of call by adding callback
						var xargs = args.join(" ");
						xargs = xargs.slice(0, -1);
						var xargsarr = xargs.split("(");
						var vpararr = eval("[ " + xargsarr.slice(1).join("(") + " ]");

						var defs = newargs.split("(");
						var xxxfn = defs[0].split(".").slice(0,-1).join(".");
						if (util.isArray(eval(xxxfn))) {
							console.log("newargs2:");
							console.log(newargs);
							return cb(null, eval(newargs));
						} else {
							var voriparnum = eval(defs[0] + ".length");
							if (vpararr.length>=voriparnum) {
								vpararr.length = voriparnum - 1;
							}
							var cbfn = function(err, res) {
								if (typeof cb=="function") cb(err,res);
							}
							vpararr.push(cbfn);
							for (var x=0; x<vpararr.length; x++) {
								if (typeof vpararr[x]=="string") vpararr[x]='"' + vpararr[x] + '"';
								if (typeof vpararr[x]=="object") vpararr[x]=JSON.stringify(vpararr[x]);
							}
							var fnstr = defs[0] + "(" + vpararr.join(",") + ")";
							return eval(fnstr);
						}						
					} else {
						//probably an object
						return cb(null, eval(newargs));
					}
				} catch(errx) {
					cb("unknown db command : " + errx, null);
				}
			} else {
				cb(null,null);
			}
		}
	}
	this.processconsolecommand = function(pcmd, cb) {
		if (pcmd=="") {
			if (typeof cb == "function") cb();
			return;
		}
		var vargs = pcmd.split(" ");
		var cmd = vargs[0];					
		if (thatdbsrv.consolecommands.hasOwnProperty(pcmd)) {
			thatdbsrv.consolecommands[pcmd]();
			if (typeof cb == "function") cb();
			return;
		} else {
			thatdbsrv.consoleevaluator(vargs, function(err, res) {
				if (err) console.log(err);
				if (res) console.log(res);
				if (err==null && res==null) console.log("y1 unknown command : " + cmd);
				if (typeof cb == "function") cb();
				return;
			});
		}
	}	
	this.initconsole = function() {
		thatdbsrv.processconsolecommand("cls");
		this.rl = readline.createInterface({ input: process.stdin, output: process.stdout });	
		this.rl.on("close", function() { process.exit(); });
		this.rl_lines = [];
		this.multiline = false;
		var othat = this;
		this.getinput = function() {
			othat.rl.question(((othat.rl_lines.length==0)?"> ":".."), function(answer) {
				if (answer.indexOf("\n")>-1) console.log("new line 1");
				if (answer.indexOf("\r")>-1) console.log("new line 2");
				if (!othat.multiline) {
					thatdbsrv.processconsolecommand(answer, function() {
						othat.getinput();
					});
				} else {
					if (answer!="run") {
						othat.rl_lines.push(answer);
						othat.getinput();
					} else {
						var cmd = othat.rl_lines.join("\r\n");
						othat.rl_lines.length=0;
						othat.multiline=false;
						thatdbsrv.processconsolecommand(cmd, function() {
							othat.getinput();
						});
					}
				}
			});
		}
		this.getinput();
	}

	
	var http_sys_anon_commands = {
		"gettime" : function(req, cb) {
			cb(null, new Date());
		},
		"gettoken" : function(req, cb) {
			var tkn = newrequesttoken(req.body.serverkey);
			thatdbsrv[req.accountname].requesttokens.insert({ token:tkn[2], guid:tkn[1], ip:req.ip, host:req.from }, function() {});
			cb(null,tkn[2]);
		}
	}
	/*
	var db_admin_commands = {
		"createtable" : function(req,cb) {
			if (req.objectname) {
			}
		}
	}
	*/
	var validate_request = function(req, cb) {
		var valerr = "";
		if (req.accountname=="") valerr = "account not specified"
		else if (req.type!=undefined && req.type!=null && req.type!="" && req.type!="db" && req.type!="sys" && req.type!="app") valerr = "unsupported request type"
		else if (!req.body) valerr = "request message not found"
		else if (thatdbsrv.info.databases.indexOf(req.accountname)==-1) valerr = "account not found"
		else if (thatdbsrv[req.accountname].info.blocked == true) valerr = "account is blocked"
		//else if (req.server[req.accountname].info.tables.indexOf("requestservers")==-1) valerr = "no server is defined to generate requesttokens, contact your administrator for registering your server"
		if (valerr!="") {
			cb(valerr, null);
		} else {
			var account = thatdbsrv[req.accountname];
			//validate server key
			account.requestservers.filter(function(itm) { return itm.host == req.from }, function(err,res) {
				if (err) {
					cb("host is not registered with your account, please register : " + err, null);
				} else {
					if (req.body.serverkey==res.result[0].token) {
						//server key ok, now auth user
						var auth_resp = "";
						if (!req.body.auth) {
							auth_resp = "no user credentials supplied";		
						} else {
							req.body.auth = new Buffer(req.body.auth, 'base64').toString('ascii');
							if (req.body.auth==":") {
								auth_resp = "missing user credential";
							} else {
								if (req.body.auth.length<=4) {
									auth_resp = "credential supplied is not supported";
								} else {
									var vauth = req.body.auth.split(":");
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
							if (req.objectname!="gettoken") {								
								if (!req.body.requesttoken) {
									cb("request token missing, get a new token",null);
								} else {
									if (!account.hasOwnProperty("requesttokens")) {
										cb("request token distribution not enabled",null);
									} else {
										account.requesttokens.filter(function(itm) { if (itm) { return itm.token == req.body.requesttoken } else { return false }}, function(err2,res2) {
											if (err2) {
												cb("request token is not valid, get a new token : " + err2,null);
											} else {
												if (res2.count>0) {
												var tok = res2.result[0];
												if (tok.ip==req.ip && tok.host==req.from) {
													//check credits													
													if (account.info.account.credits<=0) {
														valerr="not enough credits";
													} else {
														//do simple request validation here
														if (req.type=="sys") {
															if (!http_sys_anon_commands.hasOwnProperty(req.objectname)) valerr = "unsupported command : " + req.objectname
														} else if (req.type=="db") {
															if (account.info.tables.indexOf(req.objectname)==-1 && req.body.action!="createtable") valerr = "invalid table specified : " + req.accountname + "." + req.objectname
														} else if (req.type=="app") {
															if (req.objectname) {
																if (thatdbsrv.info.http_modules.indexOf(req.objectname)==-1) {
																	valerr = "invalid app specified : " + req.accountname + " : " + req.objectname
																} else {
																	if (req.body.appcalls.indexOf(req.objectname)==-1) req.body.appcalls.apps.push(req.objectname);
																}
															}
														} else {
															if (!req.type && req.body.appcalls) {
																for (var x=0;x<req.body.appcalls.length;x++) {
																	if (thatdbsrv.info.http_modules.indexOf(req.body.appcalls[x])==-1) {
																		valerr = "Invalid app call : " + req.body.appcalls[x];
																		break;
																	}
																	req.type = "app";
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

	var process_sys_level_request = function(req, cb) {
		if (http_sys_anon_commands.hasOwnProperty(req.objectname)) {
			var resp = http_sys_anon_commands[req.objectname](req, function(err,res,vcredits) {
				if (err) {
					cb(err,null);
				} else {
					cb(null,res,vcredits);
				}
			});
		} else {
			cb("under construction : " + self.objectname, null);
		}
	}
		
	var process_table_level_request = function(req,cb) {
		var _table = req.objectname;
		var _table_index = req.objectindex;
		var _table_filter = req.querystring_filter || req.body.filter;
		var _table_action = req.body.action || "select";
		var _table_data = req.body.data;
		var tblacts = ["select","update","insert","delete","updatebyPK"];
		var tblacts2 = ["createtable","renametable"]
		var account = thatdbsrv[req.accountname];
		
		if (tblacts.indexOf(_table_action)==-1 && tblacts2.indexOf(_table_action)==-1) {
			cb("unsupported db action : " + _table_action,null);
		} else {
			if (account.info.tables.indexOf(_table)==-1 && _table_action!="createtable") {
				cb("table (" + _table + ") not found in database " + req.accountname,null);
			} else {
				if (_table_action=="select") {
					if (_table_index!=undefined && _table_index!=null) {
						//if (_table_index < account[_table].data.length) {
						if (_table_index < account[_table].count()) {
							cb(null, {
								"stats":process.hrtime(requeststart),
								"count":1,
								"result":[ account[_table].item(_table_index) ]
							}, 1);
						} else {
							cb("record not found",null,1);
						}
					} else {
						if (_table_filter) {
							var qfiltstr = "var filteval = function(itm) { return " + _table_filter + " }";
							try {
								eval(qfiltstr);
								account[_table].filter(filteval, function(err1,res1) {
									if (err1) {
										cb(err1, null, 1);
									} else {
										cb(null, res1, 1);
									}
								});	
							} catch(er) {
								cb("unsupported filter in call to " + _table,null);
							}
						} else {
							if (account[_table].count()>100) {
								cb(null, account[_table].range(1,100), 1);
							} else {
								cb(null, account[_table].range(1), 1);
							}
						}
					}
				} else {
					if (!_table_data && _table_action!="createtable" && _table_action!="renametable" && _table_action!="deletetable") {
						cb("no data specified for the requested db action : " + _table_action,null);
					} else {
						if (_table_action=="insert") {
							account[_table].insert(_table_data, null, function(err,res) { cb(err, res); }, 1);
						} else {
							if (_table_index==undefined || _table_index==null) {
								//this is probably a multi update or multi delete
								if (_table_action=="update") {
									account[_table].updateFiltered(_table_data, _table_filter, function(err, res) { cb(err, res); }, 1);
								} else {
									if (_table_action=="updatebyPK") {
										account[_table].updatebyPK(_table_data, function(err, res) { cb(err, res); }, 1);
									} else {
										if (_table_action=="createtable") {
											if (req.body.hasOwnProperty("objectinfo")) {
												account.createtable2(_table, req.body.objectinfo, function(err2,res2) {
													cb(err2,res2, 0);
												})
											} else {
												cb("table definition missing for createtable operation", null, 0);
											}
										} else {
											if (_table_action=="renametable") {
												if (req.body.hasOwnProperty("objectname2")) {
													account.renametable(_table, req.body.objectname2, function(err2,res2) {
														cb(err2, res2, 0);
													})
												} else {
													cb("new table name not specified", null, 0);
												}
											} else {
												//account[_table].deleteFiltered(_table_data, _table_filter, function(err, res) { respond(err, res); }, 1);
												cb("deleteFiltered is under construction", null, 0);
											}
										}
									}
								}								
							} else {
								if (_table_index < account[_table].count()) {
									_table_data["__id"] = _table_index;
									if (_table_action=="update") {
										account[_table].update(_table_data, function(err, res) { cb(err, res); }, 1);
									} else {
										if (_table_action=="updatebyPK") {
											account[_table].updatebyPK(_table_data, function(err, res) { cb(err, res); }, 1);
										} else {
											if (_table_action=="delete") {
												account[_table].deleteRow(_table_data, function(err, res) { cb(err, res); }, 1);
											} else {
												cb("unsupported table action : " + _table_action, null, 1);
											}
										}
									}
								} else {
									cb("record not found", null, 1);
								}									
							}
						}
					}		
				}
			}
		}
	}
	
	var process_db_level_request = function(req, cb) {
		if (req.objectname) {
			process_table_level_request(req, cb);
		} else {
		/*if (self.requestbody.jql) {
			respond(null, "jql is underconstruction");
		} else {*/
			if (req.body.sql) {
				thatdbsrv.query(req.requestbody.sql, function(err, res) {
					if (res) res.stats = process.hrtime(requeststart);
					cb(err,res,1);
				});
				//respond(null, "sql is underconstruction");				
			} else {
				/*if (db_admin_commands.hasOwnProperty(req.body.action)) {
					var resp = http_sys_anon_commands[req.body.action](req, function(err,res,vcredits) {
						if (err) {
							cb(err,null);
						} else {
							cb(null,res,vcredits);
						}
					});
				} else {*/
					cb("invalid database level call",null);
				//}
			}
		}
	}	

	var process_app_level_request = function(req,cb) {
		var appiterator = function(app, callback) {
			if (req.body.appcalls.indexOf(app.name)>-1) {
				app.process(req, thatdbsrv, function(err,appresp) {
					callback(err, appresp, 1);		//instead of 1 use app.credits
				})
			}
		}
		var resparr = [];
		async.map(thatdbsrv.apps, appiterator, function(result) {
			cb(null, result);
		});
	}	
	
	var getresponse = function(req, err, data) {
		var resp = {
			"data":null,
			"error":null,
			"request"	: req,
			"sysinfo"	: {
				"spid"		: process.pid,
				"server"	: thatdbsrv.info.name
			}
		}		
		if (err) {
			resp.error = err;
		} else {
			if (data) {
				if (typeof data == "string") {
					resp.data = data;
				} else {
					if (util.isDate(data)) {
						resp.data = data;
					} else {
						hlp.ogextend(resp,data);
					}
				}
			} else {
				resp.error = "no data";
			}
		}
		resp.sysinfo.responsedate = new Date();
		resp.sysinfo.responsetime = process.hrtime(req.hrtime);
		return resp;
	}
	
	this.processrequest = function(req, cb) {
		validate_request(req, function(err,succ) {
			if (err) {
				cb(getresponse(req, err, null),null);
			} else {
				if (req.type=="sys") process_sys_level_request(req, function(err2,res2) {
						if (err2) {
							cb(getresponse(req, err2, null),null);
						} else {
							cb(null, getresponse(req, null, res2));							
						}
					})
				else if (req.type=="db") process_db_level_request(req, function(err3,res3) {
						if (err3) {
							cb(getresponse(req, err3, null),null);
						} else {
							cb(null, getresponse(req, null, res3));							
						}
					})
				else if (req.type=="app") process_app_level_request(req, function(err4,res4) {
						if (err4) {
							cb(getresponse(req, err4, null),null);
						} else {
							cb(null, getresponse(req, null, res4));	
						}
					})
				else if (1==1) {
					cb(getresponse(req, "request cannot be processed", null),null);
				}
			}
		})
	}
	
}

module.exports = dbserver;








