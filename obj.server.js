var	cluster = require("cluster"),
	http = require("http"),
	url = require("url"),
	dbserver = require("./obj.database.js");
	
var numReqs = 0;
var numcpus = 8;

var dbpath = "db";	

if (cluster.isMaster) {
	var ogserver = new dbserver();
	ogserver.load(function() {
		ogserver.initconsole();
	});
	for (var i = 0; i < numcpus; i++) cluster.fork(); 
	Object.keys(cluster.workers).forEach(function(id) {
		cluster.workers[id].on('message', function(msg) {
			ogserver.processrequest(msg, function(err, btnresp) {
				if (err) {
					cluster.workers[id].send(err);  
				} else {					
					cluster.workers[id].send(btnresp);  
				}
			});
		});
	});
} else if (cluster.isWorker) {
	var response_queue = {};
	var responseindex = -1;
	process.on('message', function(msg) {
		if (msg.request.rindex) {
			var vrindex = msg.request.rindex;
			delete msg.request;
			response_queue[vrindex].setHeader("Access-Control-Allow-Origin", "*");
			//response.setHeader("Access-Control-Allow-Headers", "X-Requested-With"); 
			response_queue[vrindex].setHeader("access-control-allow-methods", "GET, POST, PUT, DELETE, OPTIONS");
			response_queue[vrindex].setHeader("access-control-allow-headers", "content-type, accept, referer");
			response_queue[vrindex].end(JSON.stringify(msg));
			delete response_queue[vrindex];
		}
	});	
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
	var crequest = function(request, vbody) {
		var urlobject=url.parse(request.url.toLowerCase(),true, true);		
		var urlobject2=url.parse(request.url,true, true);		
		var paths = urlobject.pathname.slice(1).split("/");
		var req = {
					accountname	: paths[0],
					from 		: ((request.headers["origin"])?request.headers["origin"]:request.headers["host"]),
					to 			: null,
					ip 			: request.headers["x-real-ip"] || request.connection.remoteAddress || request.headers["x-forwarded-for"],
					cross 		: ((request.headers["origin"])?true:false),
					url			: request.url,
					hash		: urlobject.hash,					
					querystring	: unescape(urlobject2.search.substring(1,urlobject2.search.length)),
					type		: null,
					action		: null,
					objectname	: null,
					objectindex : null,
					body		: null,
					requestdate : new Date(),
					hrtime 		: process.hrtime(),
					pid			: process.pid,
					rindex		: null
				}
		if (vbody) {
			vbody = vbody.toString();
			try {
				req.body = JSON.parse(vbody);
			} catch(errp) {
				console.log(vbody)
				console.log(errp);
				return "invalid request body";
			}
			if (req.body) {
				if (req.body.hasOwnProperty("appcalls")) {
					req.appcalls = req.body["appcalls"];
				}
			}
		} else {
			return "request message not found";
		}
		if (req.querystring) req.querystring_filter = parsequerystring(req.querystring);
		if (paths.length>1) {
			req.type = paths[1];		
			if (paths.length>2) {
				req.objectname = paths[2];	
				if (paths.length>3) {
					if (paths[3].isNumeric()) {
						req.objectindex = paths[3];
					}
				}
			}
		}
		
		//do some basic validation
		var valerr = "";
		if (req.accountname=="") valerr = "account not specified"
		else if (req.type!=undefined && req.type!=null && req.type!="" && req.type!="db" && req.type!="sys" && req.type!="app") valerr = "unsupported request type"
		else if (!req.body) valerr = "request message not found";
		if (valerr!="") return valerr;
		if (req.body.action) req.action = req.body.action;
		if (req.body.objectname) req.objectname = req.body.objectname;
		if (req.body.objectindex) req.objectindex = req.body.objectindex;
		return req;		
	}
	var dbrequest = function(request, response) {
		var vbody = "";
		request.on("data",function(data) {
			vbody+=data;
			if (vbody.length>250000) {
				response.setHeader("Access-Control-Allow-Origin", "*");
				//response.setHeader("Access-Control-Allow-Headers", "X-Requested-With"); 
				response.setHeader("access-control-allow-methods", "GET, POST, PUT, DELETE, OPTIONS");
				response.setHeader("access-control-allow-headers", "content-type, accept, referer");
				response.end('{ "error":"Data too long (' + vbody.length + ')" } ');
				request.connection.destroy();
				//return that.raiseexception("DATA too long");
			}
		});
		request.on("end", function() {
				if (request.method.toUpperCase()=="OPTIONS") {
					var origin = (request.headers.origin || "*");
					response.writeHead("204", "No Content", {
						"access-control-allow-origin": origin,
						"access-control-allow-methods": "GET, POST, PUT, DELETE, OPTIONS",
						"access-control-allow-headers": "content-type, accept, referer",
						"access-control-max-age": 10, // Seconds.
						"content-length": 0
					});
					return response.end();
				} 
				var basicrequest = crequest(request, vbody);
				if (typeof basicrequest == "string") {
					response.setHeader("Access-Control-Allow-Origin", "*");
					//response.setHeader("Access-Control-Allow-Headers", "X-Requested-With"); 
					response.setHeader("access-control-allow-methods", "GET, POST, PUT, DELETE, OPTIONS");
					response.setHeader("access-control-allow-headers", "content-type, accept, referer");
					return response.end('{ "error":"' + basicrequest + '" } ');
				} else {
					responseindex++;
					response_queue["resp_" + responseindex] = response;
					basicrequest.rindex = "resp_" + responseindex;
					process.send(basicrequest);
				}
		});
		request.on("error", function(err) {
			console.log(err);
			request.connection.destroy();
		});		
	}
		
	var shttp = http.createServer();
	shttp.on("request", dbrequest);
	shttp.on("close",function() {
		console.log("http listener is down");
	})
	shttp.listen(8100, "192.168.2.9", 511, function() {

	});
}

