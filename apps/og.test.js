//var events = require("events");

function ogtest() { 
	this.name="test";
}

ogtest.prototype.process = function(req, dbsrv, oncomplete) {
	console.log(req);
	var resp = {
		"appname":"test",
		"result":"test ok = " + process.hrtime(),
		"error":""
	}

	if (typeof oncomplete == "function") {
		return oncomplete(resp);
	} else {
		return resp;
	}
}

module.exports = new ogtest();