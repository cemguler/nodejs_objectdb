var 	crypto = require('crypto');

exports.oghelpersversion = oghelpersversion = "1.0.2";

exports.globaltokens = globaltokens = [ 
	'ordergrinder-system',
	'568059726477281-72ee8f37bed95dc2c89e17014305a8d2',
	'wrrPgAQiG6lgEtg8J37vfxjx6O+GHuXA6tfasiz/uMaChG42GU3YsD3efqKCFeDNuphGKvafrU968Iv0PHhcmA==' 
]

function flatten(arr) {
    return arr.reduce(function(acc, val) {
		return acc.concat(val.constructor === Array ? flatten(val) : val);
	},[]);
}
exports.newtoken = newtoken = function() {
	return process.hrtime().join("") + "-" + crypto.randomBytes(16).toString('hex');
}
exports.newaccounttoken = newaccounttoken = function() {
	var vtoken = newtoken();
	var acckey = globaltokens[2];
	var cipher = crypto.createCipher('aes-128-ecb', acckey);
	var crypted = cipher.update(vtoken, 'utf-8', 'base64');
	crypted += cipher.final('base64');
	cipher=null;
	return [acckey,vtoken,crypted];
}
exports.newservertoken = newservertoken = function(acckey) {
	var vtoken = newtoken();
	var cipher = crypto.createCipher('aes-128-ecb', acckey);
	var crypted = cipher.update(vtoken, 'utf-8', 'base64');
	crypted += cipher.final('base64');
	cipher=null;
	return [acckey,vtoken,crypted];
}
exports.newrequesttoken = newrequesttoken = function(srvkey) {
	var vtoken = newtoken();
	var cipher = crypto.createCipher('aes-128-ecb', srvkey);
	var crypted = cipher.update(vtoken, 'utf-8', 'base64');
	crypted += cipher.final('base64');
	cipher=null;
	return [srvkey,vtoken,crypted];
}

String.prototype.format = function() { 
    var formatted = this; 
    for (var i = 0; i < arguments.length; i++) { 
        var regexp = new RegExp('\\{'+i+'\\}', 'gi'); 
        formatted = formatted.replace(regexp, arguments[i]); 
    } 
    return formatted; 
}; 

String.prototype.trim = function() {
	return this.replace(/^\s+|\s+$/g, "");
}
String.prototype.trimLeft = function(chars) {
	chars = chars || "\\s";
	return this.replace(new RegExp("^[" + chars + "]+", "g"), "");
};
String.prototype.isNumeric=function() {
	var sText = this;
	var ValidChars = "0123456789.";
	var IsNumber=true;
	var Char;
	for (i = 0; i < sText.length && IsNumber == true; i++) { 
		Char = sText.charAt(i); 
		if (ValidChars.indexOf(Char) == -1) {
			IsNumber = false;
		}
	}
	return IsNumber;
}
String.prototype.isInteger=function() {
	var sText = this;
	var ValidChars = "0123456789";
	var IsNumber=true;
	var Char;
	for (i = 0; i < sText.length && IsNumber == true; i++) { 
		Char = sText.charAt(i); 
		if (ValidChars.indexOf(Char) == -1) {
			IsNumber = false;
		}
	}
	return IsNumber;
}
String.prototype.isIPv4=function() {
	var sText = this;
	var sarr = sText.split(".");
	if (sarr.length!=4) return false;
	for (var i=0; i<sarr.length; i++) {
		if (!sarr[i].isInteger()) return false;
		if (sarr[i]*1>255 || sarr[i]*1<0) return false;
	}
	return true;
}
String.prototype.repeat = function(num) { 
    return new Array(parseInt(num) + 1).join( this ); 
} 
String.prototype.addChars = function(vch, vlen, vrightalign) {
	if (vrightalign=="undefined") vrightalign=false;
	var i = vlen - this.length;
	var pre = "";
	if (i>0) vch.repeat(i);
	return ((vrightalign)?pre + this:this + pre);
}
Date.prototype.format = function(mask) {
	var d = this; // Needed for the replace() closure
	//d = d.replace("T"," ");
	// If preferred, zeroise() can be moved out of the format() method for performance and reuse purposes
	var zeroize = function (value, length) {
		if (!length) length = 2;
		value = String(value);
		for (var i = 0, zeros = ''; i < (length - value.length); i++) {
			zeros += '0';
		}
		return zeros + value;
	};
	return mask.replace(/"[^"]*"|'[^']*'|\b(?:d{1,4}|m{1,4}|yy(?:yy)?|([hHMs])\1?|TT|tt|[lL])\b/g, function($0) {
		switch($0) {
			case 'd':	return d.getDate();
			case 'dd':	return zeroize(d.getDate());
			case 'ddd':	return ['Sun','Mon','Tue','Wed','Thr','Fri','Sat'][d.getDay()];
			case 'dddd':	return ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'][d.getDay()];
			case 'm':	return d.getMonth() + 1;
			case 'mm':	return zeroize(d.getMonth() + 1);
			case 'mmm':	return ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][d.getMonth()];
			case 'mmmm':	return ['January','February','March','April','May','June','July','August','September','October','November','December'][d.getMonth()];
			case 'yy':	return String(d.getFullYear()).substr(2);
			case 'yyyy':	return d.getFullYear();
			case 'h':	return d.getHours() % 12 || 12;
			case 'hh':	return zeroize(d.getHours() % 12 || 12);
			case 'H':	return d.getHours();
			case 'HH':	return zeroize(d.getHours());
			case 'M':	return d.getMinutes();
			case 'MM':	return zeroize(d.getMinutes());
			case 's':	return d.getSeconds();
			case 'ss':	return zeroize(d.getSeconds());
			case 'l':	return zeroize(d.getMilliseconds(), 3);
			case 'L':	var m = d.getMilliseconds();
					if (m > 99) m = Math.round(m / 10);
					return zeroize(m);
			case 'tt':	return d.getHours() < 12 ? 'am' : 'pm';
			case 'TT':	return d.getHours() < 12 ? 'AM' : 'PM';
			// Return quoted strings with the surrounding quotes removed
			default:	return $0.substr(1, $0.length - 2);
		}
	});
};

Array.prototype.avg = function() {
	var av = 0;
	var cnt = 0;
	var len = this.length;
	for (var i = 0; i < len; i++) {
		var e = +this[i];
		if(!e && this[i] !== 0 && this[i] !== '0') e--;
		if (this[i] == e) {av += e; cnt++;}
	}
	return av/cnt;
}

Array.prototype.clone = function() {
	return JSON.parse(JSON.stringify(this));
}
exports.ObjectCloner = function(obj) {
	var str = JSON.stringify(obj);
	return [ JSON.parse(str), str.length ];
}
exports.cloner = cloner = function(obj) {
	var str = JSON.stringify(obj);
	return [ JSON.parse(str), str.length ];
}


//Object.prototype.clone = function() {
  /*var ret = {}; 
  Object.keys(this).forEach(function (val) { 
    ret[val] = this[val]; 
  }); 
  return ret; */
//	return JSON.parse(JSON.stringify(this));
//}


exports.getRandomInt = function(min, max) {
	return Math.floor(Math.random() * (max - min + 1)) + min;
}

/**
 * Adopted from jquery's extend method. Under the terms of MIT License.
 *
 * http://code.jquery.com/jquery-1.4.2.js
 *
 * Modified by mscdex to use Array.isArray instead of the custom isArray method
 */
exports.ogextend = function() {
  // copy reference to target object
  var target = arguments[0] || {}, i = 1, length = arguments.length, deep = false, options, name, src, copy;

  // Handle a deep copy situation
  if (typeof target === 'boolean') {
    deep = target;
    target = arguments[1] || {};
    // skip the boolean and the target
    i = 2;
  }

  // Handle case when target is a string or something (possible in deep copy)
  if (typeof target !== 'object' && !typeof target === 'function')
    target = {};

  var isPlainObject = function(obj) {
    // Must be an Object.
    // Because of IE, we also have to check the presence of the constructor property.
    // Make sure that DOM nodes and window objects don't pass through, as well
    if (!obj || toString.call(obj) !== '[object Object]' || obj.nodeType || obj.setInterval)
      return false;
    
    var has_own_constructor = hasOwnProperty.call(obj, 'constructor');
    var has_is_property_of_method = hasOwnProperty.call(obj.constructor.prototype, 'isPrototypeOf');
    // Not own constructor property must be Object
    if (obj.constructor && !has_own_constructor && !has_is_property_of_method)
      return false;
    
    // Own properties are enumerated firstly, so to speed up,
    // if last one is own, then all properties are own.

    var last_key;
    for (key in obj)
      last_key = key;
    
    return typeof last_key === 'undefined' || hasOwnProperty.call(obj, last_key);
  };


  for (; i < length; i++) {
    // Only deal with non-null/undefined values
    if ((options = arguments[i]) !== null) {
      // Extend the base object
      for (name in options) {
        src = target[name];
        copy = options[name];

        // Prevent never-ending loop
        if (target === copy)
            continue;

        // Recurse if we're merging object literal values or arrays
        if (deep && copy && (isPlainObject(copy) || Array.isArray(copy))) {
          var clone = src && (isPlainObject(src) || Array.isArray(src)) ? src : Array.isArray(copy) ? [] : {};

          // Never move original objects, clone them
          target[name] = extend(deep, clone, copy);

        // Don't bring in undefined values
        } else if (typeof copy !== 'undefined')
          target[name] = copy;
      }
    }
  }

  // Return the modified object
  return target;
};


exports.getprocesstime = getprocesstime = function(vref) {
	var intm = process.hrtime(vref);
	var secs = intm[0];
	var mss = Math.floor(intm[1]/1000000);
	var nano = intm[1]-mss*1000000;
	return [secs,mss,nano];
}

exports.getserverfromargs = getserverfromargs = function() {
	var res = [null,null,null,null];
	var netres   = {};	//{ "type":"", "ip":"", "port":0 };
	var httpres  = {};	//{ "ip":"", "port":0, "modules":[] }
	var dbpath = "c:\\ordergrinder\\db";
	
	var args = process.argv;
	if (args[0].toLowerCase()=="node") {
		args.splice(0,2);	// NODE UDPSERVER 
	} else {
		args.splice(0,1);	// UDPSERVER
	}
	if (args.length==0) {
		console.log("server parameters not supplied");
		return null;
	}
	var err = "";
	for (var x=0; x<args.length; x++) {
		if (args[x].toLowerCase()=="-net") {
			if (x+1<args.length) {
				var netarr = args[x+1].split(":");
				//console.log(netarr);
				//if (netarr.length!=3) {
				//	err = "error: invalid private network binding, server type, ip and port are required";
				if (netarr.length!=2) {
					err = "error: invalid private network binding, ip and port are required";
				} else {
					//if (netarr[0].toLowerCase()=="core" || netarr[0].toLowerCase()=="app") {
						//netres["type"]=netarr[0].toLowerCase();
						if (netarr[0].isIPv4()) {
							netres["ip"] = netarr[0];
							if (netarr[1].isInteger() && netarr[1].length==4) {
								if (parseInt(netarr[1])>=9100 && parseInt(netarr[1])<9200) {
									if (netarr[1].charAt(netarr[1].length-1)=="0") {
										netres["port"] = parseInt(netarr[1]);
									} else {
										err = "error: incorrect port number specified for private network binding : " + netarr[1] + "\r\nport number should be between 9100 and 9190 and increments of 10";
									}
								} else {
									err = "error: incorrect port number specified for private network binding : " + netarr[1] + "\r\nport number should be between 9100 and 9190";
								}
							} else {
								err = "error: incorrect port number specified for private network binding : " + netarr[1];
							}
						} else {
							err = "error: incorrect ip address specified for private network binding : " + netarr[0];
						}
					//} else {
					//	err = "error: incorrect private network type : " + netarr[0];
					//}
				}
			} else {
				err = "error: missing private network binding";
			}
		}
		if (err!="") {
			console.log(err);
			return null;
		}
		if (args[x].toLowerCase()=="-dbpath") {
			if (x+1<args.length) {
				dbpath=args[x+1];
			}
		}
		if (args[x].toLowerCase()=="-http") {
			if (x+1<args.length) {
				var netarr = args[x+1].split(":");
				if (netarr.length!=2) {
					err = "error: invalid public network http binding, both ip and port are required";
				} else {
					if (netarr[0].isIPv4()) {
						httpres["ip"] = netarr[0];
						if (netarr[1].isInteger()) {
							httpres["port"] = parseInt(netarr[1]);
						} else {
							err = "error: incorrect port number specified for public network http binding : " + netarr[1];
						}
					} else {
						err = "error: incorrect ip address specified for public network http binding : " + netarr[0];
					}
				}
			} else {
				err = "error: missing public http network binding";
			}
		}
		if (err!="") {
			console.log(err);
			return null;
		}
		if (args[x].toLowerCase()=="-modules") {
			if (x+1<args.length) {
				var modarr = args[x+1].toLowerCase().split(",");
				if (modarr.length>0) {					
					var httpmods = [];
					for (var y=0; y<modarr.length; y++) {
						if (httpmodules.hasOwnProperty(modarr[y])) {
							httpmods.push(modarr[y]);
						} else {
							err += "error: incorrect module name : " + modarr[y] + "\r\n";
						}
					}
					if (httpmods.length==0) {
						err += "error: missing module list\r\n";
					} else {
						//httpres["modules"] = httpmods.clone();
						httpres["modules"] = cloner(httpmods)[0];	
						httpmods = null;
					}
				} else {
					err = "error: missing module list";
				}
			} else {
				err = "error: missing http module list";
			}
		}
		if (err!="") {
			console.log(err);
			return null;
		}
	}
	var _http = true;
	var _net = true;
	if (!httpres.hasOwnProperty("ip") || !httpres.hasOwnProperty("port")) _http=false;
	//if (!netres.hasOwnProperty("type") || !netres.hasOwnProperty("ip") || !netres.hasOwnProperty("port")) _net=false;
	if (!netres.hasOwnProperty("ip") || !netres.hasOwnProperty("port")) _net=false;
	if (!_net) {
		err = "error: at least one private network binding is required to startup the service";
	} else {
		if (httpres.hasOwnProperty("modules") && !_http) {
			err = "A module list is provided but no http binding is defined for public network";
		} else {
			if (!httpres.hasOwnProperty("modules") && _http) {
				err = "No module to publish over public network http binding";
			}
		}
	}
	if (err!="") {
		console.log(err);
		return null;
	}
	return [ netres, httpres, dbpath];
}


var cute_codes = {  
	"off": 0,
	"bold": 1,
	"italic": 3,
	"underline": 4,
	"blink": 5,
	"inverse": 7,	
	"hidden": 8,
	"black": 30,
	"red": 31,
	"green": 32,
	"yellow": 33,
	"blue": 34,
	"magenta": 35,
	"cyan": 36,
	"white": 37,
	"black_bg": 40,
	"red_bg": 41,
	"green_bg": 42,
	"yellow_bg": 43,
	"blue_bg": 44,
	"magenta_bg": 45,
	"cyan_bg": 46,
	"white_bg": 47
};
/*
for (var x=0; x<10000; x++) {
	var len = ("" + x).length;
	process.stdout.write("\033[" + len + "D" + x);		//move cursor left
	//process.stdout.write("\033[" + len + "C" + x);	//move cursor right
	//process.stdout.write("\033[" + len + "A" + x);	//move cursor up
	//process.stdout.write("\033[" + len + "B" + x);	//move cursor down
}
console.log("");
*/
exports.cute = function(tiparr) {
	var cutestring = "";
	for (var x=0; x<tiparr.length; x++) {
		if (cute_codes.hasOwnProperty(tiparr[x])) {
			cutestring += "\033[" + cute_codes[tiparr[x]] + "m";
		}
	}
	return cutestring;
}

exports.boxer = function(bwidth, blines, vprint) {
	vprint = vprint || false;
	var fline = "┌" + "─".repeat(bwidth-2) + "┐\r\n";
	var bline = "│" + " ".repeat(bwidth-2) + "│\r\n";
	var lline = "└" + "─".repeat(bwidth-2) + "┘\r\n";
	var mlines = "";
	for (var x=0; x<blines.length; x++) {
		var bplain = blines[x].replace(/\033[[0-9;]*m/g,"");
		var diff = blines[x].length - bplain.length;
		mlines += "│  " + (blines[x] + " ".repeat(bwidth-6)).substring(0, bwidth-6 + diff) + "  │\r\n";
	}
	var box = fline + bline + mlines + bline + lline;
	if (vprint) console.log(box);
	return box;
}
exports.cls = function() {
	console.log('\u001B[2J\u001B[0;0f') 
}





