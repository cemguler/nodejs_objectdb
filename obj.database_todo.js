/*
codename = INFINITEDATA / DBSERVER
phase 1 : single server, dedicated db, app modules, authentication, static and dynamic tokens, http, 
		  REST API (admin & test), nodejs app module, .net dbclient, javascript dbclient, javascript remote dbclient,
		  accounting/logging etc.
phase 2 : tcp, clustering, shared db
phase 3 : ftp port, multi server, scaling
*/


//phase 1:
var apppath = "app"; 
/*
 if shared then something like "c:\\ordergrinder\\app"; should be defined during startup
 app support lets dbserver to act as an application server.
 apps are nodejs modules. apps can be global or private to a single db, specified during setup. global_app_path or private_app_path
 
 Development:
 there should be ready to use app templates and apps should be installable, installation should use an install settings file.
 for example Packing app:
 - og.pack.js
 - og.pack.settings
	- app specific settings 
	- defaults
 - og.pack.setup
	- version info
	- Data definitions
		- structs for tables product and container
	- Miscellenous server level settings 
		- like coverage (global, server level, db level etc.)
		- like dbsettings
		- credits per transaction / accounting settings
Each app runs in its own virtual directory/scope and writing or accessing files located anywhere else is not possible.
- developers can only access the sample db that will be created for their use.
- upload files to devserver and setup app using the developer portal
- edit files with in place web based code editor
- download documents
- download app template (zip file which includes sample app files)
- Q&A
 */

//phase 1:
var http = "192.168.2.9:8100"	//http ip and port for accepting requests

//phase 3:
var ftp = "192.168.2.9:8100"	//ftp ip and port for accepting uploads or sending files

//phase 2:
var tcp = "192.168.2.9:9100"	//ip and port for sending peer to peer private messages or accepting tcp requests like http
var udp = "192.168.2.9:9100"	//ip and port for broadcasting private sync messages, requires a seperate net interface, used in scaling and clustering
/*
 dbserver has its own http port to accept requests and send back results in JSON format
 dbserver accepts JSON requests posted to its port, so in simplest form curl command can be used to access dbserver
 dbserver has its own query engine which parses,compiles,executes and caches requests
 each connection to service is made by using userid,password and requesttoken 

 Basic idea behind dbserver is to have in memory database with serialization option.
   
 Phase 1 : Data File structure
 OK - Databases are folders
 OK - Tables are simple text files
 OK - Each record has a specific width, so if you wish to access 100th record in that file, assuming a 10Byte record size, then you simply read data between 1000 and 1009 bytes
 - inmem_reccnt and indisk_reccnt is always maintained to see if there is any more data located in disk

 Phase 2 : MaxMem Usage and Data Scope definition:	
	Assume you have a 4Gb server and a 40Gb data. Since 40Gb cannot be loaded at once into memory, 
	then only a 4Gb part of it should be loaded and rest should be accessed by disk IO. But which 4Gb?
	- nth 4Gb from start,
	- or all possible or specifically selected databases/accounts that fits into 4Gb
	- or specifically selected database and filtered tables that fits into 4Gb,
	
	for example:
	- auto mode : if server is using shared data files, then in auto mode each server puts a lock on db during startup, this prevents other servers loading the same data.  cluster servers also wait but they load the same data after the mirror server has completed its startup. 
		datascope = null
	- first 4Gb block from start 
		datascope = { start:0, size:4096, filter:[] }
	- the block between 4Gb and 8Gb
		datascope = { start:4096, size:4096, filter:[] }
	- something like facebook users table with userids between 1 and 1.000.000
		datascope = { start:null, size:4096, filter:[{ "facebook":[ "users","userid>=1 && userid<=1000000" ] }] }
	- or XYZ db, all records in all tables with last access date in past 4 weeks
		datascope = { start:null, size:4096, filter:[{ "facebook":[ "*","lastaccess>=new Date(+new Date - 1000 * 60 * 60 * 24 * 14)" ] }] }
	- or all db, all records in all tables with last access date in past 4 weeks
		datascope = { start:null, size:4096, filter:[{ "*":[ "*","lastaccess>=new Date(+new Date - 1000 * 60 * 60 * 24 * 14)" ] }] }

 Phase 1 : Select behaviour:
	- OK - use only memory data
	- Phase 2 : use memory and disk at the same time : to prevent large tables from filling memory do partial query execution by fetching data block by block and fetch until all are completed
	
 Phase 1 : Save behaviour:
	OK - All data is never saved as a whole file, 
	OK - dbserver only saves inmemory records using __id field and reclen to write data to appropriate positions in table file.	if __id field is supplied wrong then a wrong record may get updated.	
		
 Phase 2 : "Not Enough Memory" problem is handled by Insert and AutoExpire behaviours : 
 Phase 2 : Insert behaviour:
	- least (default) : Remove least accessed in same table and insert new record
	- oldest		  : Remove oldest index in same table and insert new record
	- leastdb		  : Remove least accessed in same db and insert new record (more processing time)
	- oldestdb		  : Remove oldest index in same db and insert new record (more processing time)
 	
 Phase 1 : AutoExpire behaviour:
	- Scans records in memory at specific intervals (default is autoexpire_scaninterval = 10seconds)
	- Checks last access date of records and removes records matching the criteria from memory (default is autoexpire_expiryperiod = null, specifying records never expire)
 
 
 
 
 
 Phase 2 : Clustering : 
 1000 PC's can either hold same data and all can become active at the same time in shared mode
 - a clusterid simply points to a clouid and manages the same datasets

 
 
 Phase 3 : Ability to scale : 
 dbserver should allow use of 1000 PC's working in parallel in shared mode
 this means 1000 PC's will write to same file, so concurrent writes should be supported.  
 if the data is too large, data can be distributed evenly amongst these servers for example 2Gb for each server 
 allowing us to scale up to a networked 2Tb in memory db acting as a single db.
 the question is how:
 - each server has a clusterid and a cloudid
 - a cloudid is a unique identifier for each server, along with definition of how it manages datasets

 in scaled mode : 
	- no seek is done instead a udp broadcast is performed asking other servers to send the related data in their memory (is it really faster than disk IO?)
	- colloborative/parallel query:
	  - assume you execute "select sum(sales) from customer" on a 100 million record table whose data is distributed to 1000 servers
	    then each server can make a calculation with they have in memory, and send the result to requesting server which then sums all results to a single result
	  - assume you execute "select * from customer" on a 100 million record table whose data is distributed to 1000 servers
	    then initial server simply sends back the first n rows back using the paging mechanism, default page size is 500 records.
	  - assume you execute "select * from customer c join user u where c.userid=u.id" on two 100 million record tables whose data are distributed to 1000 servers
	    and the initial server has no user data stored
	    then initial server broadcasts a message to other servers, each server sends back the first n rows back using the paging mechanism, default page size is 500 records.
		each server executes the query and sends the query result back, but the final execution is performed by initial server
 
 
 */
