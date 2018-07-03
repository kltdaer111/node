var mysql = require('mysql');
var schedule = require('node-schedule');
var moment = require('moment');

var glog_dbconfig = {
	host : '139.196.41.108',
	user : 'wsy',
	password : 'Wsy1985!',
	database : 'shengda_20180615_sg_glog',
};

var log_dbconfig = {
	host : '127.0.0.1',
	user : 'root',
	password : 'Sanguo1!',
	database : 'wangmeijie_trunk_sg_log',
};

var res = {
	'glog' : false,
	'log' : false,
};

res.reset = function(){
	this.glog = false;
	this.log = false;
}

res.set_glog_data = function(data){
	this.glog = data;
}

res.set_log_data = function(data){
	this.log = data;
}

res.process_data_if_get_all = function(){
	if(this.glog !== false && this.log !== false){
		console.log(11111111);
		var start_of_today = moment().startOf('day');
		console.log(start_of_today.unix(), start_of_today.subtract(1, 'day').unix());


		this.reset();
	}
}

function logic(){
	var con_glog = mysql.createConnection(glog_dbconfig);
	var now = new Date();
	con_glog.query('SELECT * FROM account_log', function(error, results, fields){
		//console.log(results[0]);
		res.set_glog_data(results);
		res.process_data_if_get_all();
	});

	var con_log = mysql.createConnection(log_dbconfig);
	con_log.query('SELECT * FROM task_log', function(error, results, fields){
		//console.log(results[1]);
		res.set_log_data(results);
		res.process_data_if_get_all();
	});
}

logic();

schedule.scheduleJob('0 0 4 * * *', function(){
	logic();
});