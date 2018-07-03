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

var res = {};

res.reset = function(){
	this.task_data = false;
	this.account_data = false;
	this.login_data = false;
}

res.reset();

res.set_account_data = function(data){
	this.account_data = data;
}

res.set_task_data = function(data){
	this.task_data = data;
}

res.set_login_data = function(data){
	this.login_data = data;
}

res.process_data_if_get_all = function(){
	if(this.account_data !== false && this.task_data !== false && this.login_data !== false){
		console.log(11111111);
		


		this.reset();
	}
}

function logic(){
	var con_glog = mysql.createConnection(glog_dbconfig);
	var start_of_today = moment().startOf('day');
	var timestamp_today_begin = start_of_today.unix();
	var day_seconds = 24 * 60 * 60;
	var timestamp_yesterday_begin = timestamp_today_begin - day_seconds;
	var timestamp_tdby_begin = timestamp_yesterday_begin - day_seconds;
	con_glog.query('SELECT * FROM account_log WHERE UNIX_TIMESTAMP(create_date)=? AND (source_type=1562 OR source_type=1560);', [timestamp_tdby_begin],function(error, results, fields){
		//console.log(results[0]);
		res.set_account_data(results);
		res.process_data_if_get_all();
	});

	var con_log = mysql.createConnection(log_dbconfig);
	con_log.query('SELECT * FROM task_log WHERE task_type=1 AND op_type=702 AND UNIX_TIMESTAMP(create_time)>=? AND UNIX_TIMESTAMP(create_time)<?;', [timestamp_tdby_begin, timestamp_yesterday_begin], function(error, results, fields){
		//console.log(results[1]);
		res.set_task_data(results);
		res.process_data_if_get_all();
	});

	con_log.query('SELECT * FROM login_log WHERE UNIX_TIMESTAMP(create_time)>=? AND UNIX_TIMESTAMP(create_time)<?;', [timestamp_yesterday_begin, timestamp_today_begin], function(error, results, fields){

	});
}

logic();

schedule.scheduleJob('0 0 4 * * *', function(){
	logic();
});