var mysql = require('mysql');
var schedule = require('node-schedule');
var moment = require('moment');

var glog_dbconfig = {
	host : '139.196.41.108',
	user : 'wsy',
	password : 'Wsy1985!',
	database : 'wai_trunk_sg_glog',
};

var log_dbconfig = {
	host : '192.168.1.5',
	user : 'root',
	password : 'Sanguo1!',
	database : 'nei_trunk_sg_log',
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

var stepIO = [];
var filter_data = [];

var con_glog = mysql.createConnection(glog_dbconfig);
var con_log = mysql.createConnection(log_dbconfig);
var start_of_today = moment().startOf('day');
var timestamp_today_begin = start_of_today.unix();
var day_seconds = 24 * 60 * 60;
var timestamp_yesterday_begin = timestamp_today_begin - day_seconds;
var timestamp_tdby_begin = timestamp_yesterday_begin - day_seconds;
function gen_filter_data(){
	//注册
	filter_data.push({col : 'source_type', value : 1560});
	stepIO.push({input : 'mobile_uuid', output : 'account_uid', src_log : 'account_data'});
	//创角
	filter_data.push({col : 'source_type', value : 1562});
	stepIO.push({input : 'account_uid', output : 'role_uid', src_log : 'account_data'});
	//任务
	//获取主线任务id序列 --保险起见应改为配置表读取
	con_log.query('SELECT task_id FROM `task_log` AS t5 RIGHT JOIN (SELECT COUNT(*) AS cola,t4.role_uid FROM (SELECT id,t3.role_uid,task_id FROM `task_log` AS t3 LEFT OUTER JOIN (SELECT role_uid FROM (SELECT role_uid,MIN(UNIX_TIMESTAMP(create_time)) as firsttime FROM `task_log` GROUP BY role_uid) as t1 WHERE t1.firsttime>=? AND t1.firsttime<?) AS t2 ON t2.role_uid=t3.role_uid WHERE task_type=1 AND op_type=702) AS t4 GROUP BY t4.role_uid ORDER BY cola DESC LIMIT 1) AS t6 ON t5.role_uid=t6.role_uid WHERE task_type=1 AND op_type=702 ORDER BY id;', [timestamp_tdby_begin, timestamp_yesterday_begin], function(error, results, fields){
		//console.log(results);
		for(idx in results){
			var task_id = results[idx]['task_id'];
			if(task_id == 0){
				continue;
			}
			filter_data.push({col : 'task_id', value : task_id});
			stepIO.push({input : 'role_uid', output : 'role_uid', src_log : 'task_data'});
		}
	});

	logic();
}
gen_filter_data();

function filter(mysql_raw_data, filter_col, value){
	var res = [];
	for(idx in mysql_raw_data){
		var piece = mysql_raw_data[idx];
		if(piece[filter_col] == value){
			res.push(piece);
		}
	}
	return res;
}

function gen_remaining(input, input_data, output, data){
	var res = {};
	for(idx in data){
		var every = data[idx];
		if(input_data === null){
			res[every[output]] = 0;
		}
		else{
			if(every[input] in input_data){
				res[every[output]] = 0;
			}
		}
	}
	return res;
}

function get_obj_length(obj){
	var i = 0;
	for(idx in obj){
		i += 1;
	}
	return i;
}

var results = [];
res.process_data_if_get_all = function(){
	if(this.account_data !== false && this.task_data !== false && this.login_data !== false){
		console.log(11111111);
		var IO = null;
		var data = null;
		for(idx in stepIO){
			IO = stepIO[idx];
			data = gen_remaining(IO.input, data, IO.output, filter(res[IO.src_log], filter_data[idx]['col'], filter_data[idx]['value']));
			results.push(data);
		}
		console.log(results);
		this.reset();
	}
}

function logic(){
	//取出指定日期的数据
	con_glog.query('SELECT * FROM account_log WHERE UNIX_TIMESTAMP(create_date)=? AND (source_type=1562 OR source_type=1560);', [timestamp_tdby_begin],function(error, results, fields){
		//console.log(results[0]);
		res.set_account_data(results);
		res.process_data_if_get_all();
	});
	con_log.query('SELECT * FROM task_log WHERE task_type=1 AND op_type=702 AND UNIX_TIMESTAMP(create_time)>=? AND UNIX_TIMESTAMP(create_time)<?;', [timestamp_tdby_begin, timestamp_yesterday_begin], function(error, results, fields){
		//console.log(results[1]);
		res.set_task_data(results);
		res.process_data_if_get_all();
	});
	con_log.query('SELECT * FROM login_log WHERE UNIX_TIMESTAMP(create_time)>=? AND UNIX_TIMESTAMP(create_time)<?;', [timestamp_yesterday_begin, timestamp_today_begin], function(error, results, fields){
		res.set_login_data(results);
		res.process_data_if_get_all();
	});
}

schedule.scheduleJob('0 0 4 * * *', function(){
	logic();
});