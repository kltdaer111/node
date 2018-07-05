const mysql = require('mysql');
const schedule = require('node-schedule');
const moment = require('moment');
const layer_data_processor = require('./layer_data_processor');
const LayerDataProcessor = layer_data_processor.LayerDataProcessor;
const util = require('util');

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

var con_glog = mysql.createConnection(glog_dbconfig);
var con_log = mysql.createConnection(log_dbconfig);
var start_of_today = moment().startOf('day');
var timestamp_today_begin = start_of_today.unix();
var day_seconds = 24 * 60 * 60;
var timestamp_yesterday_begin = timestamp_today_begin - day_seconds;
var timestamp_tdby_begin = timestamp_yesterday_begin - day_seconds;

function compare_equal(val1, val2){
	return val1 == val2;
}

function compare_lessequal(val1, val2){
	return val1 <= val2;
}

function compare_greater(val1, val2){
	return val1 > val2;
}

function do_with_results(results, need_cols){
	var res = [];
	for(idx in need_cols){
		res.push(get_col_data(results, need_cols[idx]));
	}
	return res;
}

function Percentage(number1, number2) { 
    return (Math.round(number1 / number2 * 10000) / 100.00 + "%");// 小数点后两位百分比
}

function fillSqlIn(sql, symbol, array){
	var tmp = '';
	for(idx in array){
		tmp += "'" + idx + "',";
	}
	tmp = tmp.slice(0, -1);
	return sql.replace(symbol, tmp);
}

function Count(obj){
	var i = 0;
	for(idx in obj){
		i += 1;
	}
	return i;
}


function gen_filter_data(prcssr, type, callback){
	//注册
	prcssr.pushFilterToChain('account', 'source_type', 1560, compare_equal, 'mobile_uuid', 'account_uid');
	//创角
	prcssr.pushFilterToChain('account', 'source_type', 1562, compare_equal, 'account_uid', 'role_uid');
	
	switch(type){
		//任务
		//获取主线任务id序列 --保险起见应改为配置表读取
		case 'task':{
			con_log.query('SELECT task_id FROM `task_log` AS t5 RIGHT JOIN (SELECT COUNT(*) AS cola,t4.role_uid FROM (SELECT id,t3.role_uid,task_id FROM `task_log` AS t3 LEFT OUTER JOIN (SELECT role_uid FROM (SELECT role_uid,MIN(UNIX_TIMESTAMP(create_time)) as firsttime FROM `task_log` GROUP BY role_uid) as t1 WHERE t1.firsttime>=? AND t1.firsttime<?) AS t2 ON t2.role_uid=t3.role_uid WHERE task_type=1 AND op_type=702) AS t4 GROUP BY t4.role_uid ORDER BY cola DESC LIMIT 1) AS t6 ON t5.role_uid=t6.role_uid WHERE task_type=1 AND op_type=702 ORDER BY id;', [timestamp_tdby_begin, timestamp_yesterday_begin], function(error, results, fields){
				//console.log(results);
				for(idx in results){
					var task_id = results[idx]['task_id'];
					if(task_id == 0){
						continue;
					}
					prcssr.pushFilterToChain('task', 'task_id', task_id, compare_equal, 'role_uid', 'role_uid');
				}
				callback(prcssr, type);
			});
		}
		break;
		case 'duration':{
			var duration_cfg = [3 * 60, 10 * 60, 30 * 60, 60 * 60];
			for(idx in duration_cfg){
				var value = duration_cfg[idx];
				prcssr.pushFilterToChain('duration', 'duration', value, compare_lessequal, 'role_uid', 'role_uid');
			}
			prcssr.pushFilterToChain('duration', 'duration', 60 * 60, compare_greater, 'role_uid', 'role_uid');
			callback(prcssr, type);
		}
		break;
		case 'level':{
			for(var i = 2; i <= 100; i++){
				prcssr.pushFilterToChain('level', 'new_level', i, compare_equal, 'role_uid', 'role_uid');
			}
			callback(prcssr, type);
		}
		break;
	}
}

var task_table = ['达成人数', '相对达成率', '绝对达成率', '离开人数', '次日留存', '留存率'];

function get_col_data(compelete_results, type){
	var data = [];
	switch(type){
		case '达成人数':
		{
			for(idx in compelete_results){
				data.push(Count(compelete_results[idx][1]));
			}
		}
		break;
		case '相对达成率':
		{
			var denominator = Count(compelete_results[0][1]);
			for(idx in compelete_results){
				if(denominator === 0){
					data.push(0);
				}
				else{
					var val = Count(compelete_results[idx][1]);
					data.push(Percentage(val, denominator));
					denominator = val;
				}
			}
		}
		break;
		case '绝对达成率':
		{
			var denominator = Count(compelete_results[0][1]);
			for(idx in compelete_results){
				if(denominator === 0){
					data.push(0);
				}
				else{
					var val = Count(compelete_results[idx][1]) / denominator;
					data.push(Percentage(val, denominator));
				}
			}
		}
		break;
		case '离开人数':
		{
			var last_num = Count(compelete_results[0][1]);
			for(idx in compelete_results){
				var val = Count(compelete_results[idx][1]);
				data.push(last_num - val);
				last_num = val;
			}
		}
		break;
		case '次日留存':
		{
			for(idx in compelete_results){
				var val = Count(compelete_results[idx][2]);
				data.push(val);
			}
		}
		break;
		case '留存率':
		{
			for(idx in compelete_results){
				var buf = compelete_results[idx];
				var val1 = Count(buf[1]);
				var val2 = Count(buf[2]);
				data.push(Percentage(val2, val1));
			}
		}
		break;
	}
	return data;
}

var sum_for_check = 0;

function do_with_SecondDayRemaining_result(err, idx, src_result, result){
	if(err){
		console.log(err);
		return;
	}
	src_result[idx].push(result);
	//console.log(result);
	//console.log(sum_for_check);
	sum_for_check -= 1;
	var res = [];
	if(sum_for_check === 0){
		console.log(src_result);
		res = do_with_results(src_result, task_table);
		console.log(res);
	}
	return res;
}

function fill_result_if_ready(processor){
	if(processor.checkMysqlDataReady()){
		var res = processor.getChainWorkResult();
		//console.log(results);
		for(idx in res){
			sum_for_check += 1;
			switch(res[idx][0]){
				case 'account_uid':
				{
					getSecondDayRemainingByAccountid(res[idx][1], timestamp_yesterday_begin, idx, res, do_with_SecondDayRemaining_result);
				}
				break;
				case 'role_uid':
				{
					getSecondDayRemainingByRoleid(res[idx][1], timestamp_yesterday_begin, idx, res, do_with_SecondDayRemaining_result);
				}
				break;
			}
		}
	}
}

function get_mysql_data_for_processor(processor, type){
	//取出指定日期的数据
	processor.regNeedMysqlDataName('account');
	processor.regNeedMysqlDataName('login');
	processor.regNeedMysqlDataName(type);

	con_glog.query('SELECT * FROM account_log WHERE UNIX_TIMESTAMP(create_date)=? AND (source_type=1562 OR source_type=1560);', [timestamp_tdby_begin],function(error, results, fields){
		//console.log(results[0]);
		if(error){
			throw error;
		}
		processor.insertMysqlRawResults('account', results);
		fill_result_if_ready(processor);
	});
	con_log.query('SELECT * FROM login_log WHERE UNIX_TIMESTAMP(create_time)>=? AND UNIX_TIMESTAMP(create_time)<?;', [timestamp_yesterday_begin, timestamp_today_begin], function(error, results, fields){
		if(error){
			throw error;
		}
		processor.insertMysqlRawResults('login', results);
		fill_result_if_ready(processor);
	});
	switch(type){
		case 'task':{
			con_log.query('SELECT * FROM task_log WHERE task_type=1 AND op_type=702 AND UNIX_TIMESTAMP(create_time)>=? AND UNIX_TIMESTAMP(create_time)<?;', [timestamp_tdby_begin, timestamp_yesterday_begin], function(error, results, fields){
				//console.log(results[1]);
				if(error){
					throw error;
				}
				processor.insertMysqlRawResults('task', results);
				fill_result_if_ready(processor);
			});
		}
		break;
		case 'level':{
			con_log.query('SELECT * FROM role_exp_level_log WHERE old_level!=new_level AND source_param != 0 AND UNIX_TIMESTAMP(create_time)>=? AND UNIX_TIMESTAMP(create_time)<?;', [timestamp_tdby_begin, timestamp_yesterday_begin], function(error, results, fields){
				//console.log(results[1]);
				if(error){
					throw error;
				}
				processor.insertMysqlRawResults('level', results);
				fill_result_if_ready(processor);
			});
		}
		break;
		case 'duration':{
			con_log.query('SELECT role_uid,sum(online_time) as duration FROM login_log WHERE UNIX_TIMESTAMP(create_time)>=? AND UNIX_TIMESTAMP(create_time)<? GROUP BY role_uid;', [timestamp_tdby_begin, timestamp_yesterday_begin], function(error, results, fields){
				//console.log(results[1]);
				if(error){
					throw error;
				}
				processor.insertMysqlRawResults('duration', results);
				fill_result_if_ready(processor);
			});
		}
		break;
	}
	
}

function getSecondDayRemainingByAccountid(account_array, second_day_start_timestamp, para1, para2, callback){
	if(Count(account_array) === 0){
		callback(null, para1, para2, []);
		return;
	}
	var sql = "SELECT account_uid FROM account_log WHERE UNIX_TIMESTAMP(create_date)=? AND account_uid IN (&) GROUP BY account_uid;";
	sql = fillSqlIn(sql, '&', account_array);
	// console.log(sql);
	con_glog.query(sql, [second_day_start_timestamp], function(error, results, fields){
		//console.log(results);
		callback(error, para1, para2, results);
	});
}

function getSecondDayRemainingByRoleid(role_array, second_day_start_timestamp, para1, para2, callback){
	if(Count(role_array) === 0){
		callback(null, para1, para2, []);
		return;
	}
	var sql = "SELECT role_uid FROM login_log WHERE UNIX_TIMESTAMP(create_time)>=? AND UNIX_TIMESTAMP(create_time)<? AND role_uid IN (&) GROUP BY role_uid;";
	sql = fillSqlIn(sql, '&', role_array);
	// console.log(sql);
	// console.log(role_array);
	con_log.query(sql, [second_day_start_timestamp, second_day_start_timestamp + 24 * 60 * 60], function(error, results, fields){
		//console.log(results);
		callback(error, para1, para2, results);
	});
}

function start_work(processor, type){
	gen_filter_data(processor, type, get_mysql_data_for_processor);
}


schedule.scheduleJob('0 0 4 * * *', function(){
	var processor1 = new LayerDataProcessor();
	var processor2 = new LayerDataProcessor();
	var processor3 = new LayerDataProcessor();
	start_work(processor1, 'level');
	start_work(processor2, 'duration');
	start_work(processor3, 'task');
});

var processor1 = new LayerDataProcessor();
var processor2 = new LayerDataProcessor();
var processor3 = new LayerDataProcessor();
start_work(processor1, 'level');
start_work(processor2, 'duration');
start_work(processor3, 'task');