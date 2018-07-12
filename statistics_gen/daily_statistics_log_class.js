function compare_equal(val1, val2){
	return val1 == val2;
}

function compare_greater(val1, val2){
	return val1 > val2;
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

function Worker(processor, con_glog, con_log, type, col_table, unixtime_begin_of_today){
	this.processor = processor;
	this.show_data = [];
	this.result_data = [];
	this.sum_for_check = 0;
	this.con_glog = con_glog;
	this.con_log = con_log;
	this.type = type;
	this.col_table = col_table;
	this.timestamp_yesterday_begin = unixtime_begin_of_today - 24 * 60 * 60;
	this.timestamp_today_begin = unixtime_begin_of_today;
	this.timestamp_tdby_begin = unixtime_begin_of_today - 2 * 24 * 60 * 60;
}

Worker.prototype.start_work = function(){
	this.gen_filter_data(this.get_mysql_data_for_processor);
}

Worker.prototype.getSecondDayRemainingByAccountid = function(account_array, second_day_start_timestamp, para1, callback){
	if(Count(account_array) === 0){
		callback.call(this, null, para1, []);
		return;
	}
	var sql = "SELECT account_uid FROM account_log WHERE UNIX_TIMESTAMP(create_date)=? AND account_uid IN (&) GROUP BY account_uid;";
	sql = fillSqlIn(sql, '&', account_array);
	// console.log(sql);
	const self = this;
	this.con_glog.query(sql, [second_day_start_timestamp], function(error, results, fields){
		//console.log(results);
		callback.call(self, error, para1, results);
	});
}

Worker.prototype.getSecondDayRemainingByRoleid = function(role_array, second_day_start_timestamp, para1, callback){
	if(Count(role_array) === 0){
		callback.call(this, null, para1, []);
		return;
	}
	var sql = "SELECT role_uid FROM login_log WHERE UNIX_TIMESTAMP(create_time)>=? AND UNIX_TIMESTAMP(create_time)<? AND role_uid IN (&) GROUP BY role_uid;";
	sql = fillSqlIn(sql, '&', role_array);
	// console.log(sql);
	// console.log(role_array);
	const self = this;
	this.con_log.query(sql, [second_day_start_timestamp, second_day_start_timestamp + 24 * 60 * 60], function(error, results, fields){
		//console.log(results);
		callback.call(self, error, para1, results);
	});
}

Worker.prototype.do_with_SecondDayRemaining_result = function(err, idx, result){
	if(err){
		console.log(err);
		return;
	}
	this.result_data[idx].push(result);
	//console.log(result);
	//console.log(sum_for_check);
	this.sum_for_check -= 1;
	var res = [];
	if(this.sum_for_check === 0){
		this.do_with_results();
	}
}

Worker.prototype.get_col_data = function get_col_data(type){
	var data = [];
	switch(type){
		case '栏名':
		{
			for(idx in this.result_data){
				data.push(this.result_data[idx][2]);
			}
		}
		break;
		case '达成人数':
		{
			for(idx in this.result_data){
				data.push(Count(this.result_data[idx][1]));
			}
		}
		break;
		case '相对达成率':
		{
			var denominator = Count(this.result_data[0][1]);
			for(idx in this.result_data){
				if(denominator === 0){
					data.push(0);
				}
				else{
					var val = Count(this.result_data[idx][1]);
					data.push(Percentage(val, denominator));
					denominator = val;
				}
			}
		}
		break;
		case '绝对达成率':
		{
			var denominator = Count(this.result_data[0][1]);
			for(idx in this.result_data){
				if(denominator === 0){
					data.push(0);
				}
				else{
					var val = Count(this.result_data[idx][1]);
					data.push(Percentage(val, denominator));
				}
			}
		}
		break;
		case '离开人数':
		{
			var last_num = Count(this.result_data[0][1]);
			for(idx in this.result_data){
				var val = Count(this.result_data[idx][1]);
				data.push(last_num - val);
				last_num = val;
			}
		}
		break;
		case '次日留存':
		{
			for(idx in this.result_data){
				var val = Count(this.result_data[idx][3]);
				data.push(val);
			}
		}
		break;
		case '留存率':
		{
			for(idx in this.result_data){
				var buf = this.result_data[idx];
				var val1 = Count(buf[1]);
				var val2 = Count(buf[3]);
				if(val1 == 0){
					data.push(Percentage(0, 1));
				}
				else{
					data.push(Percentage(val2, val1));
				}
			}
		}
		break;
	}
	return data;
}

Worker.prototype.do_with_results = function(){
	for(idx in this.col_table){
		this.show_data.push(this.get_col_data(this.col_table[idx]));
	}
	var str = JSON.stringify(this.show_data);
	//var buf = Buffer.from(str);
	this.con_log.query("INSERT INTO daily_statistics_log(type,data,date) VALUES(?,?,FROM_UNIXTIME(?));", [this.type, str, this.timestamp_tdby_begin], function(err, results, fields){
		if(err){
			throw err;
		}
		console.log('OK');
	});
	console.log(this.result_data);
	console.log(this.show_data);
	//console.log(this.type);
}

Worker.prototype.fill_result_if_ready = function(){
	if(this.processor.checkMysqlDataReady()){
		this.result_data = this.processor.getChainWorkResult();
		//console.log(results);
		for(idx in this.result_data){
			this.sum_for_check += 1;
			switch(this.result_data[idx][0]){
				case 'account_uid':
				{
					this.getSecondDayRemainingByAccountid(this.result_data[idx][1], this.timestamp_yesterday_begin, idx, this.do_with_SecondDayRemaining_result);
				}
				break;
				case 'role_uid':
				{
					this.getSecondDayRemainingByRoleid(this.result_data[idx][1], this.timestamp_yesterday_begin, idx, this.do_with_SecondDayRemaining_result);
				}
				break;
			}
		}
	}
}

Worker.prototype.get_mysql_data_for_processor = function(){
	//取出指定日期的数据
	this.processor.regNeedMysqlDataName('account');
	this.processor.regNeedMysqlDataName('login');
	this.processor.regNeedMysqlDataName(this.type);
	const self = this;
	this.con_glog.query('SELECT * FROM account_log WHERE UNIX_TIMESTAMP(create_date)=? AND (source_type=1562 OR source_type=1560);', [this.timestamp_tdby_begin], function(error, results, fields){
		//console.log(results[0]);
		if(error){
			throw error;
		}
		self.processor.insertMysqlRawResults('account', results);
		self.fill_result_if_ready();
	});
	this.con_log.query('SELECT * FROM login_log WHERE UNIX_TIMESTAMP(create_time)>=? AND UNIX_TIMESTAMP(create_time)<?;', [this.timestamp_yesterday_begin, this.timestamp_today_begin], function(error, results, fields){
		if(error){
			throw error;
		}
		self.processor.insertMysqlRawResults('login', results);
		self.fill_result_if_ready();
	});
	switch(this.type){
		case 'task':{
			this.con_log.query('SELECT * FROM task_log WHERE task_type=1 AND op_type=702 AND UNIX_TIMESTAMP(create_time)>=? AND UNIX_TIMESTAMP(create_time)<?;', [this.timestamp_tdby_begin, this.timestamp_yesterday_begin], function(error, results, fields){
				//console.log(results[1]);
				if(error){
					throw error;
				}
				self.processor.insertMysqlRawResults('task', results);
				self.fill_result_if_ready();
			});
		}
		break;
		case 'level':{
			this.con_log.query('SELECT * FROM role_exp_level_log WHERE old_level!=new_level AND source_param != 0 AND UNIX_TIMESTAMP(create_time)>=? AND UNIX_TIMESTAMP(create_time)<?;', [this.timestamp_tdby_begin, this.timestamp_yesterday_begin], function(error, results, fields){
				//console.log(results[1]);
				if(error){
					throw error;
				}
				self.processor.insertMysqlRawResults('level', results);
				self.fill_result_if_ready();
			});
		}
		break;
		case 'duration':{
			this.con_log.query('SELECT role_uid,sum(online_time) as duration FROM login_log WHERE UNIX_TIMESTAMP(create_time)>=? AND UNIX_TIMESTAMP(create_time)<? GROUP BY role_uid;', [this.timestamp_tdby_begin, this.timestamp_yesterday_begin], function(error, results, fields){
				//console.log(results[1]);
				if(error){
					throw error;
				}
				self.processor.insertMysqlRawResults('duration', results);
				self.fill_result_if_ready();
			});
		}
		break;
	}
	
}

Worker.prototype.gen_filter_data = function(callback){
	//注册
	this.processor.pushFilterToChain('account', 'source_type', 1560, compare_equal, 'mobile_uuid', 'account_uid', '注册');
	//创角
	this.processor.pushFilterToChain('account', 'source_type', 1562, compare_equal, 'account_uid', 'role_uid', '创建角色');
	
	const self = this;
	switch(this.type){
		//任务
		//获取主线任务id序列 --保险起见应改为配置表读取
		case 'task':{
			this.con_log.query('SELECT task_id FROM `task_log` AS t5 RIGHT JOIN (SELECT COUNT(*) AS cola,t4.role_uid FROM (SELECT id,t3.role_uid,task_id FROM `task_log` AS t3 LEFT OUTER JOIN (SELECT role_uid FROM (SELECT role_uid,MIN(UNIX_TIMESTAMP(create_time)) as firsttime FROM `task_log` GROUP BY role_uid) as t1 WHERE t1.firsttime>=? AND t1.firsttime<?) AS t2 ON t2.role_uid=t3.role_uid WHERE task_type=1 AND op_type=702) AS t4 GROUP BY t4.role_uid ORDER BY cola DESC LIMIT 1) AS t6 ON t5.role_uid=t6.role_uid WHERE task_type=1 AND op_type=702 ORDER BY id;', [this.timestamp_tdby_begin, this.timestamp_yesterday_begin], function(error, results, fields){
				//console.log(results);
				for(idx in results){
					var task_id = results[idx]['task_id'];
					if(task_id == 0){
						continue;
					}
					var name = '任务' + task_id;
					self.processor.pushFilterToChain('task', 'task_id', task_id, compare_equal, 'role_uid', 'role_uid', name);
				}
				if(callback !== undefined){
					callback.call(self);
				}
			});
		}
		break;
		case 'duration':{
			var duration_cfg = [1, 3, 10, 30, 60];
			var name = '';
			for(idx in duration_cfg){
				var value = duration_cfg[idx];
				var sec = value * 60;
				name = '>' + value + 'min';
				self.processor.pushFilterToChain('duration', 'duration', sec, compare_greater, 'role_uid', 'role_uid', name);
			}
			if(callback !== undefined){
				callback.call(self);
			}		
		}
		break;
		case 'level':{
			for(var i = 2; i <= 100; i++){
				var name = 'lv' + i;
				self.processor.pushFilterToChain('level', 'new_level', i, compare_equal, 'role_uid', 'role_uid', name);
			}
			if(callback !== undefined){
				callback.call(self);
			}		
		}
		break;
	}
}

module.exports = Worker;