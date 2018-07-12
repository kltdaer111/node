const mysql = require('mysql');
const schedule = require('node-schedule');
const moment = require('moment');
const layer_data_processor = require('./layer_data_processor');
const LayerDataProcessor = layer_data_processor.LayerDataProcessor;
const statistics_gen = require('./statistics_class');
const Worker = require('./daily_statistics_log_class');
const common_struct = require('./common_struct');
const util = require('util');

var glog_dbconfig = new common_struct.DBConfig('139.196.41.108', 'wsy', 'Wsy1985!', 'wai_trunk_sg_glog');
// {
// 	host : '139.196.41.108',
// 	user : 'wsy',
// 	password : 'Wsy1985!',
// 	database : 'wai_trunk_sg_glog',
// };

var stcs_dbconfig = {
	host : '139.196.41.108',
	user : 'wsy',
	password : 'Wsy1985!',
	database : 'sg_statistics',
};

var log_dbconfig = {
	host : '192.168.1.5',
	user : 'root',
	password : 'Sanguo1!',
	database : 'nei_trunk_sg_log',
};

function timer_work(){
	var col_table = ['栏名', '达成人数', '相对达成率', '绝对达成率', '离开人数', '次日留存', '留存率'];
	var con_glog = mysql.createConnection(glog_dbconfig);
	var con_stcs = mysql.createConnection(stcs_dbconfig);
	var con_log = mysql.createConnection(log_dbconfig);

	var start_of_today = moment().startOf('day');
	var timestamp_today_begin = start_of_today.unix();
	var day_seconds = 24 * 60 * 60;
	var timestamp_yesterday_begin = timestamp_today_begin - day_seconds;

	var processor1 = new LayerDataProcessor();
	var worker1 = new Worker(processor1, con_glog, con_log, 'level', col_table, timestamp_today_begin);
	var processor2 = new LayerDataProcessor();
	var worker2 = new Worker(processor2, con_glog, con_log, 'duration', col_table, timestamp_today_begin);
	var processor3 = new LayerDataProcessor();
	var worker3 = new Worker(processor3, con_glog, con_log, 'task', col_table, timestamp_today_begin);

	var worker4 = new statistics_gen.NewUserGen(con_glog, con_stcs, timestamp_yesterday_begin);
	var worker5 = new statistics_gen.ActiveUserGen(con_glog, con_stcs, timestamp_yesterday_begin);
	var worker6 = new statistics_gen.RemainUser(con_glog, con_stcs, timestamp_yesterday_begin);
	var worker7 = new statistics_gen.ComeBackUser(con_glog, con_stcs, timestamp_yesterday_begin - 4 * 24 * 60 * 60, timestamp_yesterday_begin);

	worker1.start_work();
	worker2.start_work();
	worker3.start_work();
	worker4.gen();
	worker5.gen();
	worker6.gen();
	worker7.gen();
}



function service_start(service_manager){
	schedule.scheduleJob('0 0 4 * * *', function(){
		timer_work();
	});
}

module.exports = service_start;

