const mysql = require('mysql');

function Percentage(number1, number2) { 
    return (Math.round(number1 / number2 * 10000) / 100.00 + "%");// 小数点后两位百分比
}

function NewUserGen(con_glog, con_statistics, day_start_time){
    this.con_glog = con_glog;
    this.con_statistics = con_statistics;

    this.new_mobile_count = 0;
    this.new_account_count = 0;
    this.reg_mobile_count = 0;
    this.new_role_count = 0;
    
    this.cov_rate = 0;
    this.start_time = day_start_time;
    this.end_time = day_start_time + 24 * 60 * 60;
};

NewUserGen.prototype.gen = function(){
    var self = this;
    //新增设备
    self.con_glog.query("SELECT COUNT(id) AS quantity FROM mobile_log WHERE UNIX_TIMESTAMP(create_time)>=? AND UNIX_TIMESTAMP(create_time)<?;", [self.start_time, self.end_time], function(err, results, fields){
        console.log(results);
        self.new_mobile_count = results[0].quantity;
        

        //新增账号 注册设备
        self.con_glog.query("SELECT COUNT(DISTINCT account_uid) AS quantity1, COUNT(DISTINCT mobile_uuid) AS quantity2 FROM account_log WHERE source_type=1560 AND UNIX_TIMESTAMP(create_time)>=? AND UNIX_TIMESTAMP(create_time)<?;", [self.start_time, self.end_time], function(err, results, fields){
            console.log(results);
            self.new_account_count = results[0].quantity1;
            self.reg_mobile_count = results[0].quantity2;

            //新增角色
            self.con_glog.query("SELECT COUNT(DISTINCT role_uid) AS quantity FROM account_log WHERE source_type=1562 AND UNIX_TIMESTAMP(create_time)>=? AND UNIX_TIMESTAMP(create_time)<?;", [self.start_time, self.end_time], function(err, results, fields){
                console.log(results);
                self.new_role_count = results[0].quantity;

                //插入数据库
                self.con_statistics.query("REPLACE INTO `new_user_log`(`time`, `mobile`, `reg_mobile`, `account`, `role`, `rate`) VALUES(FROM_UNIXTIME(?), ?, ?, ?, ?, ?);", [self.start_time, self.new_mobile_count, self.reg_mobile_count, self.new_account_count, self.new_role_count, self.reg_mobile_count / self.new_account_count], function(err, results, fields){
                    if(err){
                        console.log(err);
                    }
                })
            });
        });
    });

}

function ActiveUserGen(con_glog, con_statistics, day_start_time){
    this.con_glog = con_glog;
    this.con_statistics = con_statistics;

    this.start_time = day_start_time;
    this.end_time = day_start_time + 24 * 60 * 60;
    this.dau = 0;
    this.wau = 0;
    this.mau = 0;
}

ActiveUserGen.prototype.get_need_data = function(begin, end, callback){
    var self = this;
    this.con_glog.query("SELECT COUNT(DISTINCT account_uid) AS quantity FROM account_log WHERE UNIX_TIMESTAMP(create_time)>=? AND UNIX_TIMESTAMP(create_time)<?", [begin, end], function(err, results, fields){
        //console.log(results);
        callback.call(self, results);
    });
}

ActiveUserGen.prototype.gen = function(){
    //日活跃
    var self = this;
    this.get_need_data(this.start_time, this.end_time, function(data){
        console.log(data);
        self.dau = data[0].quantity;
        //周活跃
        self.get_need_data(self.end_time - 7 * 24 * 60 * 60, self.end_time, function(data){
            console.log(data);
            self.wau = data[0].quantity;
            //月活跃
            self.get_need_data(self.end_time - 30 * 24 * 60 * 60, self.end_time, function(data){
                console.log(data);
                self.mau = data[0].quantity;
                self.con_statistics.query("REPLACE INTO `active_user_log`(`time`, `dau`, `wau`, `mau`, `dau/mau`) VALUES(FROM_UNIXTIME(?), ?, ?, ?, ?);", [self.start_time, self.dau, self.wau, self.mau, self.dau/self.mau],function(err, results, fields){
                    if(err){
                        console.log(err);
                    }
                });
            });
        });
    });
    
    
}

function RemainUser(con_glog, con_statistics, day_start_time){
    this.con_glog = con_glog;
    this.con_statistics = con_statistics;

    this.start_time = day_start_time;
    this.end_time = day_start_time + 24 * 60 * 60;
}

RemainUser.prototype.get_need_data = function(interval_day, callback){
    var new_date_start = this.end_time - interval_day * 24 * 60 * 60;
    var new_date_end = new_date_start + 24 * 60 * 60;
    var self = this;
    self.con_glog.query("SELECT COUNT(DISTINCT account_uid) AS quantity FROM account_log WHERE source_type=1560 AND UNIX_TIMESTAMP(create_time)>=? AND UNIX_TIMESTAMP(create_time)<?", [new_date_start, new_date_end], function(err, results, fields){
        // console.log(results);

        self.con_glog.query("SELECT COUNT(t1.account_uid) AS quantity FROM (SELECT DISTINCT account_uid FROM account_log WHERE source_type=1560 AND UNIX_TIMESTAMP(create_time)>=? AND UNIX_TIMESTAMP(create_time)<?) AS t1 INNER JOIN (SELECT DISTINCT account_uid FROM account_log WHERE UNIX_TIMESTAMP(create_time)>=? AND UNIX_TIMESTAMP(create_time)<?) AS t2 ON t1.account_uid=t2.account_uid;", [new_date_start, new_date_end, self.start_time, self.end_time], function(err, results, fields){
            callback.call(self, new_date_start, results);
        });
    });

    
}

RemainUser.prototype.gen = function(){
    var self = this;
    //生成昨天的次日留存
    this.get_need_data(2, function(date, data){
        console.log(data);
        //插入数据库
        self.con_statistics.query("INSERT INTO remain_user_log(time, second_day) VALUES(FROM_UNIXTIME(?), ?) ON DUPLICATE KEY UPDATE second_day=?;", [date, data[0].quantity, data[0].quantity], function(err, results, fields){
            if(err){
                console.log(err);
            };
        });
    });
     //生成七天前的七日留存
     self.get_need_data(7, function(date, data){
        console.log(data);
        //插入数据库
        self.con_statistics.query("INSERT INTO remain_user_log(time, seventh_day) VALUES(FROM_UNIXTIME(?), ?) ON DUPLICATE KEY UPDATE seventh_day=?;", [date, data[0].quantity, data[0].quantity], function(err, results, fields){
            if(err){
                console.log(err);
            };
        });
    });
    //生成30天前的月留存
    self.get_need_data(30, function(date, data){
        console.log(data);
        //插入数据库
        self.con_statistics.query("INSERT INTO remain_user_log(time, thirtieth_day) VALUES(FROM_UNIXTIME(?), ?) ON DUPLICATE KEY UPDATE thirtieth_day=?;", [date, data[0].quantity, data[0].quantity], function(err, results, fields){
            if(err){
                console.log(err);
            };
        });
    });
}

function ComeBackUser(con_glog, con_statistics, active_time_begin, back_time_begin){
    this.con_glog = con_glog;
    this.con_statistics = con_statistics;

    this.active_time_begin = active_time_begin;
    this.active_time_end = active_time_begin + 24 * 60 * 60;
    this.back_time_begin = back_time_begin;
    this.back_time_end = back_time_begin + 24 * 60 * 60;
    this.active_quantity = 0;
    this.leave_quantity = 0;
    this.back_quantity = 0;
}

function fillSqlIn(sql, symbol, array){
	var tmp = '';
	for(idx in array){
		tmp += "'" + idx + "',";
	}
	tmp = tmp.slice(0, -1);
	return sql.replace(symbol, tmp);
}

ComeBackUser.prototype.gen = function (){
    //活跃账号数量
    var self = this;
    this.con_glog.query("SELECT COUNT(DISTINCT account_uid) AS quantity FROM account_log WHERE UNIX_TIMESTAMP(create_time)>=? AND UNIX_TIMESTAMP(create_time)<?", [self.active_time_begin, self.active_time_end], function(err, results, fields){
        console.log(results);
        self.active_quantity = results[0].quantity;
        //活跃账号中的流失账号
        self.con_glog.query("SELECT t1.account_uid FROM (SELECT DISTINCT account_uid FROM account_log WHERE UNIX_TIMESTAMP(create_time)>=? AND UNIX_TIMESTAMP(create_time)<?) AS t1 LEFT JOIN (SELECT DISTINCT account_uid FROM account_log WHERE UNIX_TIMESTAMP(create_time)>=? AND UNIX_TIMESTAMP(create_time)<?) AS t2 ON t1.account_uid=t2.account_uid WHERE t2.account_uid IS NULL", [self.active_time_begin, self.active_time_end, self.active_time_end, self.back_time_begin], function(err, need_result, fields){
            if(err){
                console.log(err);
            }
            console.log(need_result.length);
            self.leave_quantity = need_result.length;
            var sql = "SELECT COUNT(DISTINCT account_uid) AS quantity FROM account_log WHERE UNIX_TIMESTAMP(create_time)>=? AND UNIX_TIMESTAMP(create_time)<? AND account_uid IN (&)";
            sql = fillSqlIn(sql, '&', need_result);
            self.con_glog.query(sql, [self.back_time_begin, self.back_time_end], function(err, results, fields){
                if(err){
                    console.log(err);
                }
                console.log(results);
                self.back_quantity = results.length;
                //插入数据库
                self.con_statistics.query("REPLACE INTO `leave_retrun_user_log`(`time`, `leave_count`, `leave_rate`, `return_count`, interval_days) VALUES(FROM_UNIXTIME(?), ?, ?, ?, ?);", [self.active_time_begin, self.leave_quantity, self.leave_quantity/self.active_quantity, self.back_quantity, (self.back_time_begin - self.active_time_begin)/(24 * 60 * 60)], function(err, results, fields){
                    if(err){
                        console.log(err);
                    }
                });
            });
        });

    });
}

module.exports.NewUserGen = NewUserGen;
module.exports.ActiveUserGen = ActiveUserGen;
module.exports.RemainUser = RemainUser;
module.exports.ComeBackUser = ComeBackUser;
