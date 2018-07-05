function LayerDataProcessor(){
    
}

LayerDataProcessor.prototype.m_need_name = [];

LayerDataProcessor.prototype.regNeedMysqlDataName = function(name){
    this.m_need_name.push(name);
}

LayerDataProcessor.prototype.checkMysqlDataReady = function(){
    for(idx in this.m_need_name){
        if(this.m_mysql_raw_results[this.m_need_name[idx]] === undefined){
            return false;
        }
    }
    return true;
}

LayerDataProcessor.prototype.m_mysql_raw_results = {};

LayerDataProcessor.prototype.insertMysqlRawResults = function(name, data){
    this.m_mysql_raw_results[name] = data;
}

LayerDataProcessor.prototype.m_chain = [];

LayerDataProcessor.prototype.pushFilterToChain = function(mysql_raw_data_name, filter_col_name, filter_value, compare_func, input_col, output_col){
    this.m_chain.push({col : filter_col_name, value : filter_value, src : mysql_raw_data_name, func : compare_func, input : input_col, output : output_col});
}

LayerDataProcessor.prototype.__filter_mysql_raw_data = function(mysql_raw_data, filter_col, value, func){
	var res = [];
	for(idx in mysql_raw_data){
		var piece = mysql_raw_data[idx];
		if(func(piece[filter_col], value)){
			res.push(piece);
		}
	}
	return res;
}

LayerDataProcessor.prototype.__gen_remaining = function(input, input_data, output, filtered_data){
	var res = {};
	for(idx in filtered_data){
		var every = filtered_data[idx];
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

LayerDataProcessor.prototype.getChainWorkResult = function(){
    var result = [];
    var data = null;
    for(idx in this.m_chain){
        var filter = this.m_chain[idx];
        var data = this.__gen_remaining(filter.input, data, filter.output, this.__filter_mysql_raw_data(this.m_mysql_raw_results[filter.src], filter.col, filter.value, filter.func));
        result.push([filter.output, data]);
    }
    return result;
}

function LayerDataProcessorResult(){
    
}

module.exports.LayerDataProcessor = LayerDataProcessor;