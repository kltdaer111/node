function LayerDataProcessor(){
    
}

LayerDataProcessor.prototype.m_mysql_raw_results = {};

LayerDataProcessor.prototype.insertMysqlRawResults = function(name, data){
    this.m_mysql_raw_results[name] = data;
}

LayerDataProcessor.prototype.m_filter = [];

LayerDataProcessor.prototype.pushFilter = function(mysql_raw_data_name, col_name, value){

}

