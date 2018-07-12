var GMConnector = require('../net/gmserver_connector');

function net_service(service_recorder){
    this.service_recorder = service_recorder;
    this.name = 'net';
    this.depend = ['config_fetcher'];
}

net_service.start = function(){
    var res = this.check_dependence();
    if(res !== true){
        throw new Error(this.name + ' service depend on ' + res + ' service, and it has not been started yet.');
    }
    this.gm_con = new GMConnector;
    service_recorder.add_service(this.name, this);
}

net_service.check_dependence = function(){
    for(let v of this.depend){
        if(this.service_recorder.has_started(v)){
            return v;
        }
    }
    return true;
}