function RunningServiceRecorder(){
    this.running_service = new Map();
}

RunningServiceRecorder.prototype.add_service = function(service_name, service){
    if(this.running_service.has(service_name)){
        return false;
    }
    this.running_service.set(service_name, service);
    return true;
}

RunningServiceRecorder.prototype.has_started = function(service_name){
    return this.running_service.has(service_name);
}

RunningServiceRecorder.prototype.get_service = function(service_name){
    return this.running_service.get(service_name);
}




