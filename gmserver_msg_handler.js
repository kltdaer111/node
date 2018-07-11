function GM_server_msg_handler(){
    this.msg_func = new Map();
}

GM_server_msg_handler.prototype.on_msg = function(id, msg){
   if(this.msg_func.has(id)){
       let func = this.msg_func.get(id);
       func(msg);
   }
}

GM_server_msg_handler.prototype.reg_func= function(id, func){
    this.msg_func.set(id, func);
}