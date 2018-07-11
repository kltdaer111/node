const net = require('net');
const packer = require('./servermsg_packer');

function GMConnector(ip, port){
    this.ip = ip;
    this.port = port;
    this.socket = null;
    this.buf = Buffer.alloc(0);
}

GMConnector.prototype.connect = function(){
    this.socket = net.createConnection(this.port, this.ip);
}

GMConnector.prototype.bind_handler = function(handler){
    this.handler = handler;
}

GMConnector.prototype.start_recv = function(){
    var self = this;
    this.socket.on('data', (buff) => {
        var data = Buffer.concat([self.buf, buff]);
        var size, id, data = packer.Unpack(data);
        if(size === false){
            self.buf = data;
        }
        if(size < data.length){
            self.buf = data.slice(size, data.length);
        }
        handler(id, data);
    });
}

GMConnector.prototype.send = function(id, data){
    var msg = packer.Pack(id, data);
    this.socket.write(msg);
}