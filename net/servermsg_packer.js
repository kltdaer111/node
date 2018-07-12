function SeverMsgPacker(){}

/*
@author major

pack data for server use

head define:
trunk\network\command.hpp
struct msg_head_t
{
	msg_head_t()
		: len(0)
		, cmd(0)
		, cmd_ex(0)
		, flags(0)
		, unused(0)
	{
	}

	static const uint8_t flag_uid = (1 << 5);
	static const uint8_t flag_callback = (1 << 6);
	static const uint8_t flag_compress = (1 << 7);

	uint32_t len;
	uint16_t cmd;
	uint16_t cmd_ex;
	uint16_t flags;
	int16_t unused;
};

*/

ServerMsgPacker.Pack = function(id, data){
    var headlength = 4 + 2 + 2 + 2 + 2;
    var msg = Buffer.alloc(headlength + data.length);
    msg.writeInt32LE(data.length, 0);
    msg.writeInt16LE(id, 4);
    msg.write(data, headlength);
    return msg;
}

ServerMsgPacker.Unpack = function(buf){
    if(buf.length < 4){
        return false;
    }
    var length = buf.readInt32LE(0);
    if(buf.length < 4 + length){
        return false;
    }
    var id = buf.readInt32LE(4);
    var data = buf.slice(8, 4 + length);
    var total_unpack_size = 4 + length;
    return total_unpack_size, id, data;
}

module.exports = ServerMsgPacker;