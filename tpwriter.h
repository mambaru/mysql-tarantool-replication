#ifndef REPLICATOR_TPWRITER_H
#define REPLICATOR_TPWRITER_H

#include <string>
#include <map>
#include <vector>
#include "serializable.h"
#include <tarantool/tarantool.h>

namespace replicator {

class TPWriter
{
public:
	TPWriter(
		const std::string &host,
		const std::string &user,
		const std::string &password,
		uint32_t binlog_key_space,
		uint32_t binlog_key,
		unsigned connect_retry = 15,
		unsigned sync_retry = 1000,
		bool disconnect_on_error = false
	);
	~TPWriter();

	bool Connect();
	void Disconnect();
	bool ReadBinlogPos(std::string &binlog_name, unsigned long &binlog_pos);
	bool Sync(bool force = false);
	bool BinlogEventCallback(const SerializableBinlogEvent &ev);
	inline void Ping();

	// return values:
	// -1 for error
	// otherwise returns number of complete replies read from socket
	int ReadReply();
	uint64_t GetReplyCode() const;
	const std::string& GetReplyErrorMessage() const;
	bool DisconnectOnError() const { return disconnect_on_error; }

	typedef std::vector<unsigned> Tuple;

	void AddTable(
		const std::string &db,
		const std::string &table,
		const unsigned space,
		const Tuple &keys,
		const std::string &insert_call = empty_call,
		const std::string &update_call = empty_call,
		const std::string &delete_call = empty_call
	);

	static const std::string empty_call;
	std::map<uint32_t, unsigned> space_last_id;

private:
	static const unsigned int PING_TIMEOUT = 5000;

	std::string host;
	std::string user;
	std::string password;
	uint32_t binlog_key_space;
	uint32_t binlog_key;
	std::string binlog_name;
	unsigned long binlog_pos;
	unsigned long seconds_behind_master;
	unsigned connect_retry;
	unsigned sync_retry;
	::time_t next_connect_attempt; /* seconds */
	uint64_t next_sync_attempt; /* milliseconds */
	uint64_t next_ping_attempt; /* milliseconds */
	struct ::tnt_stream sess;
	bool disconnect_on_error;

	// blocking send
	int64_t Send(struct ::tnt_request *req);

	// non-blocking receive
	int Recv(struct ::tnt_reply *re);

	inline void SaveBinlogPos();

	uint64_t Milliseconds();

	uint64_t reply_server_code;
	std::string reply_error_msg;
	uint64_t secbase;

	class TableSpace
	{
	public:
		TableSpace() : space(0), insert_call(""), update_call(""), delete_call("") {}
		uint32_t space;
		Tuple keys;
		std::string insert_call;
		std::string update_call;
		std::string delete_call;
	};

	typedef struct ::tnt_stream s_tnt_stream;
	struct __tnt_object : s_tnt_stream {
		__tnt_object() { ::tnt_object((s_tnt_stream*)this); }
		~__tnt_object() { ::tnt_stream_free((s_tnt_stream*)this); }
		inline s_tnt_stream* operator & () { return (s_tnt_stream*)this; }
	};

	typedef struct ::tnt_request s_tnt_request;
	struct __tnt_request : s_tnt_request {
		~__tnt_request() { ::tnt_request_free((s_tnt_request*)this); }
		inline s_tnt_request* operator & () { return (s_tnt_request*)this; }
	};

	typedef struct ::tnt_reply s_tnt_reply;
	struct __tnt_reply : s_tnt_reply {
		__tnt_reply() { ::tnt_reply_init((s_tnt_reply*)this); }
		~__tnt_reply() { ::tnt_reply_free((s_tnt_reply*)this); }
		inline s_tnt_reply* operator & () { return (s_tnt_reply*)this; }
	};

	typedef std::map<std::string, TableSpace> TableMap;
	typedef std::map<std::string, TableMap> DBMap;
	DBMap dbs;

};

}

#endif // REPLICATOR_TPWRITER_H
