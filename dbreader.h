#ifndef REPLICATOR_DBREADER_H
#define REPLICATOR_DBREADER_H

#include <vector>
#include <string>
#include <utility>
#include <functional>

#include <Slave.h>
#include <DefaultExtState.h>
#include <nanomysql.h>

#include "serializable.h"

namespace replicator {

typedef std::function<bool (SerializableBinlogEventPtr&&)> BinlogEventCallback;

struct DBTable
{
	DBTable() {}

	DBTable(const std::string& db, const std::string& table, std::map<std::string, unsigned>& filter, bool do_dump_)
		: name(db, table), filter(filter), do_dump(do_dump_) {}

	std::pair<std::string, std::string> name;
	std::map<std::string, unsigned> filter;
	bool do_dump;
};

class DBReader
{
public:
	DBReader (nanomysql::mysql_conn_opts &opts, unsigned int connect_retry = 60);
	~DBReader();

	void AddTable(const std::string &db, const std::string &table, std::map<std::string, unsigned>& filter, bool do_dump);
	void DumpTables(std::string &binlog_name, unsigned long &binlog_pos, BinlogEventCallback f);
	void ReadBinlog(const std::string &binlog_name, unsigned long binlog_pos, BinlogEventCallback cb);
	void Stop();

	void EventCallback(const slave::RecordSet& event, const std::map<std::string, unsigned>& filter, BinlogEventCallback cb);
	void DummyEventCallback(const slave::RecordSet& event) {};

	void XidEventCallback(unsigned int server_id, BinlogEventCallback cb);
	void DumpTablesCallback(
		const std::string &db_name,
		const std::string &tbl_name,
		const std::vector<std::pair<unsigned, slave::PtrField>>& filter,
		const nanomysql::fields_t &f,
		BinlogEventCallback cb
	);

	unsigned GetSecondsBehindMaster() const;

private:
	slave::DefaultExtState state;
	slave::Slave slave;
	std::vector<DBTable> tables;
	bool stopped;

	::time_t last_event_when;
};

} // replicator

#endif // REPLICATOR_DBREADER_H
