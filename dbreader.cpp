#include <sstream>
#include "dbreader.h"
#include "serializable.h"

using std::placeholders::_1;

namespace replicator {

DBReader::DBReader(nanomysql::mysql_conn_opts &opts, unsigned connect_retry)
	: state(), slave(slave::MasterInfo(opts, connect_retry), state), stopped(false), last_event_when(0) {}

DBReader::~DBReader()
{
	slave.close_connection();
}

void DBReader::AddTable(const std::string& db, const std::string& table, std::map<std::string, unsigned>& filter, bool do_dump)
{
	tables.emplace_back(db, table, filter, do_dump);
}

void DBReader::DumpTables(std::string &binlog_name, BinlogPos &binlog_pos, BinlogEventCallback cb)
{
	slave::callback dummycallback = std::bind(&DBReader::DummyEventCallback, this, _1);

	// start temp slave to read DB structure
	slave::Slave tempslave(slave.masterInfo(), state);
	for (auto i = tables.begin(), end = tables.end(); i != end; ++i) {
		tempslave.setCallback(i->name.first, i->name.second, dummycallback);
	}

	tempslave.init();
	tempslave.createDatabaseStructure();

	last_event_when = ::time(NULL);

	slave::Slave::binlog_pos_t bp = tempslave.getLastBinlog();
	binlog_name = bp.first;
	binlog_pos = bp.second;

	state.setMasterLogNamePos(bp.first, bp.second);

	// dump tables
	nanomysql::Connection conn(slave.masterInfo().conn_options);

	conn.query("SET NAMES utf8");

	for (auto table = tables.begin(), end = tables.end(); !stopped && table != end; ++table) {
		if (!table->do_dump) continue;

		// build field_name -> field_ptr map for filtered columns
		std::vector<std::pair<unsigned, slave::PtrField>> filter_;
		const auto rtable = tempslave.getRli().getTable(table->name);
		std::string s_fields;

		for (const auto ptr_field : rtable->fields) {
			const auto it = table->filter.find(ptr_field->field_name);
			if (it != table->filter.end()) {
				filter_.emplace_back(it->second, ptr_field);
				s_fields += ptr_field->field_name;
				s_fields += ',';
			}
		}
		s_fields.pop_back();

		conn.select_db(table->name.first);
		conn.query(std::string("SELECT ") + s_fields  + " FROM " + table->name.second);
		conn.use(std::bind(&DBReader::DumpTablesCallback,
			this,
			std::cref(table->name.first),
			std::cref(table->name.second),
			std::cref(filter_),
			_1,
			cb
		));
	}

	// send binlog position update event
	if (!stopped) {
		SerializableBinlogEvent ev;
		ev.binlog_name = binlog_name;
		ev.binlog_pos = binlog_pos;
		ev.seconds_behind_master = GetSecondsBehindMaster();
		ev.unix_timestamp = long(time(NULL));
		ev.event = "IGNORE";
		stopped = cb(ev);
	}

	//tempslave.close_connection();
}

void DBReader::ReadBinlog(const std::string &binlog_name, BinlogPos binlog_pos, BinlogEventCallback cb)
{
	stopped = false;
	state.setMasterLogNamePos(binlog_name, binlog_pos);

	for (auto table = tables.begin(), end = tables.end(); table != end; ++table) {
		slave.setCallback(
			table->name.first,
			table->name.second,
			std::bind(&DBReader::EventCallback, this, _1, std::cref(table->filter), cb)
		);
	}
	slave.setXidCallback(std::bind(&DBReader::XidEventCallback, this, _1, cb));
	slave.init();
	slave.createDatabaseStructure();

	slave.get_remote_binlog(std::bind(&DBReader::ReadBinlogCallback, this));
}

void DBReader::Stop()
{
	stopped = true;
	slave.close_connection();
}

void DBReader::EventCallback(const slave::RecordSet& event, const std::map<std::string, unsigned>& filter, BinlogEventCallback cb)
{
	last_event_when = event.when;

	SerializableBinlogEvent ev;
	ev.binlog_name = state.getMasterLogName();
	ev.binlog_pos = state.getMasterLogPos();
	ev.seconds_behind_master = GetSecondsBehindMaster();
	ev.unix_timestamp = long(time(NULL));
	ev.database = event.db_name;
	ev.table = event.tbl_name;

	switch (event.type_event) {
		case slave::RecordSet::Delete: ev.event = "DELETE"; break;
		// case slave::RecordSet::Update: ev.event = "UPDATE"; break;
		// case slave::RecordSet::Write:  ev.event = "INSERT"; break;
		case slave::RecordSet::Update:
		case slave::RecordSet::Write:  ev.event = "UPSERT"; break;
		default:                       ev.event = "IGNORE";
	}

	for (auto fi = filter.begin(), end = filter.end(); fi != end; ++fi) {
		const auto ri = event.m_row.find(fi->first);
		if (ri != event.m_row.end()) {
			ev.row[ fi->second ] = ri->second;
		}
	}

	stopped = cb(ev);
}

void DBReader::XidEventCallback(unsigned int server_id, BinlogEventCallback cb)
{
	last_event_when = ::time(NULL);

	// send binlog position update event
	SerializableBinlogEvent ev;
	ev.binlog_name = state.getMasterLogName();
	ev.binlog_pos = state.getMasterLogPos();
	ev.seconds_behind_master = GetSecondsBehindMaster();
	ev.unix_timestamp = long(time(NULL));
	ev.event = "IGNORE";
	stopped = cb(ev);
}

bool DBReader::ReadBinlogCallback()
{
	return stopped != 0;
}

void DBReader::DumpTablesCallback(
	const std::string &db_name,
	const std::string &tbl_name,
	const std::vector<std::pair<unsigned, slave::PtrField>>& filter,
	const nanomysql::fields_t& fields,
	BinlogEventCallback cb
) {
	SerializableBinlogEvent ev;
	ev.binlog_name = "";
	ev.binlog_pos = 0;
	ev.database = db_name;
	ev.table = tbl_name;
	// ev.event = "INSERT";
	ev.event = "UPSERT";
	ev.seconds_behind_master = GetSecondsBehindMaster();
	ev.unix_timestamp = long(time(NULL));

	for (const auto& it : filter) {
		slave::PtrField ptr_field = it.second;
		const auto& field = fields.at(ptr_field->field_name);
		if (field.is_null) {
			ev.row[ it.first ] = boost::any();
		} else {
			ptr_field->unpack_str(field.data);
			ev.row[ it.first ] = ptr_field->field_data;
		}
	}
	if (!stopped) {
		stopped = cb(ev);
	}
}

unsigned DBReader::GetSecondsBehindMaster() const
{
	return std::max(::time(NULL) - last_event_when, 0L);
}

} // replicator
