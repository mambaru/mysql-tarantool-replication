#include <sstream>
#include <boost/any.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/text_oarchive.hpp>

#include <tarantool/tarantool.h>
#include <tarantool/tnt_net.h>
#include <tarantool/tnt_opt.h>
#include <msgpuck.h>

#include <zmq.h>
#include <zmq_utils.h>

#include <sys/time.h>

#include "tpwriter.h"
#include "serializable.h"

namespace replicator {

const std::string TPWriter::empty_call("");

TPWriter::TPWriter(
	const std::string &host,
	const std::string &user,
	const std::string &password,
	uint32_t binlog_key_space,
	uint32_t binlog_key,
	unsigned connect_retry,
	unsigned sync_retry,
	bool disconnect_on_error
) :
	host(host),
	user(user),
	password(password),
	binlog_key_space(binlog_key_space),
	binlog_key(binlog_key),
	binlog_name(""),
	binlog_pos(0),
	seconds_behind_master(0),
	connect_retry(connect_retry),
	sync_retry(sync_retry),
	next_connect_attempt(0),
	next_sync_attempt(0),
	next_ping_attempt(0),
	disconnect_on_error(disconnect_on_error),
	reply_server_code(0),
	reply_error_msg("")
{
	::tnt_net(&sess);

	::tnt_set(&sess, ::TNT_OPT_URI, host.c_str());
	::tnt_set(&sess, ::TNT_OPT_SEND_BUF, 0);
	::tnt_set(&sess, ::TNT_OPT_RECV_BUF, 16 * 1024 * 1024);

	::timeval tmout;
	auto make_timeout = [&tmout] (const unsigned t) -> ::timeval* {
		tmout.tv_sec  =  t / 1000;
		tmout.tv_usec = (t % 1000) * 1000;
		return &tmout;
	};

	::tnt_set(&sess, ::TNT_OPT_TMOUT_RECV, make_timeout(100));
	::tnt_set(&sess, ::TNT_OPT_TMOUT_SEND, make_timeout(10000));
}

bool TPWriter::Connect()
{
	// connect to tarantool
	if (::time(NULL) < next_connect_attempt) {
		::sleep(1);
		return false;
	}
	if (::tnt_connect(&sess) < 0) {
		std::cout << "Could not connect to Tarantool: " << ::tnt_strerror(&sess) << std::endl;
		::tnt_close(&sess);
		next_connect_attempt = ::time(NULL) + connect_retry;
		return false;
	}

	std::cout << "Connected to Tarantool at " << host << std::endl;
	next_sync_attempt = 0;

	return true;
}

TPWriter::~TPWriter()
{
	::tnt_stream_free(&sess);
}

bool TPWriter::ReadBinlogPos(std::string &binlog_name, unsigned long &binlog_pos)
{
	// read initial binlog pos
	int64_t sync;
	{
		__tnt_object key;
		::tnt_object_add_array(&key, 1);
		::tnt_object_add_uint(&key, binlog_key);
		::tnt_object_container_close(&key);

		__tnt_request req;
		::tnt_request_select(&req);
		::tnt_request_set_space(&req, binlog_key_space);
		::tnt_request_set_limit(&req, 1);
		::tnt_request_set_key(&req, &key);

		sync = Send(&req);
	}

	__tnt_reply re;
	do {
		const int r = Recv(&re);
		if (r == 0) {
			break;
		}
		else if (r < 0 && reply_server_code) {
			std::cerr << "ReadBinlogPos Tarantool error: " << reply_error_msg << " (code: " << reply_server_code << ")" << std::endl;
			return false;
		}
		else {
			std::cerr << "ReadBinlogPos error: no replies, weird" << std::endl;
			return false;
		}
	} while (true);

	if ((&re)->sync != sync) {
		std::cerr << "ReadBinlogPos error: not requested reply" << std::endl;
		return false;
	}

	do {
		const char *data = (&re)->data;

		// result rows
		if (mp_unlikely(mp_typeof(*data) != MP_ARRAY)) break;
		if (mp_unlikely(mp_decode_array(&data) == 0)) {
			// no binlog created yet
			this->binlog_name = binlog_name = "";
			this->binlog_pos = binlog_pos = 0;
			return true;
		}

		// row
		if (mp_unlikely(mp_typeof(*data) != MP_ARRAY)) break;
		if (mp_unlikely(mp_decode_array(&data) != 3)) break;

		// binlog_key
		if (mp_unlikely(mp_typeof(*data) != MP_UINT)) break;
		if (mp_unlikely(mp_decode_uint(&data) != binlog_key)) break;;

		if (mp_unlikely(mp_typeof(*data) != MP_STR)) break;
		uint32_t _binlog_name_len;
		const char *_binlog_name = mp_decode_str(&data, &_binlog_name_len);

		if (mp_unlikely(mp_typeof(*data) != MP_UINT)) break;
		uint64_t _binlog_pos = mp_decode_uint(&data);

		this->binlog_name = binlog_name = std::string(_binlog_name, _binlog_name_len);
		this->binlog_pos = binlog_pos = _binlog_pos;

		next_ping_attempt = Milliseconds() + TPWriter::PING_TIMEOUT;

		return true;
	} while (0);

	std::cerr << "binlog record format error" << std::endl;
	this->binlog_name = binlog_name = "";
	this->binlog_pos = binlog_pos = 0;
	return true;
}

void TPWriter::Disconnect()
{
	::tnt_close(&sess);
}

inline void TPWriter::Ping()
{
	__tnt_request req;
	::tnt_request_ping(&req);
	Send(&req);
}

void TPWriter::AddTable(
	const std::string &db,
	const std::string &table,
	const unsigned space,
	const Tuple &keys,
	const std::string &insert_call,
	const std::string &update_call,
	const std::string &delete_call
) {
	TableMap &d = dbs[db];
	TableSpace &s = d[table];
	s.space = space;
	s.keys = keys;
	s.insert_call = insert_call;
	s.update_call = update_call;
	s.delete_call = delete_call;
}

inline void TPWriter::SaveBinlogPos()
{
	__tnt_object tuple;
	::tnt_object_add_array(&tuple, 3);
	::tnt_object_add_uint(&tuple, binlog_key);
	::tnt_object_add_str(&tuple, binlog_name.c_str(), binlog_name.length());
	::tnt_object_add_uint(&tuple, binlog_pos);
	::tnt_object_container_close(&tuple);

	__tnt_request req;
	::tnt_request_replace(&req);
	::tnt_request_set_space(&req, binlog_key_space);
	::tnt_request_set_tuple(&req, &tuple);

	Send(&req);
}

bool TPWriter::BinlogEventCallback(const SerializableBinlogEvent &ev)
{
	// spacial case event "IGNORE", which only updates binlog position
	// but doesn't modify any table data
	if (ev.event == "IGNORE")
		return false;

	const auto idb = dbs.find(ev.database);
	if (idb == dbs.end())
		return false;

	const TableMap &tm = idb->second;
	const auto itm = tm.find(ev.table);
	if (itm == tm.end())
		return false;

	const TableSpace &ts = itm->second;

	const auto irn = replace_null.find(ts.space);
	const std::map<unsigned, SerializableValue>* replace_null_;
	if (irn != replace_null.end()) {
		replace_null_ = &irn->second;
	} else {
		replace_null_ = NULL;
	}

	auto add_nil_with_replace = [&] (struct ::tnt_stream *o, const unsigned index) -> void {
		if (replace_null_) {
			const auto irnv = replace_null_->find(index);
			if (irnv != irn->second.end()) {
				const SerializableValue& v = irnv->second;
				if (v.get_type_id() == "string") {
					const std::string &vs = v.value_string();
					::tnt_object_add_str(o, vs.c_str(), vs.length());
				} else if (v.get_type_id() == "int") {
					::tnt_object_add_int(o, boost::any_cast<long long>(*v));
				} else if (v.get_type_id() == "uint") {
					::tnt_object_add_uint(o, boost::any_cast<unsigned long long>(*v));
				} else {
					throw std::range_error(std::string("Typecasting error for non-null value for column: " + index));
				}
				return;
			}
		}

		::tnt_object_add_nil(o);
	};

	auto add_value = [&] (struct ::tnt_stream *o, const unsigned index, const SerializableValue &v) -> void {
		try {
			if (v.get_type_id() == "string") {
				const std::string &vs = v.value_string();
				::tnt_object_add_str(o, vs.c_str(), vs.length());
			} else if (v.get_type_id() == "int") {
				::tnt_object_add_int(o, boost::any_cast<long long>(*v));
			} else if (v.get_type_id() == "uint") {
				::tnt_object_add_uint(o, boost::any_cast<unsigned long long>(*v));
			} else if (v.get_type_id() == "float") {
				::tnt_object_add_float(o, boost::any_cast<float>(*v));
			} else if (v.get_type_id() == "double") {
				::tnt_object_add_double(o, boost::any_cast<double>(*v));
			} else {
				add_nil_with_replace(o, index);
			}
		}
		catch (boost::bad_any_cast &ex) {
			throw std::range_error(std::string("Typecasting error for column: ") + ex.what());
		}
	};

	auto add_key = [&] (struct ::tnt_stream *o) -> void {
		::tnt_object_add_array(o, ts.keys.size());
		for (const auto i : ts.keys) {
			add_value(o, i, ev.row.at(i));
		}
		::tnt_object_container_close(o);
	};

	auto add_tuple = [&] (struct ::tnt_stream *o) -> void {
		::tnt_object_add_array(o, space_last_id[ts.space] + 1);
		unsigned i_nil = 0;
		// ev.row may have gaps, since it's not an array but a map
		// so fill the gaps to match columns count
		for (auto it = ev.row.begin(), end = ev.row.end(); it != end; ++it) {
			// fill gaps
			for (; i_nil < it->first; ++i_nil) add_nil_with_replace(o, i_nil);

			add_value(o, it->first, it->second);
			i_nil = it->first + 1;
		}
		// fill gaps
		for (; i_nil <= space_last_id[ts.space]; ++i_nil) add_nil_with_replace(o, i_nil);

		::tnt_object_container_close(o);
	};

	auto add_ops = [&] (struct ::tnt_stream *o, const bool sparse = true) -> void {
		if (sparse) {
			::tnt_update_container_reset(o);
		} else {
			::tnt_object_add_array(o, ev.row.size());
		}
		for (auto it = ev.row.begin(), end = ev.row.end(); it != end; ++it) {
			__tnt_object sval;
			add_value(&sval, it->first, it->second);
			::tnt_update_assign(o, it->first, &sval);
		}
		if (sparse) {
			::tnt_update_container_close(o);
		} else {
			::tnt_object_container_close(o);
		}
	};

	// add Tarantool request
	if (ev.event == "DELETE") {
		if (ts.delete_call.empty()) {
			__tnt_object key;
			add_key(&key);

			__tnt_request req;
			::tnt_request_delete(&req);
			::tnt_request_set_space(&req, ts.space);
			::tnt_request_set_key(&req, &key);

			Send(&req);
		} else {
			__tnt_object args;
			::tnt_object_add_array(&args, 1);
			add_key(&args);
			::tnt_object_container_close(&args);

			__tnt_request req;
			::tnt_request_call(&req);
			::tnt_request_set_tuple(&req, &args);

			const std::string& func = ts.delete_call;
			::tnt_request_set_func(&req, func.c_str(), func.length());

			Send(&req);
		}
	} else if (ev.event == "INSERT") {
		if (ts.insert_call.empty()) {
			// __tnt_object tuple;
			// add_tuple(&tuple);

			// __tnt_request req;
			// ::tnt_request_replace(&req);
			// ::tnt_request_set_space(&req, ts.space);
			// ::tnt_request_set_tuple(&req, &tuple);

			__tnt_object tuple;
			add_tuple(&tuple);

			__tnt_object ops;
			add_ops(&ops);

			__tnt_request req;
			::tnt_request_upsert(&req);
			::tnt_request_set_space(&req, ts.space);
			::tnt_request_set_tuple(&req, &tuple);
			::tnt_request_set_ops(&req, &ops);

			Send(&req);
		} else {
			__tnt_object args;
			::tnt_object_add_array(&args, 2);
			add_tuple(&args);
			add_ops(&args, false);
			::tnt_object_container_close(&args);

			__tnt_request req;
			::tnt_request_call(&req);
			::tnt_request_set_tuple(&req, &args);

			const std::string& func = ts.insert_call;
			::tnt_request_set_func(&req, func.c_str(), func.length());

			Send(&req);
		}
	} else if (ev.event == "UPDATE") {
		if (ts.update_call.empty()) {
			__tnt_object key;
			add_key(&key);

			__tnt_object ops;
			add_ops(&ops);

			__tnt_request req;
			::tnt_request_update(&req);
			::tnt_request_set_space(&req, ts.space);
			::tnt_request_set_key(&req, &key);
			::tnt_request_set_ops(&req, &ops);

			Send(&req);
		} else {
			__tnt_object args;
			::tnt_object_add_array(&args, 2);
			add_key(&args);
			add_ops(&args, false);
			::tnt_object_container_close(&args);

			__tnt_request req;
			::tnt_request_call(&req);
			::tnt_request_set_tuple(&req, &args);

			const std::string& func = ts.update_call;
			::tnt_request_set_func(&req, func.c_str(), func.length());

			Send(&req);
		}
	} else {
		throw std::range_error("Uknown binlog event: " + ev.event);
	}

	if (ev.binlog_name != "") {
		binlog_name = ev.binlog_name;
		binlog_pos = ev.binlog_pos;
	}

	return false;
}

// blocking send
int64_t TPWriter::Send(struct ::tnt_request *req)
{
	struct ::tnt_stream sbuf;
	::tnt_buf(&sbuf);
	const int64_t sync = ::tnt_request_compile(&sbuf, req);

	size_t len = TNT_SBUF_SIZE(&sbuf);
	char *buf = TNT_SBUF_DATA(&sbuf);

	while (len > 0) {
		const ssize_t r = sess.write(&sess, buf, len);
		if (r < 0) {
			const int _errno = ::tnt_errno(&sess);
			if (_errno == EWOULDBLOCK || _errno == EAGAIN) {
				continue;
			}
			::tnt_stream_free(&sbuf);
			throw std::runtime_error("Send failed: " + std::string(::tnt_strerror(&sess)));
		}
		len -= r;
		buf += r;
	}

	::tnt_stream_free(&sbuf);
	return sync;
}

bool TPWriter::Sync(bool force)
{
	if (next_ping_attempt == 0 || Milliseconds() > next_ping_attempt) {
		force = true;
		next_ping_attempt = Milliseconds() + TPWriter::PING_TIMEOUT;
		Ping();
	}
	if (force || next_sync_attempt == 0 || Milliseconds() >= next_sync_attempt) {
		SaveBinlogPos();
		next_sync_attempt = Milliseconds() + sync_retry;
	}
	return true;
}

// non-blocking receive
int TPWriter::Recv(struct ::tnt_reply *re)
{
	const int r = sess.read_reply(&sess, re);

	if (r == 0) {
		if (re->code) {
			reply_server_code = re->code;
			reply_error_msg = std::move(std::string(re->error, re->error_end - re->error));
			return -1;
		} else if (reply_server_code) {
			reply_server_code = 0;
			reply_error_msg = "";
		}
		return r;
	}
	if (r < 0) {
		const int _errno = ::tnt_errno(&sess);
		if (_errno == EWOULDBLOCK || _errno == EAGAIN) {
			return r;
		}
		throw std::runtime_error("Recv failed: " + std::string(::tnt_strerror(&sess)));
	}
	return r;
}

int TPWriter::ReadReply()
{
	int r;
	__tnt_reply re;

	do r = Recv(&re);
	while (r == 0);
	return r;
}

uint64_t TPWriter::GetReplyCode() const
{
	return reply_server_code;
}

const std::string& TPWriter::GetReplyErrorMessage() const
{
	return reply_error_msg;
}

uint64_t TPWriter::Milliseconds()
{
	struct timeval tp;
	::gettimeofday( &tp, NULL );
	if (!secbase) {
		secbase = tp.tv_sec;
		return tp.tv_usec / 1000;
	}
	return (uint64_t)(tp.tv_sec - secbase)*1000 + tp.tv_usec / 1000;
}

}
