#ifndef REPLICATOR_SERIALIZABLE_H
#define REPLICATOR_SERIALIZABLE_H

#include <string>
#include <map>
#include <sstream>
#include <boost/any.hpp>

namespace replicator {

class SerializableValue {
private:
	std::string type_id;

	void fromAny (const boost::any &v)
	{
		if (v.type() == typeid(std::string)) {
			type_id = "string";
			second = boost::any_cast<std::string>(v);
		} else {
			std::ostringstream s;

			if (v.type() == typeid(char)) {
				type_id = "int";
				s << (short)boost::any_cast<char>(v);
			}
			else if (v.type() == typeid(unsigned char)) {
				type_id = "uint";
				s << (unsigned short)boost::any_cast<unsigned char>(v);
			}
			else if (v.type() == typeid(short)) {
				type_id = "int";
				s << boost::any_cast<short>(v);
			}
			else if (v.type() == typeid(unsigned short)) {
				type_id = "uint";
				s << boost::any_cast<unsigned short>(v);
			}
			else if (v.type() == typeid(int)) {
				type_id = "int";
				s << boost::any_cast<int>(v);
			}
			else if (v.type() == typeid(unsigned int)) {
				type_id = "uint";
				s << boost::any_cast<unsigned int>(v);
			}
			else if (v.type() == typeid(long)) {
				type_id = "int";
				s << boost::any_cast<long>(v);
			}
			else if (v.type() == typeid(unsigned long)) {
				type_id = "uint";
				s << boost::any_cast<unsigned long>(v);
			}
			else if (v.type() == typeid(long long)) {
				type_id = "int";
				s << boost::any_cast<long long>(v);
			}
			else if (v.type() == typeid(unsigned long long)) {
				type_id = "uint";
				s << boost::any_cast<unsigned long long>(v);
			}
			else if (v.type() == typeid(float)) {
				type_id = "float";
				s << boost::any_cast<float>(v);
			}
			else if (v.type() == typeid(double)) {
				type_id = "double";
				s << boost::any_cast<double>(v);
			}
			else {
				type_id = "null";
			}
			second = s.str();
		}
	}

public:
	std::string second;

	SerializableValue () {}

	SerializableValue (const boost::any &v) { fromAny(v); }

	SerializableValue& operator= (const boost::any &v) { fromAny(v); return *this; }

	boost::any operator *() const {
		if (type_id == "string") {
			return boost::any(second);
		}

		std::istringstream s(second);

		if (type_id == "int") {
			long long val;
			s >> val;
			return boost::any(val);
		}
		if (type_id == "uint") {
			unsigned long long val;
			s >> val;
			return boost::any(val);
		}
		if (type_id == "float") {
			float val;
			s >> val;
			return boost::any(val);
		}
		if (type_id == "double") {
			double val;
			s >> val;
			return boost::any(val);
		}

		return boost::any();
	}

	const std::string& value_string() const {
		return second;
	}

	const std::string& get_type_id() const {
		return type_id;
	}
};


struct SerializableBinlogEvent
{
	std::string binlog_name;
	unsigned long binlog_pos;
	unsigned long seconds_behind_master;
	unsigned long unix_timestamp;
	std::string database;
	std::string table;
	std::string event;
	std::map<unsigned, SerializableValue> row;
};

} // replicator

#endif // REPLICATOR_SERIALIZABLE_H
