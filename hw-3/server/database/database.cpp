#include "database.h"
#include "../config/config.h"

namespace database{
    Database::Database(){
        _connection_string+="host=";
        _connection_string+=Config::get().get_host();
        _connection_string+=";port=";
        _connection_string+=Config::get().get_port();
        _connection_string+=";user=";
        _connection_string+=Config::get().get_login();
        _connection_string+=";db=";
        _connection_string+=Config::get().get_database();
        _connection_string+=";password=";
        _connection_string+=Config::get().get_password();
        std::cout << "connection string:" << _connection_string << std::endl;
        Poco::Data::MySQL::Connector::registerConnector();
    }

    Database& Database::get(){
        static Database _instance;
        return _instance;
    }

    Poco::Data::Session Database::create_session(){
        return Poco::Data::Session(Poco::Data::SessionFactory::instance().create(Poco::Data::MySQL::Connector::KEY, _connection_string));
    }

    size_t Database::shards_number(){
        return 2;
    }

    std::string Database::sharding_hint(std::string login){
        size_t shard_number = std::hash<std::string>{}(login) % shards_number();

        std::string result = "-- sharding:";
        result += std::to_string(shard_number);
        return result;
    }
}
