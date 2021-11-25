#include <string>
#include <iostream>
#include <fstream>

#include <Poco/Data/MySQL/Connector.h>
#include <Poco/Data/MySQL/MySQLException.h>
#include <Poco/Data/SessionFactory.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Dynamic/Var.h>



void drop_person_table(int shard, Poco::Data::Session &session)
{
    std::string drop_table_cmd = "DROP TABLE IF EXISTS `Person`;-- sharding:" + std::to_string(shard);

    Poco::Data::Statement drop_stmt(session);
    drop_stmt << drop_table_cmd;
    drop_stmt.execute();

    std::cout << "Table has been successfully dropped for shard " << shard << std::endl;
}



void create_person_table(int shard, Poco::Data::Session &session)
{
    std::string create_table_cmd = 
        "CREATE TABLE IF NOT EXISTS `Person` ("
        "`login` VARCHAR(256) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL UNIQUE,"
        "`first_name` VARCHAR(256) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,"
        "`last_name` VARCHAR(256) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,"
        "`age` SMALLINT NOT NULL,"
        "PRIMARY KEY (`login`));-- sharding:" + std::to_string(shard);

    Poco::Data::Statement create_stmt(session);
    create_stmt << create_table_cmd;
    create_stmt.execute();
    std::cout << "Table has been successfully created for shard " << shard << std::endl;
}



void create_person_index(int shard, Poco::Data::Session &session)
{
    std::string create_index_cmd = 
        "CREATE INDEX fn_ln ON Person (first_name, last_name);-- sharding:" + std::to_string(shard);
    
    Poco::Data::Statement create_index_stmt(session);
    create_index_stmt << create_index_cmd;
    create_index_stmt.execute();
    std::cout << "Index has been successfully created for shard " << shard << std::endl;
}



auto main(int argc, char *argv[]) -> int
{
    if (argc < 2) 
    {
        std::cout << "Enter ip addres as a first argument." << std::endl;
        return 1;
    }

    std::string host(argv[1]);
    std::cout << "connecting to:" << host << std::endl;
    Poco::Data::MySQL::Connector::registerConnector();
    std::cout << "connector registered" << std::endl;

    std::string connection_str;
    connection_str = "host=";
    connection_str += host;
    connection_str += ";user=test;db=sql_test;password=pzjqUkMnc7vfNHET;port=6033";

    std::cout << "connectiong to: " << connection_str << std::endl;
    Poco::Data::Session session(
        Poco::Data::SessionFactory::instance().create(
            Poco::Data::MySQL::Connector::KEY, connection_str));
    std::cout << "session created" << std::endl;
    try
    {
        drop_person_table(0, session);
        drop_person_table(1, session);

        create_person_table(0, session);
        create_person_table(1, session);

        create_person_index(0, session);
        create_person_index(1, session);

        // https://www.onlinedatagenerator.com/
        std::string json;
        std::ifstream is("./data.json");
        std::istream_iterator<char> eos;
        std::istream_iterator<char> iit(is);
        while (iit != eos)
            json.push_back(*(iit++));
        is.close();

        Poco::JSON::Parser parser;
        // check that we read the json
        if (json.empty()) 
        {
            std::cout << "Empty json. Check that the file exists." << std::endl;
            return 1;
        }
        Poco::Dynamic::Var result = parser.parse(json);
        Poco::JSON::Array::Ptr arr = result.extract<Poco::JSON::Array::Ptr>();

        size_t i{0};
        auto array_size = arr->size();
        for (i = 0; i < array_size; ++i)
        {
            Poco::JSON::Object::Ptr object = arr->getObject(i);

            std::string first_name = object->getValue<std::string>("first_name");
            std::string last_name = object->getValue<std::string>("last_name");
            unsigned short int age = (std::rand() % 80) + 20; // random in range [20; 100]
            std::string login = "login_" + std::to_string(i);

            Poco::Data::Statement insert(session);
            size_t shard_number = std::hash<std::string>{}(login) % 2;

            insert << "INSERT INTO Person (login,first_name,last_name,age) VALUES(?, ?, ?, ?)-- sharding:" + std::to_string(shard_number),
                Poco::Data::Keywords::use(login),
                Poco::Data::Keywords::use(first_name),
                Poco::Data::Keywords::use(last_name),
                Poco::Data::Keywords::use(age);

            insert.execute();

            if (i % 1000 == 0) 
            {
                std::cout << "Inserted " << i << "/" << array_size << " records" << std::endl;
            }
        }       
    }
    catch (Poco::Data::MySQL::ConnectionException &e)
    {
        std::cout << "connection:" << e.what() << std::endl;
    }
    catch (Poco::Data::MySQL::StatementException &e)
    {
        std::cout << "statement:" << e.what() << std::endl;
    }
    catch (Poco::Data::DataException ex)
    {
        std::cout << "exception:" << ex.what() << std::endl;
    }

    return 0;
}
