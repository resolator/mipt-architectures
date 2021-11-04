#include "person.h"
#include "database.h"
#include "cache.h"
#include "../config/config.h"

#include <Poco/Data/MySQL/Connector.h>
#include <Poco/Data/MySQL/MySQLException.h>
#include <Poco/Data/SessionFactory.h>
#include <Poco/Data/RecordSet.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Dynamic/Var.h>

#include <sstream>
#include <exception>

using namespace Poco::Data::Keywords;
using Poco::Data::Session;
using Poco::Data::Statement;

namespace database
{
    void Person::init()
    {
        try
        {
            Poco::Data::Session session = database::Database::get().create_session();

            // Statement drop_stmt(session);
            // drop_stmt << "DROP TABLE IF EXISTS Person", now;

            // (re)create table
            Statement create_stmt(session);
            create_stmt << "CREATE TABLE IF NOT EXISTS `Person` ("
                        << "`login` VARCHAR(256) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL UNIQUE,"
                        << "`first_name` VARCHAR(256) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,"
                        << "`last_name` VARCHAR(256) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,"
                        << "`age` SMALLINT NOT NULL,"
                        << "PRIMARY KEY (`login`),KEY `fn` (`first_name`),KEY `ln` (`last_name`));",
                now;
        }
        catch (Poco::Data::MySQL::ConnectionException &e)
        {
            std::cout << "connection:" << e.what() << std::endl;
            throw;
        }
        catch (Poco::Data::MySQL::StatementException &e)
        {
            std::cout << "statement:" << e.what() << std::endl;
            throw;
        }
    }

    Poco::JSON::Object::Ptr Person::toJSON() const
    {
        Poco::JSON::Object::Ptr root = new Poco::JSON::Object();

        root->set("first_name", _first_name);
        root->set("last_name", _last_name);
        root->set("login", _login);
        root->set("age", _age);

        return root;
    }

    Person Person::fromJSON(const std::string &str)
    {
        Person person;
        Poco::JSON::Parser parser;
        Poco::Dynamic::Var result = parser.parse(str);
        Poco::JSON::Object::Ptr object = result.extract<Poco::JSON::Object::Ptr>();

        person.first_name() = object->getValue<std::string>("first_name");
        person.last_name() = object->getValue<std::string>("last_name");
        person.login() = object->getValue<std::string>("login");
        person.age() = object->getValue<unsigned short int>("age");

        return person;
    }

    Person Person::read_by_login(std::string login)
    {
        try
        {
            Poco::Data::Session session = database::Database::get().create_session();
            Poco::Data::Statement select(session);
            Person a;
            select << "SELECT first_name, last_name, login, age FROM Person where login=?",
                into(a._first_name),
                into(a._last_name),
                into(a._login),
                into(a._age),
                use(login),
                range(0, 1); //  iterate over result set one row at a time
            select.execute();
            Poco::Data::RecordSet rs(select);
            
            if (!rs.moveFirst())
            {
                a._login = LOGIN_NOT_FOUND;
            }

            return a;
        }
        catch (Poco::Data::MySQL::ConnectionException &e)
        {
            std::cout << "connection:" << e.what() << std::endl;
            throw;
        }
        catch (Poco::Data::MySQL::StatementException &e)
        {
            std::cout << "statement:" << e.what() << std::endl;
            throw;
        }
    }

    std::vector<Person> Person::read_all()
    {
        try
        {
            Poco::Data::Session session = database::Database::get().create_session();
            Statement select(session);
            std::vector<Person> result;
            Person a;
            select << "SELECT first_name, last_name, login, age FROM Person",
                into(a._first_name),
                into(a._last_name),
                into(a._login),
                into(a._age),
                range(0, 1); //  iterate over result set one row at a time

            while (!select.done())
            {
                select.execute();
                result.push_back(a);
            }
            return result;
        }

        catch (Poco::Data::MySQL::ConnectionException &e)
        {
            std::cout << "connection:" << e.what() << std::endl;
            throw;
        }
        catch (Poco::Data::MySQL::StatementException &e)
        {
            std::cout << "statement:" << e.what() << std::endl;
            throw;
        }
    }

    std::vector<Person> Person::search(std::string first_name, std::string last_name)
    {
        try
        {
            Poco::Data::Session session = database::Database::get().create_session();
            Statement select(session);
            std::vector<Person> result;
            Person a;
            first_name+="%";
            last_name+="%";
            select << "SELECT first_name, last_name, login, age FROM Person where first_name LIKE ? and last_name LIKE ?",
                into(a._first_name),
                into(a._last_name),
                into(a._login),
                into(a._age),
                use(first_name),
                use(last_name),
                range(0, 1); //  iterate over result set one row at a time

            while (!select.done())
            {
                select.execute();
                result.push_back(a);
            }
            return result;
        }
        catch (Poco::Data::MySQL::ConnectionException &e)
        {
            std::cout << "connection:" << e.what() << std::endl;
            throw;
        }
        catch (Poco::Data::MySQL::StatementException &e)
        {
            std::cout << "statement:" << e.what() << std::endl;
            throw;
        }
    }

    Person Person::read_from_cache_by_login(std::string login)
    {
        try
        {
            std::string result;
            if (database::Cache::get().get(login, result))
                return fromJSON(result);
            else
                throw std::logic_error("key not found in the cache");
        }
        catch (std::exception err)
        {
            // std::cout << "error:" << err.what() << std::endl;
            throw;
        }
    }

    void Person::warm_up_cache()
    {
        std::cout << "wharming up persons cache ...";
        auto array = read_all();
        long count = 0;
        for (auto &a : array)
        {
            a.save_to_cache();
            ++count;
        }
        std::cout << "done: " << count << std::endl;
    }

    size_t Person::size_of_cache(){
        return database::Cache::get().size();
    }

    void Person::save_to_cache()
    {
        std::stringstream ss;
        Poco::JSON::Stringifier::stringify(toJSON(), ss);
        std::string message = ss.str();
        database::Cache::get().put(_login, message);
    }

    void Person::save_to_mysql()
    {
        try
        {
            Poco::Data::Session session = database::Database::get().create_session();
            Poco::Data::Statement insert(session);

            insert << "INSERT INTO Person (first_name,last_name,login,age) VALUES(?, ?, ?, ?)",
                use(_first_name),
                use(_last_name),
                use(_login),
                use(_age);

            insert.execute();
        }
        catch (Poco::Data::MySQL::ConnectionException &e)
        {
            std::cout << "connection:" << e.what() << std::endl;
            throw;
        }
        catch (Poco::Data::MySQL::StatementException &e)
        {
            std::cout << "statement:" << e.what() << std::endl;
            throw;
        }
    }

    const std::string &Person::get_first_name() const
    {
        return _first_name;
    }

    const std::string &Person::get_last_name() const
    {
        return _last_name;
    }

    const std::string &Person::get_login() const
    {
        return _login;
    }

    const unsigned short int &Person::get_age() const
    {
        return _age;
    }

    std::string &Person::first_name()
    {
        return _first_name;
    }

    std::string &Person::last_name()
    {
        return _last_name;
    }

    std::string &Person::login()
    {
        return _login;
    }

    unsigned short int &Person::age()
    {
        return _age;
    }
}
