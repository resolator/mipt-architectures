#ifndef PERSONHANDLER_H
#define PERSONHANDLER_H

#include "Poco/Net/HTTPServer.h"
#include "Poco/Net/HTTPRequestHandler.h"
#include "Poco/Net/HTTPRequestHandlerFactory.h"
#include "Poco/Net/HTTPServerParams.h"
#include "Poco/Net/HTTPServerRequest.h"
#include "Poco/Net/HTTPServerResponse.h"
#include "Poco/Net/HTTPServerParams.h"
#include "Poco/Net/HTMLForm.h"
#include "Poco/Net/ServerSocket.h"
#include "Poco/Timestamp.h"
#include "Poco/DateTimeFormatter.h"
#include "Poco/DateTimeFormat.h"
#include "Poco/Exception.h"
#include "Poco/ThreadPool.h"
#include "Poco/Util/ServerApplication.h"
#include "Poco/Util/Option.h"
#include "Poco/Util/OptionSet.h"
#include "Poco/Util/HelpFormatter.h"
#include <iostream>
#include <iostream>
#include <fstream>

using Poco::DateTimeFormat;
using Poco::DateTimeFormatter;
using Poco::ThreadPool;
using Poco::Timestamp;
using Poco::Net::HTMLForm;
using Poco::Net::HTTPRequestHandler;
using Poco::Net::HTTPRequestHandlerFactory;
using Poco::Net::HTTPServer;
using Poco::Net::HTTPServerParams;
using Poco::Net::HTTPServerRequest;
using Poco::Net::HTTPServerResponse;
using Poco::Net::NameValueCollection;
using Poco::Net::ServerSocket;
using Poco::Util::Application;
using Poco::Util::HelpFormatter;
using Poco::Util::Option;
using Poco::Util::OptionCallback;
using Poco::Util::OptionSet;
using Poco::Util::ServerApplication;

#include "../../database/person.h"

class PersonHandler : public HTTPRequestHandler
{
private:
    bool check_name(const std::string &name, std::string &reason)
    {
        if (name.length() < 3)
        {
            reason = "Name must be at leas 2 signs";
            return false;
        }

        if (name.find(' ') != std::string::npos)
        {
            reason = "Name can't contain spaces";
            return false;
        }

        if (name.find('\t') != std::string::npos)
        {
            reason = "Name can't contain tabs";
            return false;
        }

        return true;
    };

    bool check_login(const std::string &login, std::string &reason)
    {
        auto result = database::Person::read_by_login(login);

        if (result.get_login() == login)
        {
            reason = "User with this login already exists";
            return false;
        }

        if (login.find(' ') != std::string::npos)
        {
            reason = "Login can't contain spaces";
            return false;
        }

        if (login.find('\t') != std::string::npos)
        {
            reason = "Login can't contain tabs";
            return false;
        }

        return true;
    }

    bool check_age(const unsigned short int &age, std::string &reason)
    {
        if (age < 18 || age > 200)
        {
            reason = "Check the age";
            return false;
        }

        return true;
    }

public:
    PersonHandler(const std::string &format) : _format(format)
    {
    }

    void handleRequest(HTTPServerRequest &request,
                       HTTPServerResponse &response)
    {
        HTMLForm form(request, request.stream());
        response.setChunkedTransferEncoding(true);
        response.setContentType("application/json");
        std::ostream &ostr = response.send();

        if (form.empty())
        {
            auto results = database::Person::read_all();
            Poco::JSON::Array arr;
            for (auto s : results)
                arr.add(s.toJSON());
                
            Poco::JSON::Stringifier::stringify(arr, ostr);
            return;
        }

        if (form.has("search"))
        {
            try
            {
                std::string  fn = form.get("first_name");
                std::string  ln = form.get("last_name");
                auto results = database::Person::search(fn, ln);
                Poco::JSON::Array arr;
                for (auto s : results)
                    arr.add(s.toJSON());
                Poco::JSON::Stringifier::stringify(arr, ostr);
            }
            catch (...)
            {
                ostr << "{ \"result\": false , \"reason\": \"not found\" }";
                return;
            }
            return;
        }
        else if (form.has("add"))
        {
            if (form.has("first_name") && form.has("last_name") && form.has("login") && form.has("age"))
                {
                    database::Person person;
                    person.first_name() = form.get("first_name");
                    person.last_name() = form.get("last_name");
                    person.login() = form.get("login");
                    person.age() = (unsigned short int) strtoul(form.get("age").c_str(), NULL, 0);

                    bool check_result = true;
                    std::string message;
                    std::string reason;

                    if (!check_name(person.get_first_name(), reason) || 
                        !check_name(person.get_last_name(), reason) || 
                        !check_login(person.get_login(), reason) ||
                        !check_age(person.get_age(), reason))
                    {
                        check_result = false;
                        message += reason;
                        message += "<br>";
                    }

                    if (check_result)
                    {
                        try
                        {
                            person.save_to_mysql();
                            person.save_to_cache();
                            ostr << "{ \"result\": true }";
                            return;
                        }
                        catch (...)
                        {
                            ostr << "{ \"result\": false , \"reason\": \" database error\" }";
                            return;
                        }
                    }
                    else
                    {
                        ostr << "{ \"result\": false , \"reason\": \"" << message << "\" }";
                        return;
                    }
                }
        }
        else if (form.has("login"))
        {
            std::string login = form.get("login");
            bool no_cache = false;
            if (form.has("no_cache"))
                no_cache = true;
            
            if (!no_cache)
            {
                try
                {
                    database::Person result = database::Person::read_from_cache_by_login(login);
                    std::cout << "item from cache:" << login << std::endl;
                    Poco::JSON::Stringifier::stringify(result.toJSON(), ostr);
                    return;
                }
                catch (...)
                {
                    std::cout << "cache missed for id:" << login << std::endl;
                }
            }

            auto result = database::Person::read_by_login(form.get("login"));
            if (result.get_login() == LOGIN_NOT_FOUND)
            {
                ostr << "{ \"result\": false , \"reason\": \"Login not found\" }";
                return;
            }

            if (!no_cache)
                result.save_to_cache();

            Poco::JSON::Stringifier::stringify(result.toJSON(), ostr);
        }
    }

private:
    std::string _format;
};
#endif // !PERSONHANDLER_H
