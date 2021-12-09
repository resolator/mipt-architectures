# hw-4
This home work is about setting up kafka database writer in a POCO server.

Use `docker-compose up` in the `docker` directory to up the kafka and zookeeper servers.

## API

- `GET /person?login=some_login` - find and show user with this "some_login" login
- `POST /person?add&first_name=Viktor&last_name=Fet&login=some_login&age=23` - add new user
